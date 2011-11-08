import SocketServer
import os
import cPickle
import logging
import cgi
import itertools

import settings

from lucene import StandardAnalyzer, File, QueryParser, Version, SimpleFSDirectory, File, IndexSearcher, initVM, JavaError


vm = initVM()
print settings.INDEX_DIR

index_dir = SimpleFSDirectory(File(settings.INDEX_DIR))
searcher = IndexSearcher(index_dir)
logging.basicConfig(filename='diffdb.log',level=logging.DEBUG)

class LuceneServer(SocketServer.BaseRequestHandler):
    """
    The RequestHandler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def getheaders(self, doc):
        headers = []
        fields = doc.getFields()
        for field in fields:
            #print dir(field)
            #headers.append(field.stringValue())
            headers.append(field.name)
        print headers
        return headers
    
    def isodd(self, num):
        return num & 1 and True or False
    
    def construct_ngrams(self, data):
        words = data.split(':')
        ngrams = []
        if len(words) == 1:
            ngrams.append(self.gen_ngrams(words[0]))
        else:
            for x in xrange(len(words)):
                if self.isodd(x):
                    ngrams.append(self.gen_ngrams(words[x]))
            for x in xrange(len(ngrams)):
                tokens = ngrams[x]
                for y in xrange(len(tokens)):
                    tokens.insert(y*2, '%s:'% words[x])
                ngrams[x] = tokens
        ngrams = ' '.join(itertools.chain(*ngrams))
        return ngrams

    def gen_ngrams(self, word, n=3):
        wlen = len(word)
        if wlen <= n:
            return word
        i = 0
        ret = []
        while i < wlen - n + 1:
            ret.append(word[i:i+n])
            i += 1
        #return ' '.join(ret)
        return ret

    def serialize(self, hits):
        results = {}
        results['headings'] =[]
        
        for x, hit in enumerate(hits.scoreDocs):
            doc = searcher.doc(hit.doc)
            if x == 0:
                results['headings'] =self.getheaders(doc)
            
            #print dir(doc)
            #print doc.getFields(), doc.getValues("contents")
            #doc.get('contents') #.encode("utf-8")
            results[hit.doc] = {}
            results[hit.doc]['score'] = hit.score
            
            for header in results['headings']:
                print header, doc.get(header)
                value = doc.get(header)
#                if header == 'title':
#                    value = value.replace("'",'')
                if header == 'diff':
                    value = cgi.escape(value, quote=True)
                results[hit.doc][header] = value.encode('raw_unicode_escape').decode('utf-8')
        return cPickle.dumps(results)

    def handle(self):
        # self.request is the TCP socket connected to the client
        # self.rfile is a file-like object created by the handler;
        # we can now use e.g. readline() instead of raw recv() calls
        self.data = self.request.recv(1024).strip()
        #print "{} wrote:".format(self.client_address[0])
        print self.data
        # just send back the same data, but upper-cased
        
        MAX = 50
        analyzer = StandardAnalyzer(Version.LUCENE_34)
        self.data = self.construct_ngrams(self.data)
        
        try:
            query = QueryParser(Version.LUCENE_34, 'diff', analyzer).parse(self.data)
            print query
            hits = searcher.search(query, MAX)
            #if settings.DEBUG:
            print "Found %d document(s) that matched query '%s':" % (hits.totalHits, query)
            serialized = self.serialize(hits)
        except JavaError:
            serialized = cPickle.dumps({})
        self.request.send(serialized)


if __name__ == "__main__":
    # Create the server, binding to localhost on port 9999
    server = SocketServer.TCPServer((settings.HOST, settings.PORT), LuceneServer)
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()