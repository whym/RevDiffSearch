import SocketServer
import os
import cPickle
import logging

import settings

from lucene import StandardAnalyzer, File, QueryParser, Version, SimpleFSDirectory, File, IndexSearcher, initVM 


vm = initVM()
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
        headers = doc.getFields()
        return headers

    def serialize(self, hits):
        results = {}
        for x, hit in enumerate(hits.scoreDocs):
            doc = searcher.doc(hit.doc)
            if x == 0:
                results['headings'] =getheaders(doc)
            
            #print dir(doc)
            #print doc.getFields(), doc.getValues("contents")
            #print doc, doc.toString()
            #doc.get('contents') #.encode("utf-8")
            results[hit.doc] = {}
            results[hit.doc]['score'] = hit.score
            
            for header in results['headings']:
                results[hit.doc][header] = doc.get(header)
        return cPickle.dumps(results)

    def handle(self):
        # self.request is the TCP socket connected to the client
        # self.rfile is a file-like object created by the handler;
        # we can now use e.g. readline() instead of raw recv() calls
        self.data = self.request.recv(1024).strip()
        #print "{} wrote:".format(self.client_address[0])
        #print self.data
        # just send back the same data, but upper-cased
        
        MAX = 1000
        analyzer = StandardAnalyzer(Version.LUCENE_34)
        query = QueryParser(Version.LUCENE_34, 'contents', analyzer).parse(self.data)
        
        hits = searcher.search(query, MAX)
        if settings.DEBUG:
            print "Found %d document(s) that matched query '%s':" % (hits.totalHits, query)
        serialized = self.serialize(hits)
        self.request.send(serialized)


if __name__ == "__main__":
    # Create the server, binding to localhost on port 9999
    server = SocketServer.TCPServer((settings.HOST, settings.PORT), LuceneServer)
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()