import os
import socket
import sys
import cPickle
from cStringIO import StringIO

import web
from web import form

from mako.template import Template
from mako.runtime import Context
from mako.lookup import TemplateLookup

from lucene import StandardAnalyzer, File, QueryParser, Version, SimpleFSDirectory, File, IndexSearcher, initVM 

urls = (
'/', 'index'        
)

app = web.application(urls, globals())
lookup = TemplateLookup(directories=['templates/'])
HOST, PORT = "localhost", 9999


def serve_template(templatename, **kwargs):
    view = lookup.get_template(templatename)
    return view.render(**kwargs)


class index:
    def searchform(self):
        search = form.Form(
            form.Textarea('query', form.notnull),
            form.Button('Search!')
        )
        return search
    
    def GET(self):
        search = self.searchform()
        return serve_template('index.html',form=search) 
        #view = Template(filename='templates/index.html', lookup=lookup)
        #buf = StringIO()
        #ctx = Context(buf, name='index')
        #view.render_context(ctx)
        #print buf.getvalue()
    
    def POST(self):
        search = self.searchform()
        if not search.validates():
            return serve_template('index.html',form=search)
        else:
            query_str = search['query'].value
            results = self.fetch_results(query_str)
            headings = self.extract_headings(results)
            print results
            return serve_template('results.html',query_str=query_str, results=results, headings=headings)
    
    
    def extract_headings(self, results):
        return results.pop('headings')
        
        
    def fetch_results(self, query_str):
        # Create a socket (SOCK_STREAM means a TCP socket)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # Connect to server and send data
            sock.connect((HOST, PORT))
            sock.send(query_str)
        
            # Receive data from the server and shut down
            received = sock.recv(1024)
            results = cPickle.loads(received)
            #print "Received: {}".format(results)
        finally:
            sock.close()
        return results
        
        #print "Sent:     {}".format(query_str)
        #print "Received: {}".format(result)
    

if __name__ == '__main__':
    app.run()
    