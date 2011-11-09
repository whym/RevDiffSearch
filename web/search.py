import os
import socket
import sys
import cPickle
import cStringIO

import web
from web import form

from mako.template import Template
from mako.runtime import Context
from mako.lookup import TemplateLookup

import settings

urls = (
'/', 'index'        
)


if settings.DEBUG:
    web.config.debug = True

lookup = TemplateLookup(directories=['templates/'])
if settings.HOSTNAME == 'production':
    application = web.application(urls, globals()).wsgifunc()
else:
    app = web.application(urls, globals())

lookup = TemplateLookup(directories=[os.path.join(os.path.dirname(__file__),'templates')])


def serve_template(templatename, **kwargs):
    view = lookup.get_template(templatename)
    return view.render(**kwargs)


class index:
    def __init__(self, *args, **kwargs):
        self.links= {'rev_id':'w/index.php?diff=',
                     'title':'wiki/',
                     'user_text':'wiki/User:'}
        
    def searchform(self):
        search = form.Form(
            form.Textarea('query', form.notnull),
            form.Button('Search!')
        )
        return search
    
    def GET(self):
        search = self.searchform()
        return serve_template('index.html',form=search) 
    
    def POST(self):
        search = self.searchform()
        if not search.validates():
            return serve_template('index.html',form=search)
        else:
            query_str = search['query'].value
            results = self.fetch_results(query_str)
            headings = self.extract_headings(results)
            print 'About to send results to browser...'
            return serve_template('results.html',query_str=query_str, results=results, headings=headings, form=search, links=self.links)
    
    
    def extract_headings(self, results):
        return results.pop('headings')
        
        
    def fetch_results(self, query_str):
        # Create a socket (SOCK_STREAM means a TCP socket)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # Connect to server and send data
            sock.connect((settings.HOST, settings.PORT))
            sock.send(query_str)
        
            # Receive data from the server and shut down
            buffer = cStringIO.StringIO()
            buffer.write(sock.recv(4096))
            done = False
            while not done:
                more = sock.recv(4096)
                if not more:
                    done = True
                else:
                    buffer.write(more)
            #print buffer.getvalue()
            results = cPickle.loads(buffer.getvalue())
        except Exception,e:
            print e
            resuls = e
        finally:
            sock.close()
        
        return results
        
        #print "Sent:     {}".format(query_str)
        #print "Received: {}".format(result)
    

if __name__ == '__main__':
    if app:
        app.run()
    