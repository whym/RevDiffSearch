from cStringIO import StringIO

import web
from web import form

from mako.template import Template
from mako.runtime import Context
from mako.lookup import TemplateLookup

from lucene import StandardAnalyzer, File, QueryParser, Version, SimpleFSDirectory, File, initVM 

urls = (
'/', 'index'        
)

app = web.application(urls, globals())
lookup = TemplateLookup(directories=['templates/'])

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
            query = search['query'].value
            hits = self.fetch_results(query)
            return serve_template('results.html',query=query, hits=hits)
    
    def init_lucene(self, query_str):
        initVM()
        indexDir = "/Tmp/REMOVEME.index-dir"
        dir = SimpleFSDirectory(File(indexDir))
        searcher = IndexSearcher(dir)
        analyzer = StandardAnalyzer(Version.LUCENE_34)
        query = QueryParser(Version.LUCENE_34, 'diff', analyzer).parse(query_str)
        
        
    def fetch_results(self):
        query = self.init_lucene()
        MAX = 1000
        hits = searcher.search(query, MAX)         
        print "Found %d document(s) that matched query '%s':" % (hits.totalHits, query)
        return hits

      

if __name__ == '__main__':
    app.run()
    