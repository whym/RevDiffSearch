#! /usr/bin/env python
# -*- coding: utf-8 -*-

# this script requires
# - wikimedia-utilities https://bitbucket.org/halfak/wikimedia-utilities
# - linsuffarr http://jgosme.perso.info.unicaen.fr/Linsuffarr.html

import sys
import argparse
import ast
import json
import csv
import time
import urllib2
import traceback
import copy
import re
import wmf
import lcp
import query as query_func
from wmf.dump.iterator import Iterator

pattern_void  = re.compile(r'(<noinclude>.*?</noinclude>)', flags=re.M|re.MULTILINE)
pattern_split = re.compile(r"(\{\{[A-Z]+\}\}|\{\{#.*\}\}|\n|__[A-Z]+__|\n|\{\{\{\d\}\}\}|\{\{|\}\}|\#|~~~~|~~~)")

def load_revisions(title):
    url  ='http://en.wikipedia.org/wiki/Special:Export/%s?history' % urllib2.quote(title)
    while True:
        try:
            print >>sys.stderr, 'fetching %s' % url
            res = urllib2.urlopen(urllib2.Request(url,
                                                  headers={'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11'}))
            break
        except urllib2.URLError:
            time.sleep(5)
    ret = None
    for x in Iterator(res).readPages():
        yield x.readRevisions()

def gen_prev_next(ls, last=None):
    prev = None
    for x in ls:
        if prev != None:
            yield(prev,x)
        prev = x
    if last != None:
        yield(prev,last)

def inject_result(accum, new):
    datehits = {}
    hitsall = 0
    for dic in [new['hits'], accum['hits']]:
        for (date,hits) in dic:
            if type(hits) == int:
                datehits.setdefault(date, 0)
                datehits[date] = datehits[date] + hits
            else:
                datehits.setdefault(date, set())
                datehits[date] = datehits[date].union(hits)
            hitsall += len(datehits[date])
    ret = copy.deepcopy(accum)
    ret['hits'] = sorted(datehits.items(), key=lambda x: x[0])
    ret['hits_all'] = hitsall
    return ret

def escape_variables(text):
    return map(lambda x: x.replace('"', '\\"'), filter(lambda x: not re.match(pattern_split, x) and len(x) >= 4, re.split(pattern_split, re.sub(pattern_void, '', text))))
    # 'len(x) >= 4' is a fix for 4-gram

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--max', metavar='MAX_REVISIONS',
                        dest='maxrevs', type=int, default=10000,
                        help='')
    parser.add_argument('-n', '--namespace', metavar='NAMESPACE_ID',
                        dest='namespace', type=str, default=None),
    parser.add_argument('-R', '--revisions',
                        dest='revisions', action='store_true', default=False,
                        help='show revision IDs of the hits')
    parser.add_argument('-o', '--output', metavar='FILE',
                        dest='output', type=lambda x: open(x, 'w'), default=sys.stdout,
                        help='')
    parser.add_argument('-D', '--daily',
                        dest='daily', action='store_true', default=False,
                        help='')
    parser.add_argument('-v', '--verbose',
                        dest='verbose', action='store_true', default=False,
                        help='turn on verbose message output')
    parser.add_argument('-d', '--debug',
                        dest='debug', action='store_true', default=False,
                        help='turn on debug output')
    parser.add_argument('inputs', nargs='+')
    options = parser.parse_args()

    titles = options.inputs

    query = {'q': None, 'max_revs': options.maxrevs, 'collapse_hits': 'day' if options.daily else 'month', 'fields': ['rev_id'] if options.revisions else []}

    writer = csv.writer(options.output)
    result = {'hits': [], 'hits_all': 0}
    for title in titles:
        try:
            texts = []
            for revs in load_revisions(title):
                revs = [x for x in revs]
                for rev in revs:
                    texts.append(rev.getText().encode('utf-8'))
                # cs = lcp.common_substring(texts, len(texts) * 0.9, 20)
                # print cs
                # missed = 0
                for ((revPrev,timePrev), (revNext,timeNext)) in gen_prev_next([(x, x.getTimestamp()) for x in revs], (None, int(time.time()))):

                    query['q'] = ' '.join(['"%s"' % x for x in escape_variables(revPrev.getText())])
                    timePrev = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(timePrev))
                    timeNext = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(timeNext))
                    query['q'] += ' timestamp:[%s TO %s]' % (timePrev, timeNext)
                    print query
                    res = query_func.search('localhost', 8080, query)
                    inject_result(result, res)
                    print result['hits_all']
                    #print revPrev.getText()
                    #print revPrev.getId(), timePrev, timeNext - timePrev, revPrev.getText()[:20]
                #     if revPrev.getText().find(cs.decode('utf-8')) < 0:
                #         print 'missed' , timeNext - timePrev, revPrev.getText()#, revPrev.getText().find(cs)
                #         missed += timeNext - timePrev
                #     else:
                #         print 'ok'
                # print missed

        except Exception as e:
            traceback.print_exc(file=sys.stderr)
    print result
    query_func.format_result(writer, result, debug=options.debug)
#<span class="plainlinks">[{{{2}}} this edit]</span> you made to [[:{{{1}}}]].  If you [[Wikipedia:Vandalism|vandalize]] Wikipedia again, you will be '''[[Wikipedia:Blocking policy|blocked from  editing]] without  further notice'''.  <!-- Template:uw-huggle4 --> ~~<noinclude></noinclude>~~_[[Image:Stop hand nuvola.svg|30px]] This is the '''final 
        # if options.namespace:
        #     querystr += ' namespace:' + options.namespace
        # if options.verbose:
        #     print >>sys.stderr, querystr

        # result = query_func.search('localhost', 8080, query)
        
        # if options.verbose:
        #     print >>sys.stderr, result

        # query_func.format_result(writer, result, debug=options.debug)