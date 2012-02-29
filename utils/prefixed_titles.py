#!/usr/bin/python
# -*- coding: utf-8  -*-

# get a list of all page titles with certain prefix
# use this with the pywikipedia library in PYTHONPATH

import wikipedia as pywikibot
import query

def main():
    site = pywikibot.getSite('en', 'wikipedia')
    prefix = 'Uw-'
    ns = 10

    for p in site.prefixindex(prefix, namespace=ns):
        print p.title()

    pywikibot.stopme()
    
if __name__ == '__main__':
    main()
