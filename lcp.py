from linsuffarr import *

def longest_common_substring(text, freq, length):
    sa = SuffixArray(text, unit=UNIT_CHARACTER)
    lcp = sa._LCP_values
    sa = sa.SA
    ret = ''
    i = 0
    while i < len(sa):
        f = 1
        while i + 1 < len(sa) and lcp[i+1] >= length:
            f += 1
            i += 1
        if f >= freq and lcp[i] > len(ret):
            ret = text[sa[i-1] : sa[i-1]+lcp[i]]
        if f == 1:
            i += 1
    return ret

def common_substring(a, freq, length):
    text = ''
    anchor = 1
    for x in a:
        text += x + chr(anchor)
        anchor = (anchor + 1) % 256
    return longest_common_substring(text, freq, length)

if __name__ == '__main__':
    a = ['[[File:Stop hand nuvola.svg|30px|alt=|link=]] \'\'\'This is your last warning\'\'\'. You will be blocked from editing the next time you vandalize a page, as you did with <span class="plainlinks">[{{{2}}} this edit]</span> to [[:{{{1}}}]]. <!-- Uw-vandalism4 --><!-- Template:Huggle/warn-4 --> ~~<noinclude></noinclude>~~<noinclude> [[pt:Predefinio:Huggle/warn-4]] </noinclude>',
         '<div style=clear: both></div>{{<includeonly>safesubst:</includeonly>Huggle/uw-4 |page=[[:{{{1}}}]] with [{{{2}}} this edit] |extra=~~<noinclude></noinclude>~~ |reason={{{reason|[[Wikipedia:Vandalism|vandalize]] Wikipedia}}} }}<!-- Template:uw-vandalism4 --><noinclude> {{Huggle/TemplateNotice|series = uw-vandalism|max = 4im|s1 = uw-v4|s2 = uw-vand4|s3 = uw-vandal4|nothankyou=yes}} </noinclude>',
         '<div style=clear: both></div>{{<includeonly>safesubst:</includeonly>Huggle/uw-4 |page=[[:{{{1}}}]] with <span class="plainlinks">[{{{2}}} this edit]</span> |extra=~~<noinclude></noinclude>~~ |reason={{{reason|[[Wikipedia:Vandalism|vandalize]] Wikipedia}}} }}<!-- Template:Huggle/warn-4 --><noinclude> {{Huggle/TemplateNotice|series = uw-vandalism|max = 4im|s1 = uw-v4|s2 = uw-vand4|s3 = uw-vandal4|nothankyou=yes}} </noinclude>',
         '[[File:Stop hand nuvola.svg|30px|alt=|link=]] \'\'\'This is your last warning\'\'\'. You will be blocked from editing the next time you vandalize a page, as you did with <span class="plainlinks">[{{{2}}} this edit]</span> to [[:{{{1}}}]]. <!-- Uw-vandalism4 --><!-- Template:Huggle/warn-4 --> ~~<noinclude></noinclude>~~<noinclude> [[pt:Predefinio:Huggle/warn-4]] </noinclude>',
         '[[File:Stop hand nuvola.svg|30px]] \'\'\'This is your last warning\'\'\'. You may be \'\'\'blocked from editing without further warning\'\'\' the next time you vandalize a page, as you did with <span class="plainlinks">[{{{2}}} this edit]</span> to [[:{{{1}}}]]. <!-- Template:uw-huggle4 --> ~~<noinclude></noinclude>~~<noinclude> </noinclude>',
         '[[File:Stop hand nuvola.svg|30px]] \'\'\'This is your last warning\'\'\'. You may be \'\'\'blocked from editing without further warning\'\'\' the next time you vandalize a page, as you did with <span class="plainlinks">[{{{2}}} this edit]</span> to [[:{{{1}}}]]. <!-- Template:Huggle/warn-4 --><!-- Template:uw-vandalism4 --> ~~<noinclude></noinclude>~~<noinclude>']
    print common_substring(a, 5, 20)
