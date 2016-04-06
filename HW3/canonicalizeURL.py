__author__ = 'snehagaikwad'
from urlparse import urlparse, urlunparse
import re
from urllib import unquote
from datetime import datetime

_collapse = re.compile('([^/]+/\.\./?|/\./|//|/\.$|/\.\.$)')

def canonURL(url):
    #print urlparse(url)
    (scheme, netloc, path, parameters, query, fragment) =  urlparse(url)
    scheme = scheme.lower()
    netloc = netloc.lower()
    if scheme == "http":
        #print scheme
        fields = netloc.split(":")
        if fields.__len__() > 1:
            if fields[1] == "80":
                netloc = fields[0]
    if scheme == "https":
        fields = netloc.split(":")
        if fields.__len__() > 1:
            if fields[1] == "443":
                netloc = fields[0]
    last_path = path
    while 1:
        path = _collapse.sub('/', path, 1)
        if last_path == path:
            break
        last_path = path
    path = unquote(path)
    c_url = urlunparse((scheme, netloc, path,"","",""))
    return c_url

if __name__ == '__main__':
    start_time = datetime.now()
    url = "http://www.basketball-reference.com/awards/nba_50_greatest.html#anything"
    canon_url = canonURL(url)
    print canon_url
    print datetime.now() - start_time