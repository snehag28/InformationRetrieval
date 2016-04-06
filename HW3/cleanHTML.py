__author__ = 'snehagaikwad'

import urllib
from bs4 import BeautifulSoup
from boilerpipe.extract import Extractor
import time
from datetime import datetime

def CleanHtml(url):
    #extractor = Extractor(extractor='ArticleExtractor', url=url)
    #print "time to sleep"
    #time.sleep(1)
    #print "woke up"
    """
    raw_html = urllib.urlopen(url).read()
    if "<table" in raw_html:
        soup = BeautifulSoup(raw_html)
        for script in soup(["script", "style"]):
            script.extract()    # rip it out
            text = soup.get_text()
            # break into lines and remove leading and trailing space on each
            lines = (line.strip() for line in text.splitlines())
            # break multi-headlines into a line each
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            # drop blank lines
            text = '\n'.join(chunk for chunk in chunks if chunk)
    else:
        text = extractor.getText()
    return text,raw_html
    """
    raw_html = urllib.urlopen(url).read()
    soup = BeautifulSoup(raw_html)
    for script in soup(["script", "style"]):
        script.extract()
    text = soup.get_text()
    return text, raw_html

if __name__ == '__main__':
    start_time = datetime.now()
    url = "http://www.basketball-reference.com/awards/nba_50_greatest.html"
    text, htm_text = CleanHtml(url)
    print text
    print datetime.now() - start_time