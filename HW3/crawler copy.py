__author__ = 'snehagaikwad'
from bs4 import BeautifulSoup
import robotparser
import urllib2
import time
from cleanHTML import CleanHtml
from createIndexES import create_index
#import urlparse
from elasticsearch import Elasticsearch
from datetime import datetime
import requests
from canonicalizeURL import canonURL
import Queue
from urlparse import urlparse
from urlparse import urljoin

class Job(object):
    def __init__(self, priority, description):
        self.priority = priority
        self.description = description
        #print 'New job:', description
        return
    def __cmp__(self, other):
        return cmp(self.priority, other.priority)

if __name__== "__main__":


    start_time = datetime.now()
    # instantiate es client, connects to localhost:9200 by default
    es = Elasticsearch()
    create_index(es,'ir_hw3')

    rp = robotparser.RobotFileParser()

    inlink_url_map = {}
    inlink_url_list_map = {}
    outlink_url_list_map = {}
    # initializing the crawler with the seed URL
    seed1 = "http://www.basketball-reference.com/awards/nba_50_greatest.html"
    canon1 = canonURL(seed1)
    seed2 = "http://www.basketball-reference.com/leaders/per_career.html"
    canon2 = canonURL(seed2)
    seed3 = "http://en.wikipedia.org/wiki/LeBron_James"
    canon3 = canonURL(seed3)
    seed4 = "http://www.theguardian.com/sport/2014/jul/11/lebron-james-to-return-to-cleveland"
    canon4 = canonURL(seed4)


    frontier = Queue.PriorityQueue()
    frontier.put( Job(0, seed1) )
    frontier.put( Job(0, seed2) )
    frontier.put( Job(0, seed3) )
    frontier.put( Job(0, seed4) )

    #inlink_url_map[seed1] = 0
    #inlink_url_map[seed2] = 0
    #inlink_url_map[seed3] = 0
    #inlink_url_map[seed4] = 0

    frontier_2 = Queue.PriorityQueue()

    # we maintain two priority queues for crawling for alternate levels - frontier, frontier_2
    # the visited list keeps track of all the visited urls in the frontier
    # the inlink_url_map dict keeps track of the url inlink count
    # we traverse
    count = 0
    #while not frontier.empty() or not frontier_2.empty() or count < 1000:
    visited = []
    while (not frontier.empty() or not frontier_2.empty()) and count < 10:
        while not frontier.empty() and count < 10:
            print "count: " + str(count)
            try:
                next_job = frontier.get()
                url = next_job.description
                if not url in visited:
                    print "frontier url:" + url
                    clean_text, html_text = CleanHtml(url)
                    visited.append(url)
                    count = count + 1
                    soup = BeautifulSoup(html_text)
                    outlink_list = []
                    for link in soup.findAll('a'):

                        full_url = urljoin(url, link.get('href'))
                        print "link in soup line" + str(full_url)
                        parsed_uri = urlparse(full_url)
                        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
                        robot_file = domain + "robots.txt"
                        rp.set_url(robot_file)
                        rp.read()
                        if rp.can_fetch("*", full_url):
                            print "rp.can_fetch"
                            head_res = requests.head(full_url)
                            if "content-language" in head_res.headers and "content-type" in head_res.headers:
                                print "in if"
                                page_language = head_res.headers['content-language']
                                page_type = head_res.headers['content-type']
                            else:
                                #print "in else"
                                page_language = "en"
                                page_type = "text/html"
                            #print page_type
                            #print page_language
                            if "html" in page_type and page_language == "en":
                                canon_url = canonURL(full_url)
                                #print "in here"
                                outlink_list.append(canon_url)
                                if canon_url in inlink_url_list_map:
                                    temp = inlink_url_list_map[canon_url]
                                else:
                                    temp = []
                                if url not in temp:
                                    temp.append(url)
                                inlink_url_list_map[canon_url] = temp
                                if canon_url in inlink_url_map:
                                    inlink_url_map[canon_url] = inlink_url_map[canon_url] - 1
                                else:
                                    inlink_url_map[canon_url] = -1
                                if canon_url not in visited:
                                    frontier_2.put( Job (inlink_url_map[canon_url], canon_url))

                    outlink_url_list_map[url] = outlink_list
                    print "outlink"
                    print outlink_list
                    if url in inlink_url_list_map:
                        op = es.index(index="ir_hw3", doc_type="page", id=url, body={"clean_text": clean_text, "raw_html": html_text, "in_links" : inlink_url_list_map[url], "out_links" : outlink_list})
                    else:
                        op = es.index(index="ir_hw3", doc_type="page", id=url, body={"clean_text": clean_text, "raw_html": html_text, "in_links" : [], "out_links" : outlink_list})

            except Exception, e:
                print e

        while not frontier_2.empty() and count < 10:
            print "count: " + str(count)
            try:
                next_job = frontier_2.get()
                url = next_job.description
                if not url in visited:
                    print "frontier_2 url:" + url
                    clean_text, html_text = CleanHtml(url)
                    visited.append(url)
                    count = count + 1
                    soup = BeautifulSoup(html_text)
                    outlink_list = []
                    for link in soup.findAll('a'):
                        full_url = urljoin(url, link.get('href'))
                        print "link in soup line" + str(full_url)
                        parsed_uri = urlparse(full_url)
                        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
                        robot_file = domain + "robots.txt"
                        rp.set_url(robot_file)
                        rp.read()
                        if rp.can_fetch("*", full_url):
                            print "rp.can_fetch"
                            head_res = requests.head(full_url)
                            if "content-language" in head_res.headers and "content-type" in head_res.headers:
                                #print "in if 2"
                                page_language = head_res.headers['content-language']
                                page_type = head_res.headers['content-type']
                            else:
                                #print "in else 2"
                                page_language = "en"
                                page_type = "text/html"
                            #print page_language
                            #print page_type
                            if "html" in page_type and page_language == "en":
                                print "in if"
                                #print "in here"
                                canon_url = canonURL(full_url)
                                outlink_list.append(canon_url)
                                if canon_url in inlink_url_list_map:
                                    temp = inlink_url_list_map[canon_url]
                                else:
                                    temp = []
                                if url not in temp:
                                    temp.append(canon_url)
                                inlink_url_list_map[canon_url] = temp
                                if canon_url in inlink_url_map:
                                    inlink_url_map[canon_url] = inlink_url_map[canon_url] - 1
                                else:
                                    inlink_url_map[canon_url] = -1
                                if canon_url not in visited:
                                    frontier.put( Job (inlink_url_map[canon_url], canon_url))
                    outlink_url_list_map[url] = outlink_list
                    print "outlink"
                    print outlink_list
                    if url in inlink_url_list_map:
                        op = es.index(index="ir_hw3", doc_type="page", id=url, body={"clean_text": clean_text, "raw_html": html_text, "in_links" : inlink_url_list_map[url], "out_links" : outlink_list})
                    else:
                        op = es.index(index="ir_hw3", doc_type="page", id=url, body={"clean_text": clean_text, "raw_html": html_text, "in_links" : [], "out_links" : outlink_list})
            except Exception, e:
                print e


    print "the length of visited list is:"
    print visited.__len__()
    print visited

    total_time = datetime.now() - start_time
    print total_time