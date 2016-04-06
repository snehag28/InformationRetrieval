__author__ = 'snehagaikwad'

from bs4 import BeautifulSoup
import robotparser
import urllib2
from cleanHTML import CleanHtml
from createIndexES import create_index
from elasticsearch import Elasticsearch
from datetime import datetime
import requests
from canonicalizeURL import canonURL
import Queue
from urlparse import urlparse
from urlparse import urljoin
import heapq
from operator import itemgetter
import urllib

if __name__== "__main__":
    start_time = datetime.now()
    # instantiate es client, connects to localhost:9200 by default
    es = Elasticsearch()
    # create the index
    create_index(es,'ir_hw3')
    # instantiate robot parser
    rp = robotparser.RobotFileParser()

    # initializing the crawler with the seed URLs
    seed1 = "http://www.basketball-reference.com/awards/nba_50_greatest.html"
    canon1 = canonURL(seed1)
    seed2 = "http://www.basketball-reference.com/leaders/per_career.html"
    canon2 = canonURL(seed2)
    #seed3 = "http://en.wikipedia.org/wiki/LeBron_James"
    #canon3 = canonURL(seed3)
    #seed4 = "http://www.theguardian.com/sport/2014/jul/11/lebron-james-to-return-to-cleveland"
    #canon4 = canonURL(seed4)

    # initializing a frontier_1 as heapq
    # the heapq will have a list of [inlink count, url number, url]
    # it will first sort based on the inlink and then the count(which tells the order in which the url was visited)
    frontier_1 = []
    heapq.heappush(frontier_1, [0,1,canon1])
    heapq.heappush(frontier_1, [0,2,canon2])
    #heapq.heappush(frontier_1, [-4,3,canon3])
    #heapq.heappush(frontier_1, [0,4,canon4])

    frontier_2 = []

    # lists to maintain the list of urls being processed, make it easy to find whether the url is present in the frontier
    frontierList_1 = []
    frontierList_2 = []

    frontierList_1.append(canon1)
    frontierList_1.append(canon2)
    #frontierList_1.append(canon3)
    #frontierList_1.append(canon4)

    # we maintain two priority heaps for crawling for alternate levels - frontier_1, frontier_2
    # the visited list keeps track of all the visited urls in the frontier_1
    count = 2  # initialized to 4 as we have 4 seeds
    visited = []
    crawl_count = 0

    # maintaining inlink and outlink maps
    url_inlink_map = {}
    url_outlink_map = {}

    url_inlink_map[canon1] = []
    url_inlink_map[canon2] = []
    #url_inlink_map[canon3] = []
    #url_inlink_map[canon4] = []

    prev = ""

    while (frontier_1 or frontier_2) and crawl_count < 20:
            while frontier_1 and crawl_count < 20:
                try:
                    heapq.heapify(frontier_1)
                    url_list = heapq.heappop(frontier_1)
                    url = url_list.pop(2)

                    frontierList_1.remove(url)
                    if not url in visited:
                        try:
                            resp = requests.head(url)
                        except:
                            continue
                        try:
                            raw_html = requests.get(url)
                        except:
                            continue
                        crawl_count = crawl_count + 1
                        visited.append(url)
                        print "frontier_1 url:" + url
                        soup = BeautifulSoup(raw_html.text)
                        for script in soup(["script", "style"]):
                            script.extract()
                        clean_text = soup.get_text()

                        outlink_list = []
                        inlink_list = []

                        for link in soup.findAll('a'):
                            full_url = urljoin(url, link.get('href'))
                            parsed_uri = urlparse(full_url)
                            domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
                            robot_file = domain + "robots.txt"
                            rp.set_url(robot_file)
                            rp.read()
                            if rp.can_fetch("*", full_url):
                                try:
                                    head_res = requests.head(full_url)
                                except:
                                    continue
                                if not 'content-type' in head_res.headers:
                                    continue
                                else:
                                    page_type = head_res.headers['content-type']

                                canon_url = canonURL(full_url)

                                if "html" in page_type and not canon_url in outlink_list:
                                    # add the canon_url to the outlink map of the current url
                                    outlink_list.append(canon_url)
                                    # add the current url to the inlink map of the canon_url
                                    inlink_list = []
                                    if url_inlink_map.has_key(canon_url):
                                        inlink_list = url_inlink_map[canon_url]
                                        if not url in inlink_list:
                                            inlink_list.append(url)
                                    else:
                                        inlink_list.append(url)
                                    url_inlink_map[canon_url] = inlink_list
                                    if not canon_url in visited:
                                        if not canon_url in frontierList_1:
                                            if not canon_url in frontierList_2:
                                                count = count + 1
                                                new_url_list = [-1, count, canon_url]
                                                heapq.heappush(frontier_2, new_url_list)
                                                frontierList_2.append(canon_url)

                                            else:
                                                inlink_count = frontier_2[map(itemgetter(2),frontier_2).index(canon_url)][0]
                                                inlink_count = inlink_count - 1
                                                frontier_2[map(itemgetter(2),frontier_2).index(canon_url)][0] = inlink_count

                                        else:
                                            inlink_count = frontier_1[map(itemgetter(2),frontier_1).index(canon_url)][0]
                                            inlink_count = inlink_count - 1
                                            frontier_1[map(itemgetter(2),frontier_1).index(canon_url)][0] = inlink_count

                        url_outlink_map[url] = outlink_list
                        print "size of outlink list:" + str(outlink_list.__len__())
                        op = es.index(index="ir_hw3", doc_type="page", id=url, body={"clean_text": clean_text, "raw_html": raw_html.text, "in_links" : url_inlink_map[url], "out_links" : outlink_list})
                        total_time = datetime.now() - start_time
                        print total_time
                except Exception, e:
                    print e
                    continue

            while frontier_2 and crawl_count < 20:
                try:
                    heapq.heapify(frontier_2)
                    url_list = heapq.heappop(frontier_2)
                    url = url_list.pop(2)

                    frontierList_2.remove(url)
                    if not url in visited:
                        try:
                            resp = requests.head(url)
                        except:
                            continue
                        try:
                            raw_html = requests.get(url)
                        except:
                            continue
                        crawl_count = crawl_count + 1
                        visited.append(url)
                        print "frontier_2 url:" + url
                        soup = BeautifulSoup(raw_html.text)
                        for script in soup(["script", "style"]):
                            script.extract()
                        clean_text = soup.get_text()

                        outlink_list = []
                        inlink_list = []

                        for link in soup.findAll('a'):
                            full_url = urljoin(url, link.get('href'))
                            parsed_uri = urlparse(full_url)
                            domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
                            robot_file = domain + "robots.txt"
                            rp.set_url(robot_file)
                            rp.read()
                            if rp.can_fetch("*", full_url):
                                try:
                                    head_res = requests.head(full_url)
                                except:
                                    continue
                                if not 'content-type' in head_res.headers:
                                    continue
                                else:
                                    page_type = head_res.headers['content-type']


                                canon_url = canonURL(full_url)

                                if "html" in page_type and not canon_url in outlink_list:
                                    # add the canon_url to the outlink map of the current url
                                    outlink_list.append(canon_url)
                                    # add the current url to the inlink map of the canon_url
                                    inlink_list = []
                                    if url_inlink_map.has_key(canon_url):
                                        inlink_list = url_inlink_map[canon_url]
                                        if not url in inlink_list:
                                            inlink_list.append(url)
                                    else:
                                        inlink_list.append(url)
                                    url_inlink_map[canon_url] = inlink_list
                                    if not canon_url in visited:
                                        if not canon_url in frontierList_2:
                                            if not canon_url in frontierList_1:
                                                count = count + 1
                                                new_url_list = [-1, count, canon_url]
                                                heapq.heappush(frontier_1, new_url_list)
                                                frontierList_1.append(canon_url)

                                            else:
                                                inlink_count = frontier_1[map(itemgetter(2),frontier_1).index(canon_url)][0]
                                                inlink_count = inlink_count - 1
                                                frontier_1[map(itemgetter(2),frontier_1).index(canon_url)][0] = inlink_count

                                        else:
                                            inlink_count = frontier_2[map(itemgetter(2),frontier_2).index(canon_url)][0]
                                            inlink_count = inlink_count - 1
                                            frontier_2[map(itemgetter(2),frontier_2).index(canon_url)][0] = inlink_count

                                    else:
                                        continue

                        url_outlink_map[url] = outlink_list
                        print "size of outlink list:" + str(outlink_list.__len__())
                        op = es.index(index="ir_hw3", doc_type="page", id=url, body={"clean_text": clean_text, "raw_html": raw_html.text, "in_links" : url_inlink_map[url], "out_links" : outlink_list})
                        total_time = datetime.now() - start_time
                        print total_time
                except Exception, e:
                    print e
                    continue

    print "the length of visited list is:"
    print visited.__len__()
    print visited

    total_time = datetime.now() - start_time
    print total_time