__author__ = 'snehagaikwad'

from bs4 import BeautifulSoup
import robotparser
from createIndexES import create_index
from elasticsearch import Elasticsearch
from datetime import datetime
import requests
from canonicalizeURL import canonURL
from urlparse import urlparse
from urlparse import urljoin
import heapq
from operator import itemgetter
import urllib2
from Canonicalization import Canonicalize
import time
import json

if __name__== "__main__":
    start_time = datetime.now()
    # instantiate es client, connects to localhost:9200 by default
    es = Elasticsearch()
    # create the index
    create_index(es)
    # instantiate robot parser
    rp = robotparser.RobotFileParser()

    # initializing the crawler with the seed URLs
    seed1 = "http://en.wikipedia.org/wiki/LeBron_James"
    canon1 = canonURL(seed1)
    seed2 = "http://www.theguardian.com/sport/2014/jul/11/lebron-james-to-return-to-cleveland"
    canon2 = canonURL(seed2)
    seed3 = "http://hoopshype.com"
    canon3 = canonURL(seed3)

    # initializing a frontier_1 as heapq
    # the heapq will have a list of [inlink count, url number, url]
    # it will first sort based on the inlink and then the count(which tells the order in which the url was visited)
    frontier_1 = []
    heapq.heappush(frontier_1, [0,1,canon1])
    heapq.heappush(frontier_1, [0,2,canon2])
    heapq.heappush(frontier_1, [0,3,canon3])

    frontier_2 = []

    # lists to maintain the list of urls being processed, make it easy to find whether the url is present in the frontier
    frontierList_1 = []
    frontierList_2 = []

    frontierList_1.append(canon1)
    frontierList_1.append(canon2)
    frontierList_1.append(canon3)

    # we maintain two priority heaps for crawling for alternate levels - frontier_1, frontier_2
    # the visited list keeps track of all the visited urls in the frontier_1
    count = 3  # initialized to 3 as we have 3 seeds
    visited = []
    crawl_count = 0

    inlink = {}
    outlink_list = []

    while (frontier_1 or frontier_2) and crawl_count < 100:
            print "frontier_1 before:" + str(frontier_1.__len__())
            frontier_1 = heapq.nsmallest(50, frontier_1)
            if frontier_1:
                frontierList_1 = list(zip(*frontier_1)[2])
            print "frontier_1 after:" + str(frontier_1.__len__())

            for key in inlink.keys():
                if key not in frontierList_1:
                    del inlink[key]

            while frontier_1 and crawl_count < 100:
                try:
                    url_list = heapq.heappop(frontier_1)
                    url = url_list.pop(2)
                    inlink_count = url_list.pop(0)
                    frontierList_1.remove(url)
                    try:
                        parsed_uri = urlparse(url)
                        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
                        robot_file = domain + "robots.txt"
                        rp.set_url(robot_file)
                        rp.read()
                    except Exception, e:
                        continue

                    try:
                        head_res = requests.head(url)
                    except Exception, e:
                        continue
                    if not 'content-type' in head_res.headers:
                        continue
                    elif "html" in head_res.headers['content-type']:
                        try:
                            time.sleep(1)
                            raw_html = urllib2.urlopen(url).read()
                        except Exception, e:
                            continue
                    crawl_count = crawl_count + 1
                    visited.append(url)
                    print "frontier_1 url:" + url + " in link count:" + str(inlink_count)
                    soup = BeautifulSoup(raw_html)
                    for script in soup(["script", "style"]):
                        script.extract()
                    clean_text = soup.get_text()

                    for link in soup.findAll('a'):
                        full_url = urljoin(url, link.get('href'))
                        try:
                            canon_url = Canonicalize(full_url,url).encode("utf-8")
                        except:
                            canon_url = Canonicalize(full_url,url)

                        if canon_url == "":
                            continue

                        if not canon_url in outlink_list:
                            outlink_list.append(canon_url)


                        try:
                            if not rp.can_fetch("*", canon_url):
                                continue
                        except:
                            continue

                        try:
                            if not canon_url in visited:
                                if not canon_url in frontierList_1:
                                    if not canon_url in frontierList_2:
                                        if frontier_2.__len__() < 10000:
                                            count = count + 1
                                            new_url_list = [-1, count, canon_url]
                                            heapq.heappush(frontier_2, new_url_list)
                                            frontierList_2.append(canon_url)
                                            inlink[canon_url] = [url]
                                    else:
                                        try:
                                            frontier_2[map(itemgetter(2),frontier_2).index(canon_url)][0] -= 1
                                            if canon_url in inlink:
                                                inlink[canon_url].append(url)
                                            else:
                                                inlink[canon_url] = [url]

                                        except Exception, e:
                                            continue
                                else:
                                    try:
                                        frontier_1[map(itemgetter(2),frontier_1).index(canon_url)][0] -= 1
                                        if canon_url in inlink:
                                            inlink[canon_url].append(url)
                                        else:
                                            inlink[canon_url] = [url]
                                    except:
                                        continue
                            else:
                                if canon_url in inlink:
                                    inlink[canon_url].append(url)
                                else:
                                    inlink[canon_url] = [url]


                        except Exception, e:
                            continue
                    op = es.index(index="sneha_1", doc_type="page", id=url, body={"clean_text": clean_text, "raw_html": raw_html, "outlinks": outlink_list})
                except Exception, e:
                    continue

            total_time = datetime.now() - start_time
            print total_time


            print "frontier_2 before:" + str(frontier_2.__len__())
            frontier_2 = heapq.nsmallest(50, frontier_2)
            if frontier_2:
                frontierList_2 = list(zip(*frontier_2)[2])
            print "frontier_2 after:" + str(frontier_2.__len__())

            for key in inlink.keys():
                if key not in frontierList_1:
                    del inlink[key]

            while frontier_2 and crawl_count < 100:
                try:
                    url_list = heapq.heappop(frontier_2)
                    url = url_list.pop(2)
                    inlink_count = url_list.pop(0)
                    frontierList_2.remove(url)

                    try:
                        parsed_uri = urlparse(url)
                        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
                        robot_file = domain + "robots.txt"
                        rp.set_url(robot_file)
                        rp.read()
                    except Exception, e:
                        continue

                    try:
                        head_res = requests.head(url)
                    except Exception, e:
                        continue
                    if not 'content-type' in head_res.headers:
                        continue
                    elif "html" in head_res.headers['content-type']:
                        try:
                            time.sleep(1)
                            raw_html = urllib2.urlopen(url).read()
                        except Exception, e:
                            continue
                    crawl_count = crawl_count + 1
                    visited.append(url)
                    print "frontier_2 url:" + url + " in link count:" + str(inlink_count)
                    soup = BeautifulSoup(raw_html)
                    for script in soup(["script", "style"]):
                        script.extract()
                    clean_text = soup.get_text()

                    for link in soup.findAll('a'):
                        full_url = urljoin(url, link.get('href'))
                        try:
                            canon_url = Canonicalize(full_url,url).encode("utf-8")
                        except:
                            canon_url = Canonicalize(full_url,url)

                        if canon_url == "":
                            continue

                        if not canon_url in outlink_list:
                            outlink_list.append(canon_url)

                        try:
                            if not rp.can_fetch("*", canon_url):
                                continue
                        except:
                            continue


                        try:
                            if not canon_url in visited:
                                if not canon_url in frontierList_2:
                                    if not canon_url in frontierList_1:
                                        if frontier_1.__len__() < 10000:
                                            count = count + 1
                                            new_url_list = [-1, count, canon_url]
                                            heapq.heappush(frontier_1, new_url_list)
                                            frontierList_1.append(canon_url)
                                            inlink[canon_url] = [url]
                                    else:
                                        try:
                                            frontier_1[map(itemgetter(2),frontier_1).index(canon_url)][0] -= 1
                                            if canon_url in inlink:
                                                inlink[canon_url].append(url)
                                            else:
                                                inlink[canon_url] = [url]
                                        except Exception, e:
                                            continue
                                else:
                                    try:
                                        frontier_2[map(itemgetter(2),frontier_2).index(canon_url)][0] -= 1
                                        if canon_url in inlink:
                                            inlink[canon_url].append(url)
                                        else:
                                            inlink[canon_url] = [url]
                                    except:
                                        continue
                            else:
                                if canon_url in inlink:
                                    inlink[canon_url].append(url)
                                else:
                                    inlink[canon_url] = [url]

                        except Exception, e:
                            continue
                    op = es.index(index="sneha_1", doc_type="page", id=url, body={"clean_text": clean_text, "raw_html": raw_html, "outlinks": outlink_list})
                except Exception, e:
                    continue

            total_time = datetime.now() - start_time
            print total_time

    print "the length of visited list is:"
    print visited.__len__()
    print visited
    filename = "visited_urls_sneha_1.txt"
    f = open(filename,"w")
    for link in visited:
        f.write(link + "\n")
    f.close()

    with open("inlink_file_sneha_1.json",'w') as output:
        json.dump(inlink,output)

    total_time = datetime.now() - start_time
    print total_time

