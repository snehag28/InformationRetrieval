__author__ = 'snehagaikwad'

from datetime import datetime
import os
import math

# P -> set_of_pages
# N = |P|
# S -> set_of_sinks
# M(p) -> set of inlinks to p
# L(q) -> number of outlinks from q
# d is the PageRank damping/teleportation factor; use d = 0.85 as is typical
# PageRank[p] -> stores pagerank of p
# Entropy: Sum(P(xi)*(1/log(P(xi))))
# Perplexity: 2^Entropy

def entropy(pagerank_hash):
    entropy_value = 0
    for page in pagerank_hash.keys():
        p = pagerank_hash[page]
        log_val = math.log((float(1)/float(p)),2)
        temp = float(p * log_val)
        entropy_value += temp
    return entropy_value

def perplexity(pagerank_hash):
    entropy_value = entropy(pagerank_hash)
    perplexity_value = math.pow(2,entropy_value)
    return perplexity_value

def difference(val1, val2):
    temp = val1 - val2
    return math.fabs(temp)

if __name__ == '__main__':
    start_time = datetime.now()
    #inlink_file = "/Users/snehagaikwad/Documents/IR/new_temp_graph.txt"
    inlink_file = "/Users/snehagaikwad/Documents/IR/wt2g_inlinks.txt"

    d = 0.85

    inlinks_map = {}
    outlinks_map = {}
    set_of_pages = set()
    with open(inlink_file,'r') as lines:
        for line in lines:
            link = line.split()[0]
            set_of_pages.add(link)
            inlinks = set(line.split()[1:])
            inlinks_map[link] = inlinks
            for inlink in inlinks:
                if outlinks_map.has_key(inlink):
                    outlinks_map[inlink].append(link)
                else:
                    outlinks_map[inlink] = [link]
                outlinks_map[inlink] = list(set(outlinks_map[inlink]))

    set_of_sinks = []
    for node in inlinks_map.keys():
        if not node in outlinks_map:
            set_of_sinks.append(node)

    print "P - set of pages: " + str(set_of_pages.__len__())
    print "S - set of sink nodes: " + str(set_of_sinks.__len__())
    print "L - outlink map: " + str(outlinks_map.__len__())
    print "M - inlink map:" + str(inlinks_map.__len__())
    N = set_of_pages.__len__()

    PageRank = {}
    for link in set_of_pages:
        PageRank[link] = 1.0/float(N)

    prev_perplexity_value = perplexity(PageRank)

    iteration = 0
    newPR = {}
    count = 0
    while 1:
        count += 1
        sinkPR = 0.0
        for page in set_of_sinks:
            sinkPR += float(PageRank[page])
        for page in set_of_pages:
            newPR[page] = (1.0-d)/float(N)
            newPR[page] += (d * sinkPR/float(N))
            for q in inlinks_map[page]:
                temp = outlinks_map[q]
                newPR[page] += (d * PageRank[q]) / float(temp.__len__())
            PageRank[page] = newPR[page]
        current_perplexity = perplexity(PageRank)
        if difference(prev_perplexity_value,current_perplexity) < 1 and iteration < 4:
            iteration += 1
        elif difference(prev_perplexity_value,current_perplexity) > 1:
            iteration = 0
        else:
            break
        prev_perplexity_value = current_perplexity


    pagerank_file = "pagerank.txt"
    pagerank_sorted = sorted(PageRank.items(), key=lambda x:x[1], reverse=True)

    fh = open(pagerank_file,'w')
    count = 0
    for page in pagerank_sorted:
        count += 1
        fh.write(str(count) + " " + page[0] + ": " + str(page[1]) + " " + str(inlinks_map[page[0]].__len__()) +"\n")

    fh.close()

    total_time = datetime.now() - start_time
    print total_time
    print count
