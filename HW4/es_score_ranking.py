__author__ = 'snehagaikwad'

from elasticsearch import Elasticsearch
import os
from datetime import datetime

if __name__ == '__main__':
    start_time = datetime.now()
    es_score_file = "/Users/snehagaikwad/Dropbox/CS6200_S_Gaikw/HW4/es_score.txt"
    fh = open(es_score_file, 'w')

    es = Elasticsearch()

    #queries = ["Michael Jordan retirement", "Lebron James team change", "Kobe Bryant records"]
    query_num_map = {}
    query_num_map[15011] = "Michael Jordan retirement"
    query_num_map[15012] = "Lebron James team change"
    query_num_map[15013] = "Kobe Bryant records"

    for query_num in query_num_map.keys():
        query = query_num_map[query_num]
        result = es.search(
                index="basketball",
                doc_type="page",
                body={
                    "_source": {
                        "exclude": [ "raw_html", "outlinks", "inlinks","clean_text" ]},
                        "query": {
                            "match": {
                                "clean_text" : query
                            }
                        },
                        "size" : 200
                }
            )
        count = 0
        for url in result['hits']['hits']:
            id = url['_id']
            score = url['_score']
            count += 1
            fh.write(str(query_num) + "\t Q0 \t" + id + "\t" + str(count) + "\t" + format(score, '.10f') + "\t Exp \n")
        print count

    fh.close()
    total_time = datetime.now() - start_time
    print total_time
