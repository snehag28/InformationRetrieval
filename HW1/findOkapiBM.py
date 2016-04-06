__author__ = 'snehagaikwad'

from wordStemmer import PorterStemmer
from elasticsearch import Elasticsearch
import os
from datetime import datetime
import operator
from decimal import Decimal
import math

if __name__ == "__main__":
    start_time = datetime.now()
    no_of_docs = 84679

    # DEFINING  CONSTANTS for Okapi BM25 language model
    okapi_BM_b = 0.75
    okapi_BM_k1 = 1.2
    okapi_BM_k2 = 0


    output_file_bm = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/okapi_bm_output.txt"
    of = open(output_file_bm, 'w')

    sum_doc_len = 0
    doc_len_file = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/document_length.txt"
    doc_len_map = {}
    with open(doc_len_file, 'r') as dl:
        for dl_line in iter(dl):
            doc_id = dl_line.split()[0]
            doc_len = dl_line.split()[1]
            doc_len_map[doc_id] = doc_len
            sum_doc_len += int(doc_len)
        avg_len_d = Decimal(sum_doc_len) / Decimal(no_of_docs)

    # initializing the elasticsearch variable and creating a script
    es = Elasticsearch()

    # the stemmer object
    ps = PorterStemmer();

    # converting the query terms to their stems and removing the stopwords from the queries
    query_file = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/query_desc.51-100.short.txt"
    stopwords_path = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/stoplist.txt"
    with open(stopwords_path, 'r') as sf:
        stopList = sf.read().replace('\n', ' ')
    with open(query_file, 'r') as f:
        lines = f.readlines()

    queries = {}  # map to store the comma separated stemmed query words against the query number
    word_dfw_map = {}  # map that stores the term and the no of docs that have the term i.e. dfw
    term_id_tf_map = {}  # map TF(w,d) that stores id_tf_map(doc_id,TF) for each query_word
    for query in lines:
        line = query.lower()
        words = line.split()
        query_number = words[0].strip('.')
        queries[query_number] = ""

        okapi_score = 0
        query_tf_map = {}
        for word in words[4:]:
            if word not in stopList:
                w = word.strip(',."()')
                query_word = ps.stem(w, 0, len(w) - 1)
                if query_tf_map.has_key(query_word):
                    query_tf_map[query_word] = query_tf_map[query_word]+1
                else:
                    query_tf_map[query_word] = 1

        bm_query_result_map ={}
        for query_word in query_tf_map.keys():
            result = es.search(
                index="ir_hw1",
                doc_type="test",
                body={
                    "query": {
                        "function_score": {
                            "query": {
                                "match": {
                                    "text": query_word
                                }
                            },
                            "functions": [
                                {
                                    "script_score": {
                                        "script_id": "getTF",
                                        "lang": "groovy",
                                        "params": {
                                            "term": query_word,
                                            "field": "text"
                                        }
                                    }
                                }
                            ],
                            "boost_mode": "replace"
                        }
                    },
                    "size": 84679,
                    "fields": ["stream_id"]
                }
            )
            dfw = result['hits']['total']
            bm25_val = 0
            for doc in result['hits']['hits']:
                doc_id = str(doc['_id'])
                tf_w_d = int(doc['_score'])
                tf_w_q = int(query_tf_map[query_word])
                doc_len = int(doc_len_map[doc_id])
                len_by_avg_len = float(float(doc_len)/float(avg_len_d))
                first_term = math.log10(float(no_of_docs+0.5)/float(dfw + 0.5))
                second_term = float(tf_w_d * float(1 + okapi_BM_k1))/float(tf_w_d + float(okapi_BM_k1 * float(float(1-okapi_BM_b) + float(okapi_BM_b*len_by_avg_len))))
                third_term = float(tf_w_q * float(1+okapi_BM_k2)) / float(tf_w_q + okapi_BM_k2)
                bm25_val = float(first_term * second_term * third_term)

                if bm_query_result_map.has_key(doc_id):
                    bm_query_result_map[doc_id] = bm25_val + bm_query_result_map[doc_id]
                else:
                    bm_query_result_map[doc_id] = bm25_val

        if bm_query_result_map.__len__() > 1000:
            bm_query_result_map_sorted = sorted(bm_query_result_map.items(), key=lambda x:x[1], reverse=True)[:1000]
        else:
            bm_query_result_map_sorted = sorted(bm_query_result_map.items(), key=lambda x:x[1], reverse=True)

        count = 0
        for id in bm_query_result_map_sorted:
            count += 1
            of.write(str(query_number) + "\t Q0 \t" + str(id[0]) + "\t" + str(count) + "\t" + format(id[1], '.10f') + "\t Exp \n")

    of.close()
    total_time = datetime.now() - start_time
    print total_time



