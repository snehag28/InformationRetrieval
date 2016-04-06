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
    D = 84679

    output_file_okapi = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/okapi_output.txt"
    of_okapi = open(output_file_okapi, 'w')

    output_file_tf_idf = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/tf_idf_output.txt"
    of_tf_idf = open(output_file_tf_idf, 'w')

    sum_doc_len = 0
    doc_len_file = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/document_length.txt"
    doc_len_map = {}
    with open(doc_len_file, 'r') as dl:
        for dl_line in iter(dl):
            doc_id = dl_line.split()[0]
            doc_len = dl_line.split()[1]
            doc_len_map[doc_id] = doc_len
            sum_doc_len += int(doc_len)
        avg_len_d = Decimal(sum_doc_len) / Decimal(D)
        print avg_len_d

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

        okapi_query_result_map ={}
        tf_idf_query_result_map = {}
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
            okapi_val = 0
            tf_idf_val = 0
            for doc in result['hits']['hits']:
                doc_id = str(doc['_id'])
                tf_w_d = int(doc['_score'])
                doc_len = int(doc_len_map[doc_id])
                len_by_avg_len = float(float(doc_len)/float(avg_len_d))
                okapi_val = float(float(tf_w_d)/float(tf_w_d + 0.5 + float(1.5 * len_by_avg_len)))
                tf_idf_val = float(okapi_val * float(math.log10(float(D/dfw))))

                if okapi_query_result_map.has_key(doc_id):
                    okapi_query_result_map[doc_id] = okapi_val + okapi_query_result_map[doc_id]
                else:
                    okapi_query_result_map[doc_id] = okapi_val

                if tf_idf_query_result_map.has_key(doc_id):
                    tf_idf_query_result_map[doc_id] = tf_idf_val + tf_idf_query_result_map[doc_id]
                else:
                    tf_idf_query_result_map[doc_id] = tf_idf_val

        if okapi_query_result_map.__len__() > 1000:
            okapi_query_result_map_sorted = sorted(okapi_query_result_map.items(), key=lambda x:x[1], reverse=True)[:1000]
        else:
            okapi_query_result_map_sorted = sorted(okapi_query_result_map.items(), key=lambda x:x[1], reverse=True)

        count = 0
        for id in okapi_query_result_map_sorted:
            count += 1
            of_okapi.write(str(query_number) + "\t Q0 \t" + str(id[0]) + "\t" + str(count) + "\t" + format(id[1], '.10f') + "\t Exp \n")

        if tf_idf_query_result_map.__len__() > 1000:
            tf_idf_query_result_map_sorted = sorted(tf_idf_query_result_map.items(), key=lambda x:x[1], reverse=True)[:1000]
        else:
            tf_idf_query_result_map_sorted = sorted(tf_idf_query_result_map.items(), key=lambda x:x[1], reverse=True)

        count = 0
        for id in tf_idf_query_result_map_sorted:
            count += 1
            of_tf_idf.write(str(query_number) + "\t Q0 \t" + str(id[0]) + "\t" + str(count) + "\t" + format(id[1], '.10f') + "\t Exp \n")

    of_okapi.close()
    of_tf_idf.close()
    total_time = datetime.now() - start_time
    print total_time