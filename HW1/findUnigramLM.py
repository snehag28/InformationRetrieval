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

    # DEFINING  CONSTANTS
    V = 178050

    output_file_lm = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/unigram_lm_output.txt"
    of = open(output_file_lm, 'w')

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
    term_id_tf_map = {}  # map TF(w,d) that stores id_tf_map(doc_id,TF) for each query_word

    for query in lines:
        line = query.lower()
        words = line.split()
        query_number = words[0].strip('.')
        queries[query_number] = ""
        doc_list = []

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

        lm_query_result_map ={}
        word_doc_tf_map = {}
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
            doc_tf_map = {}
            for doc in result['hits']['hits']:
                doc_id = str(doc['_id'])
                tf_w_d = int(doc['_score'])
                doc_tf_map[doc_id] = tf_w_d
                if not doc_list.__contains__(doc_id):
                    doc_list.append(doc_id)
            word_doc_tf_map[query_word] = doc_tf_map


        query_doc_lm_map ={}
        for doc_id in doc_list:
            total_lm = 0
            doc_len = doc_len_map[doc_id]
            temp_dict = {}
            for query_word in query_tf_map.keys():
                temp_dict = word_doc_tf_map[query_word]
                if temp_dict.has_key(doc_id):
                    tf_w_d = temp_dict[doc_id]
                else:
                    tf_w_d = 0
                total_lm += float(math.log10(float(tf_w_d+1)/float(float(doc_len) + float(V))))
            query_doc_lm_map[doc_id] = total_lm

        if query_doc_lm_map.__len__() > 1000:
            query_doc_lm_map_sorted = sorted(query_doc_lm_map.items(), key=lambda x:x[1], reverse=True)[:1000]
        else:
            query_doc_lm_map_sorted = sorted(query_doc_lm_map.items(), key=lambda x:x[1], reverse=True)

        count = 0
        for id in query_doc_lm_map_sorted:
            count += 1
            of.write(str(query_number) + "\t Q0 \t" + str(id[0]) + "\t" + str(count) + "\t" + format(id[1], '.10f') + "\t Exp \n")

    of.close()
    total_time = datetime.now() - start_time
    print total_time
