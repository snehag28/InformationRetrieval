__author__ = 'snehagaikwad'

from wordStemmer import PorterStemmer
import os
from datetime import datetime
import operator
from decimal import Decimal
import math

def getTermAttributes(query_word,ifp):
    result = {}
    ttf=0
    if query_word in term_offset_map:
        offset = term_offset_map[query_word]
        ifp.seek(offset,0)
        line = ifp.readline()
        fields = line.split("-")
        ttf = int(fields[1])
        d_blocks = fields[2]
        d_block_list = d_blocks.split(",")
        dfw = d_block_list.__len__()
        result['dfw'] = dfw
        hit_list = []
        for doc in d_block_list:
            doc_tf_map = {}
            doc_fields = doc.split(":")
            doc_id = doc_fields[0]
            tf_pos_list = doc_fields[1].split("|")
            tf = tf_pos_list[0]
            doc_tf_map['id'] = doc_id
            doc_tf_map['tf'] = tf
            hit_list.append(doc_tf_map)
        result['hits'] = hit_list
    else:
        result['dfw'] = 0
        result['hits'] = []
    return (result,ttf)

if __name__ == "__main__":
    start_time = datetime.now()

    # DEFINING  CONSTANTS
    lambda_jm = 0.5

    indexFile = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/IndexNoStopStemmed/index/invertedIndex.txt"
    ifp = open(indexFile,'r')

    output_file_bm = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/IndexNoStopStemmed/unigram_jm_output.txt"
    of = open(output_file_bm, 'w')

    offsetFile = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/IndexNoStopStemmed/catalogue/catalogue.txt"
    term_offset_map = {}
    with open(offsetFile) as lines:
        for line in iter(lines):
            fields = line.split(":")
            term = fields[0]
            offset = long(fields[1])
            term_offset_map[term] = offset

    doc_name_id_map = {}
    docNameFile = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/IndexNoStopStemmed/docNames.txt"
    with open(docNameFile) as docs:
        for doc in docs:
            doc_num = doc.split()[0]
            doc_id = doc.split()[1]
            doc_name_id_map[doc_num] = doc_id

    sum_doc_len = 0
    doc_len_file = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/document_length.txt"
    doc_len_map = {}
    with open(doc_len_file, 'r') as dl:
        for dl_line in iter(dl):
            doc_id = dl_line.split()[0]
            doc_len = dl_line.split()[1]
            doc_len_map[doc_id] = int(doc_len)
            sum_doc_len += int(doc_len)
    total_doc_len = sum_doc_len
    print total_doc_len

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
    doc_list = []
    for query in lines:
        if not query == "\n":
            line = query.lower()
            words = line.split()
            query_number = words[0].strip('.')
            print "processing query number " + query_number + "..."
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

            jm_query_result_map ={}
            word_doc_tf_map = {}
            word_ttf_map = {}
            for query_word in query_tf_map.keys():
                result,ttf = getTermAttributes(query_word,ifp)
                doc_tf_map = {}
                doc_tuples = result.setdefault('hits',[])
                for doc in doc_tuples:
                    doc_num = doc['id']
                    doc_id = doc_name_id_map[doc_num]
                    tf_w_d = int(doc['tf'])
                    doc_tf_map[doc_id] = tf_w_d
                    if not doc_list.__contains__(doc_id):
                        doc_list.append(doc_id)
                word_doc_tf_map[query_word] = doc_tf_map
                word_ttf_map[query_word] = ttf

            query_doc_jm_map ={}
            for doc_id in doc_list:
                total_jm = 0
                doc_len = doc_len_map[doc_id]
                doc_len_dash = total_doc_len - doc_len
                for query_word in query_tf_map.keys():
                    if word_doc_tf_map[query_word].has_key(doc_id):
                        tf_w_d = word_doc_tf_map[query_word][doc_id]
                    else:
                        tf_w_d = 0

                    tf_w_d_dash = word_ttf_map[query_word] - tf_w_d
                    first_term = float(lambda_jm * float(float(tf_w_d) / float(doc_len)))
                    second_term = float(float(1-lambda_jm) * float (float(tf_w_d_dash)/float(doc_len_dash)))
                    sum_of_terms = float(first_term + second_term)
                    if sum_of_terms != 0:
                        total_jm += math.log(sum_of_terms)

                query_doc_jm_map[doc_id] = total_jm

            if query_doc_jm_map.__len__() > 1000:
                query_doc_jm_map_sorted = sorted(query_doc_jm_map.items(), key=lambda x:x[1], reverse=True)[:1000]
            else:
                query_doc_jm_map_sorted = sorted(query_doc_jm_map.items(), key=lambda x:x[1], reverse=True)

            count = 0
            for id in query_doc_jm_map_sorted:
                count += 1
                of.write(str(query_number) + "\t Q0 \t" + str(id[0]) + "\t" + str(count) + "\t" + format(id[1], '.10f') + "\t Exp \n")

    of.close()
    total_time = datetime.now() - start_time
    print total_time
