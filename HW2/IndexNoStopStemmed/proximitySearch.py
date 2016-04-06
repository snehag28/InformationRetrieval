__author__ = 'snehagaikwad'

from wordStemmer import PorterStemmer
import os
from datetime import datetime
import operator
from decimal import Decimal
import math
import sys

# returns the doc_positions_map for a term
def getTermPositions(query_word,ifp):
    doc_positions_map = {}
    if query_word in term_offset_map:
        offset = term_offset_map[query_word]
        ifp.seek(offset,0)
        line = ifp.readline()
        fields = line.split("-")
        d_blocks = fields[2]
        d_block_list = d_blocks.split(",")
        for doc in d_block_list:
            doc_fields = doc.split(":")
            doc_id = doc_fields[0]
            tf_pos_list = doc_fields[1].split("|")
            pos_list = tf_pos_list[1:]
            pos_list = map(int,pos_list)
            doc_positions_map[doc_id] = pos_list
    return doc_positions_map

def getSpan(word_pos_map):
    matrix = []
    exhaustedArr = [];
    for word in word_pos_map.keys():
        pos_list = word_pos_map.setdefault(word,[])
        pos_list_sorted = sorted(pos_list)
        matrix.append(pos_list_sorted)
    matrix_len = matrix.__len__()
    min_span =  sys.maxint
    span_list = []
    row_column_index_map = {}
    j = 0
    for i in range (0,matrix_len,1):
        span_list = span_list + [int(matrix[i][j])]
        row_column_index_map[i] = j
    while exhaustedArr.__len__() < matrix_len:
        span, index = calculate_span(span_list)
        if span < min_span:
            min_span = span
        j = row_column_index_map[index] + 1
        if j > matrix[index].__len__() - 1:
            j = j - 1
            exhaustedArr.append(index)
        span_list[index] = int(matrix[index][j])
        row_column_index_map[index] = j
    return min_span

def calculate_span(span_list):
    max_pos = max(span_list)
    min_pos = min(span_list)
    span = max_pos - min_pos
    index = span_list.index(min_pos)
    return (span, index)

if __name__ == '__main__':
    start_time = datetime.now()

    output_file_prox = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/IndexNoStopStemmed/proximity_output.txt"
    of = open(output_file_prox, 'w')

    indexFile = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/IndexNoStopStemmed/index/invertedIndex.txt"
    ifp = open(indexFile,'r')

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

    doc_len_file = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/document_length.txt"
    doc_len_map = {}
    with open(doc_len_file, 'r') as dl:
        for dl_line in iter(dl):
            doc_id = dl_line.split()[0]
            doc_len = dl_line.split()[1]
            doc_len_map[doc_id] = int(doc_len)

    # DEFINING  CONSTANTS
    C = 1500
    indexParamsFile = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/IndexNoStopStemmed/indexParams.txt"
    ipf = open(indexParamsFile,'r')
    param = ipf.readline()
    params = param.split("\t")
    V = int(params[1])

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
    term_doc_position_map = {}
    doc_list = []

    for query in lines:
        if not query == "\n":
            line = query.lower()
            words = line.split()
            query_number = words[0].strip('.')
            print "processing query number " + query_number + "..."
            queries[query_number] = ""

            query_tf_map = {}
            query_word_count = 0
            for word in words[4:]:
                #print word
                query_word_count += 1
                if word not in stopList:
                    w = word.strip(',."()')
                    query_word = ps.stem(w, 0, len(w) - 1)
                    if query_tf_map.has_key(query_word):
                        query_tf_map[query_word] = query_tf_map[query_word]+1
                    else:
                        query_tf_map[query_word] = 1


            doc_list = []
            print query_tf_map.keys()
            for query_word in query_tf_map.keys():
                # the term_doc_position_map will be a map of term and doc_positions_map
                term_doc_position_map[query_word] = getTermPositions(query_word,ifp)
                temp_map = {}
                temp_map = term_doc_position_map[query_word]
                doc_list = doc_list + temp_map.keys()
            #lambda_base = 0.8
            doc_proximity_map = {}
            unique_docs = set(doc_list)
            for doc_id in unique_docs:
                    word_pos_map = {}
                    doc_name = doc_name_id_map[doc_id]
                    query_word_list = query_tf_map.keys()
                    numberOfContainTerms = 0
                    for query_word in query_word_list:
                        if doc_id in term_doc_position_map[query_word]:
                            word_pos_map[query_word] = term_doc_position_map[query_word][doc_id]
                    numberOfContainTerms = word_pos_map.__len__()
                    rangeOfWindow = getSpan(word_pos_map)
                    lengthOfDoc = doc_len_map[doc_name]
                    numerator = float((C-rangeOfWindow) * numberOfContainTerms)
                    denominator = float (lengthOfDoc + V)
                    proximity_score = float(numerator/denominator)
                    doc_proximity_map[doc_name] = proximity_score

            if doc_proximity_map.__len__() > 1000:
                doc_proximity_map_sorted = sorted(doc_proximity_map.items(), key=lambda x:x[1], reverse=True)[:1000]
            else:
                doc_proximity_map_sorted = sorted(doc_proximity_map.items(), key=lambda x:x[1], reverse=True)

            count = 0
            for id in doc_proximity_map_sorted:
                count += 1
                of.write(str(query_number) + "\t Q0 \t" + str(id[0]) + "\t" + str(count) + "\t" + format(id[1], '.10f') + "\t Exp \n")

    of.close()
    total_time = datetime.now() - start_time
    print total_time
