__author__ = 'snehagaikwad'
from collections import defaultdict
from collections import OrderedDict
from datetime import datetime

def get_docs(queries,filename):

    with open("/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/qrels.adhoc.51-100.AP89.txt") as qrel_lines:
        for line in qrel_lines:
            fields = line.split()
            query_num = fields[0]
            if query_num in queries:
                doc_def[query_num].add(fields[2])
                label_dict[(query_num,fields[2])] = fields[3]

    for q_no in doc_def.iterkeys():
        docs = doc_def[q_no]
        for doc in docs:
            doc_list.add(doc)

    count = 1
    for doc in doc_list:
        doc_dict[doc] = count
        count = count + 1

    sorted_docs = OrderedDict(sorted(doc_dict.items(), key=lambda x:x[1], reverse=True))

    with open(filename, "w") as fw:
        for key in sorted_docs.iterkeys():
            fw.write(str(key) + " " + str(doc_dict[key]) + "\n")

def feature_dict(filename,query_num,doc,f_dict):
    with open(filename) as file_content:
        for line in file_content:
            if line.__contains__(query_num) and line.__contains__(doc):
                list = line.split()
                f_dict[(query_num,doc)] = list[4]


def create_matrix(queries, matrix_filename):
    okapi_dict = {}
    tf_idf_dict = {}
    bm25_dict = {}
    unigram_lm_dict = {}
    unigram_jm_dict = {}
    proximity_dict = {}

    okapi_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/okapi_output.txt"
    tf_idf_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/tf_idf_output.txt"
    bm25_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/okapi_bm_output.txt"
    unigram_lm_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/unigram_lm_output.txt"
    unigram_jm_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/unigram_jm_output.txt"
    proximity_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/proximity_output.txt"

    for query in queries:
        docs = doc_def[query]
        for doc in docs:
            #okapi
            feature_dict(okapi_file,query,doc,okapi_dict)

            #tf_idf
            feature_dict(tf_idf_file,query,doc,tf_idf_dict)

            #bm25
            feature_dict(bm25_file,query,doc,bm25_dict)

            #unigram_lm
            feature_dict(unigram_lm_file,query,doc,unigram_lm_dict)

            #unigram jm
            feature_dict(unigram_jm_file,query,doc,unigram_jm_dict)

            #proximity
            feature_dict(proximity_file,query,doc,proximity_dict)

    matrix_fw = open(matrix_filename, "w")

    for query in queries:
        docs = doc_def[query]
        for doc in docs:
            okapi = 0
            tf_idf = 0
            bm25 = 0
            unigram_lm = 0
            unigram_jm = 0
            proximity = 0
            label = 0
            doc_num = doc_dict[doc]

            if (query,doc) in okapi_dict.keys():
                okapi = okapi_dict[(query,doc)]

            if (query,doc) in tf_idf_dict.keys():
                tf_idf = tf_idf_dict[(query,doc)]

            if (query,doc) in bm25_dict.keys():
                bm25 = bm25_dict[(query,doc)]

            if (query,doc) in unigram_lm_dict.keys():
                unigram_lm = unigram_lm_dict[(query,doc)]

            if (query,doc) in unigram_jm_dict.keys():
                unigram_jm = unigram_jm_dict[(query,doc)]

            if (query,doc) in proximity_dict.keys():
                proximity = proximity_dict[(query,doc)]

            if (query,doc) in label_dict.keys():
                label = label_dict[(query,doc)]

            matrix_fw.write(str(query) + " " + str(doc_num) + " " + str(okapi) + " " + str(tf_idf) + " " + str(bm25) + " " + str(unigram_lm) + " " + str(unigram_jm) + " " + str(proximity) + " " + str(label) + "\n")


if __name__ == '__main__':

    start_time = datetime.now()
    train_queries = set()
    doc_list = set()
    doc_dict = {}
    doc_def = defaultdict(set)
    test_queries = set()
    label_dict = {}

    doc_num_train_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/document_num_train.txt"
    doc_num_test_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/document_num_test.txt"

    feature_matrix_train_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/train_feature_matrix.txt"
    feature_matrix_test_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/test_feature_matrix.txt"

    print "Get documents..."

    with open("/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/query_desc.51-100.short.txt") as lines:
        for line in lines:
            fields = line.split(".")
            train_queries.add(fields[0])

    test_set = set(['85','62','77','100','68'])
    #test_set = set(['85','59','56','71','64'])
    while test_set.__len__() > 0:
        temp = test_set.pop()
        train_queries.remove(temp)
        test_queries.add(temp)

    print "Training set of queries:"
    print train_queries
    print "Testing set of queries:"
    print test_queries

    # getting the train feature matrix
    print "Get train documents..."
    get_docs(train_queries,doc_num_train_file)

    print "Create train feature matrix..."
    create_matrix(train_queries,feature_matrix_train_file)

    # getting the test feature matrix
    print "Get test documents..."
    get_docs(test_queries,doc_num_test_file)

    print "Create test feature matrix..."
    create_matrix(test_queries,feature_matrix_test_file)

    print "Done..."
    total_time = datetime.now() - start_time
    print total_time