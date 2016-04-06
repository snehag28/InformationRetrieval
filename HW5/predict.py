__author__ = 'snehagaikwad'

import numpy
from sklearn import tree
from operator import itemgetter
from datetime import datetime



def create_trec(predicted_label_array,trec_file,matrix_file,doc_num_file):
    query_output_map = dict()
    doc_id_map = {}
    with open(doc_num_file) as doc_file:
        for line in doc_file:
            fields = line.split()
            doc_id_map[fields[1]] = fields[0]

    with open(matrix_file) as lines:
        row_num = 0
        for line in lines:
            fields = line.split()
            query_id = fields[0]
            doc_id = doc_id_map[fields[1]]
            if query_id in query_output_map:
                query_output_map[query_id].append((doc_id,predicted_label_array[row_num]))
            else:
                query_output_map[query_id] = [(doc_id,predicted_label_array[row_num])]
            row_num = row_num + 1

    f = open(trec_file, 'w')
    for query_id in query_output_map:
        result = query_output_map[query_id]
        result.sort(key=itemgetter(1))
        count = 1
        for r in result:
            f.write(str(query_id) + "\t" + 'Q0'+ "\t" + str(r[0]) + "\t" +  str(count) + "\t" + str(r[1]) + "\t" +  'Exp' + "\n")
            count += 1

if __name__ == '__main__':
    start_time = datetime.now()

    doc_num_train_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/document_num_train.txt"
    doc_num_test_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/document_num_test.txt"
    train_feature_matrix_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/train_feature_matrix.txt"
    test_feature_matrix_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/test_feature_matrix.txt"
    train_trec_eval_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/train_trec_eval.txt"
    test_trec_eval_file = "/Users/snehagaikwad/Documents/IR/IR_data/AP_DATA/HW5/test_trec_eval.txt"

    # populating the x_axis and y_axis of the decision tree
    x_axis_array = numpy.loadtxt(train_feature_matrix_file,usecols=(2, 3, 4, 5, 6, 7))
    y_axis_array = numpy.loadtxt(train_feature_matrix_file,usecols=(8,))

    # creating a classifier object
    classifier_obj = tree.DecisionTreeClassifier()
    classifier_obj = classifier_obj.fit(x_axis_array,y_axis_array)

    print classifier_obj.feature_importances_

    # predicting the training dataset
    print "Predicting the training dataset..."
    train_result = classifier_obj.predict(x_axis_array)



    # creating a trec document for training dataset
    create_trec(train_result, train_trec_eval_file,train_feature_matrix_file,doc_num_train_file)

    # predicting the testing dataset
    print "Predicting the testing dataset..."
    x_axis_array_test = numpy.loadtxt(test_feature_matrix_file,usecols=(2, 3, 4, 5, 6, 7))
    test_result = classifier_obj.predict(x_axis_array_test)

    # creating a trec document for training dataset
    create_trec(test_result,test_trec_eval_file,test_feature_matrix_file,doc_num_test_file)

    print "Done..."
    total_time = datetime.now() - start_time
    print total_time
