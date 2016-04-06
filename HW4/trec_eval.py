__author__ = 'snehagaikwad'

import sys
import math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def draw(prec_array,query_num):
    recall_array = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    plt.plot(recall_array,prec_array)
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.grid(True)
    plt.title('Interpolated Recall - Precision Averages')
    name = "prec_recall_plot_qid_"+str(query_num)
    plt.savefig(name)
    plt.close()

def check_args(args):
    print_all_queries = 0
    if len(args) < 2 or len(args) > 3:
        print "Usage: trec_eval [-q] <qrel_file> <trec_file>"
        sys.exit(2)

    if len(args) == 3:
        if args[0] == "-q":
            print_all_queries = 1
        else:
            print "Usage: trec_eval [-q] <qrel_file> <trec_file>"
            sys.exit(2)
        qrel_file = args[1]
        trec_file = args[2]
    if len(args) == 2:
        if args[0] == "-q":
            print "Usage: trec_eval [-q] <qrel_file> <trec_file>"
            sys.exit(2)
        else:
            qrel_file = args[0]
            trec_file = args[1]
    return (print_all_queries, qrel_file, trec_file)

def eval_print(qid,ret,rel,rel_ret,
               p_array,
               map,
               p_doc_array,
               rp,
               f1_array,
               recall_array,
               dcg):
    draw(p_array,qid)
    print "----------------------------------------------------------------------------------------------"
    print "Queryid (Num):    " + str(qid)
    print "Total number of documents over all queries"
    print "    Retrieved:    " + str(ret)
    print "    Relevant:     " + str(rel)
    print "    Rel_ret:      " + str(int(rel_ret))

    print "Interpolated Recall - Precision Averages:"
    print "    at 0.00       %0.4f" %p_array[0]
    print "    at 0.10       %0.4f" %p_array[1]
    print "    at 0.20       %0.4f" %p_array[2]
    print "    at 0.30       %0.4f" %p_array[3]
    print "    at 0.40       %0.4f" %p_array[4]
    print "    at 0.50       %0.4f" %p_array[5]
    print "    at 0.60       %0.4f" %p_array[6]
    print "    at 0.70       %0.4f" %p_array[7]
    print "    at 0.80       %0.4f" %p_array[8]
    print "    at 0.90       %0.4f" %p_array[9]
    print "    at 1.00       %0.4f" %p_array[10]

    print "Average precision (non-interpolated) for all rel docs(averaged over queries)"
    print "                  %0.4f" %(map)
    print "                  Precision  Recall  F1-Measure"
    print "  At    5 docs:   %0.4f      %0.4f   %0.4f" %(p_doc_array[0], recall_array[0], f1_array[0])
    print "  At   10 docs:   %0.4f      %0.4f   %0.4f" %(p_doc_array[1], recall_array[1], f1_array[1])
    print "  At   15 docs:   %0.4f      %0.4f   %0.4f" %(p_doc_array[2], recall_array[2], f1_array[2])
    print "  At   20 docs:   %0.4f      %0.4f   %0.4f" %(p_doc_array[3], recall_array[3], f1_array[3])
    print "  At   30 docs:   %0.4f      %0.4f   %0.4f" %(p_doc_array[4], recall_array[4], f1_array[4])
    print "  At  100 docs:   %0.4f      %0.4f   %0.4f" %(p_doc_array[5], recall_array[5], f1_array[5])
    #print "  At  200 docs:   %0.4f      %0.4f   %0.4f" %(p_doc_array[6], recall_array[6], f1_array[6])
    #print "  At  500 docs:   %0.4f      %0.4f   %0.4f" %(p_doc_array[7], recall_array[7], f1_array[7])
    #print "  At 1000 docs:   %0.4f      %0.4f   %0.4f" %(p_doc_array[8], recall_array[8], f1_array[8])
    print "R-Precision (precision after R (= num_rel for a query) docs retrieved):"
    print "    Exact:        %0.4f" %(rp)
    print "nDCG value for all the relevant documents at k = 5, 10, 15, 20, 30, 100, 200:"
    print "                  %0.4f" %dcg

def calculate_dcg(dcg_vector):
    sorted_dcg = sorted(dcg_vector,reverse=True)
    numerator = dcg_vector[0]
    for i in range(1,dcg_vector.__len__()):
        temp = float(dcg_vector[i]/math.log(i+1,2))
        numerator += temp
    denominator = sorted_dcg[0]
    for i in range(1,sorted_dcg.__len__()):
        temp = float(sorted_dcg[i]/math.log(i+1,2))
        denominator += temp
    if denominator != 0:
        ndcg = float(numerator/denominator)
    else:
        ndcg = 0
    return ndcg

if __name__ == '__main__':

    print_all_queries,qrel_file, trec_file  = check_args(sys.argv[1:])

    print "print_all_queries : " + str(print_all_queries)
    print "qrel_file : " + qrel_file
    print "trec_file : " + trec_file

    # Process the qrel
    with open(qrel_file, 'r') as qf:
        qrel_lines = qf.readlines()

    # take the values from each line and put them in a data structure
    # qrel is a dict, whose keys are topicIDs and whose values are
    # another hash. This hash has keys as docIDs and values that are relevance values

    dummy = 0
    qrel = {}
    doc_rel = {}
    num_rel = {}

    for line in qrel_lines:
        (topic, dummy, doc_id, rel) = line.split()
        temp_map = {}
        if qrel.has_key(topic):
            temp_map = qrel[topic]
        temp_map[doc_id] = int(rel)
        qrel[topic] = temp_map
        if num_rel.has_key(topic):
            if int(rel) != 0:
                rel = 1
            num_rel[topic] += int(rel)
        else:
            if int(rel) != 0:
                rel = 1
            num_rel[topic] = int(rel)

    # printing the contents of num_rel
    #for key in num_rel:
    #    print "key: " + str(key) + " value: " + str(num_rel[key])

    #the following code snippet prints the above structure
    # doc_map = {}
    # for topic_id in qrel.keys():
    #     doc_map = qrel[topic_id]
    #     for doc_id in doc_map.keys():
    #         print topic_id + " " + doc_id + " " + str(doc_map[doc_id])

    # Process trec
    with open(trec_file, 'r') as tf:
        trec_lines = tf.readlines()


    trec = {}
    for line in trec_lines:
        (topic, dummy, doc_id, dummy, score, dummy) = line.split()
        doc_score_map = {}
        if trec.has_key(topic):
            doc_score_map = trec[topic]
        doc_score_map[doc_id] = score
        trec[topic] = doc_score_map

    # the following code snippet prints the above structure
    # doc_map = {}
    # for topic_id in trec.keys():
    #     doc_map = trec[topic_id]
    #     for doc_id in doc_map.keys():
    #         print topic_id + " " + doc_id + " " + str(doc_map[doc_id])


    # Initializing arrays
    recalls = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    #cutoffs = [5, 10, 15, 20, 30, 100, 200, 500, 1000]
    cutoffs = [5, 10, 15, 20, 30, 100]

    # Processing data form trec to get results
    # process topics in order
    num_topics = 0
    temp_doc_rel_map = {}
    sum_avg_prec = 0
    sum_r_prec = 0
    sum_avg_f1 = 0
    sum_dcg = 0

    tot_num_ret = 0
    tot_num_rel = 0
    tot_num_rel_ret = 0
    sum_prec_at_cutoffs = {}
    sum_recall_at_cutoffs = {}
    sum_prec_at_recalls = {}
    sum_f1_at_cutoffs = {}
    key_list = trec.keys()
    key_list.sort()

    for topic in key_list:
        if not num_rel.has_key(topic):
            continue
        num_topics += 1
        temp_doc_rel_map = trec[topic]

        prec_list = {}
        rec_list = {}
        f1_list = {}

        dcg_vector = []

        num_ret = 0
        num_rel_ret = 0
        sum_prec = 0
        sum_f1 = 0

        temp_map = {}
        temp_map = qrel[topic]

        temp_doc_rel_map_sorted = sorted(temp_doc_rel_map.items(), key=lambda x:x[1], reverse=True)
        for item in temp_doc_rel_map_sorted:
            doc_id = item[0]
            num_ret += 1
            if temp_map.has_key(doc_id):
                rel = float(temp_map[doc_id])
                if int(rel) != 0:
                    bin_rel = 1
                    sum_prec += float(bin_rel) * float(1 + num_rel_ret) / float(num_ret)
                    num_rel_ret += float(bin_rel)

            dcg_vector.append(rel)
            prec_list[num_ret] = float(num_rel_ret/num_ret)
            rec_list[num_ret] = float(num_rel_ret)/float(num_rel[topic])
            denom = float(prec_list[num_ret] + rec_list[num_ret])
            if denom != 0.0:
                f1_list[num_ret] = float(2 * prec_list[num_ret] * rec_list[num_ret])/ denom
            else:
                f1_list[num_ret] = 0
            sum_f1 += f1_list[num_ret]

        #print str(num_rel_ret) + " " + str(num_rel[topic])

        #print str(topic) + " " + str(prec_list[200]) + " " + str(recalls[200])

        dcg_value = calculate_dcg(dcg_vector)
        sum_dcg += dcg_value
        avg_prec = float(sum_prec/num_rel[topic])
        avg_f1 = float(sum_f1/num_rel[topic])

        # Fill out the remainder of the precision/recall lists, if necessary.
        final_recall = int(num_rel_ret/num_rel[topic])

        # for i in range(num_ret+1, 1000, 1):
        #     prec_list[i] = num_rel_ret/i
        #     rec_list[i] = final_recall
        #     f1_list[i] = float(2 * prec_list[i] * rec_list[i]) / float(prec_list[i] + rec_list[i])

        # Now calculate precision at document cutoff levels and R-precision.
        # Note that arrays are indexed starting at 0...
        prec_at_cutoffs = []
        recall_at_cutoffs = []
        f1_at_cutoffs = []
        for cutoff in cutoffs:
            prec_at_cutoffs.append(prec_list[cutoff])
            recall_at_cutoffs.append(rec_list[cutoff])
            f1_at_cutoffs.append(f1_list[cutoff])

        # Now calculate R-precision
        r_prec = prec_list[num_rel[topic]]


        # Now calculate interpolated precisions...
        max_prec = 0
        size_prec_list = prec_list.__len__()
        for i in range(size_prec_list, 0, -1):
            if prec_list[i] > max_prec:
                max_prec = prec_list[i]
            else:
                prec_list[i] = max_prec

        # Finally, calculate precision at recall levels.
        prec_at_recalls = []
        i = 1
        for recall in recalls:
            while i <= rec_list.__len__() and rec_list[i] < recall:
                i += 1
            if i <= prec_list.__len__():
                prec_at_recalls.append(prec_list[i])
            else:
                prec_at_recalls.append(0)

        # Print stats on a per query basis if requested.
        if print_all_queries == 1:
            eval_print(topic,num_ret,num_rel[topic], num_rel_ret, prec_at_recalls,
                       avg_prec,prec_at_cutoffs,r_prec,f1_at_cutoffs, recall_at_cutoffs, dcg_value)

        # Now update running sums for overall stats.
        tot_num_ret += num_ret
        tot_num_rel += int(num_rel[topic])
        tot_num_rel_ret += num_rel_ret

        for i in range(0, cutoffs.__len__(), 1):
            if sum_prec_at_cutoffs.has_key(i):
                sum_prec_at_cutoffs[i] += prec_at_cutoffs[i]
            else:
                sum_prec_at_cutoffs[i] = prec_at_cutoffs[i]

            if sum_recall_at_cutoffs.has_key(i):
                sum_recall_at_cutoffs[i] += recall_at_cutoffs[i]
            else:
                sum_recall_at_cutoffs[i] = recall_at_cutoffs[i]

            if sum_f1_at_cutoffs.has_key(i):
                sum_f1_at_cutoffs[i] += f1_at_cutoffs[i]
            else:
                sum_f1_at_cutoffs[i] = f1_at_cutoffs[i]

        for i in range(0, recalls.__len__(), 1):
            if sum_prec_at_recalls.has_key(i):
                sum_prec_at_recalls[i] += prec_at_recalls[i]
            else:
                sum_prec_at_recalls[i] = prec_at_recalls[i]

        sum_avg_prec += avg_prec
        sum_r_prec += r_prec
        sum_avg_f1 += avg_f1

    avg_prec_at_cutoffs = []
    avg_prec_at_recalls = []
    avg_f1_at_cutoffs = []
    avg_recall_at_cutoffs = []
    # Now calculate summary stats.
    for i in range(0,cutoffs.__len__(),1):
        avg_prec_at_cutoffs.append(sum_prec_at_cutoffs[i]/num_topics)
        avg_recall_at_cutoffs.append(sum_recall_at_cutoffs[i]/num_topics)
        avg_f1_at_cutoffs.append(sum_f1_at_cutoffs[i]/num_topics)
    for i in range(0,recalls.__len__(),1):
        avg_prec_at_recalls.append(sum_prec_at_recalls[i]/num_topics)

    mean_avg_prec = float(sum_avg_prec)/float(num_topics)
    avg_r_prec = sum_r_prec/num_topics
    avg_f1_all = float(sum_avg_f1/num_topics)
    avg_dcg = float(sum_dcg/num_topics)

    eval_print(num_topics,tot_num_ret, tot_num_rel, tot_num_rel_ret,
               avg_prec_at_recalls, mean_avg_prec, avg_prec_at_cutoffs,
               avg_r_prec, avg_f1_at_cutoffs,avg_recall_at_cutoffs,avg_dcg)
