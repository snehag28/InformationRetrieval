__author__ = 'snehagaikwad'
from datetime import datetime
import os
import re

# has no stop words and the words are not stemmed
# The file creates tokens for each term
# it goes through the documents and creates tuples
# it stores the tuples for 50 data files at a time in a map,
#           at the same time it also populates the uniqueTerms.txt and docNames.txt
# the map created for 50 data files is then written in a file,
#           this file is then merged with the main file
# the tuples are of the form (term_id-{doc_id_1:[pos1,pos2,...]},{doc_id_2:[pos1,pos2,...]}

def createUniqueTerms(text,doc_id,countUniqueTerm,term_block_map, totalCF,stopList):
    position = 0
    term_pos_map = {}        # is a word,list(positions) map
    matchObj = re.finditer(r'\w+(\.?\w+)*', text, re.M|re.I)
    if matchObj:
        for term in matchObj:
            position += 1
            totalCF += 1
            word = term.group().lower()
            if word not in stopList:
                #word = ps.stem(query_word, 0, len(query_word) - 1)
                tempList = term_pos_map.setdefault(word,[])
                tempList.append(position)
                term_pos_map[word] = tempList
                if word not in uniqueTerms:
                    countUniqueTerm+=1
                    uniqueTerms[word] = countUniqueTerm
        term_block_map = makeTermDBlockMap(term_pos_map,doc_id,term_block_map)    # takes the term_pos_map and doc_id as parameter and writes into the file
    return (countUniqueTerm, term_block_map, totalCF)

def makeTermDBlockMap(term_pos_map,doc_id,term_block_map):

    for term in term_pos_map:
        doc_pos_map = {}
        posList = term_pos_map.setdefault(term,[])
        doc_pos_map[doc_id] = posList
        d_block_list = term_block_map.setdefault(term,[])
        d_block_list.append(doc_pos_map)
        term_block_map[term] = d_block_list
    return term_block_map


def writeTuples(tfn,term_block_map):
    term_block_map_sorted = sorted(term_block_map.items(), key=lambda x:x[0])
    for term_pos_pair in term_block_map_sorted:
        d_block_list = term_pos_pair[1]
        d_block_list_str = ""
        for d_block in d_block_list:
            for doc_id in d_block:
                posList = d_block.setdefault(doc_id,[])
                posStr = ""
                for pos in posList:
                    posStr += str(pos) + "|"
                posStr = posStr.rstrip('|')
                #posStr += "]"
                d_block_list_str += str(documents[doc_id]) + ":" + posStr + ","
        d_block_list_str = d_block_list_str.rstrip(',')
        #d_block_list_str += ")"
        #tfn.write(str(uniqueTerms[term_pos_pair[0]]) + "=" + d_block_list_str + "\n")
        tfn.write(term_pos_pair[0] + "=" + d_block_list_str + "\n")

def mergeFiles(file_1, file_2, mergeCount,i):
    mergeFile = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/IndexNoStopNoStemmed/tuples/merge_" + str(mergeCount) + "_" + str(i)
    mfw = open(mergeFile,'w')
    f1 = open(file_1,'r')
    f2 = open(file_2,'r')
    line_1 = f1.readline()
    line_2 = f2.readline()
    while line_1 or line_2:
        if not line_2:
            while line_1:
                mfw.write(line_1)
                line_1 = f1.readline()
        elif not line_1:
            while line_2:
                mfw.write(line_2)
                line_2 = f2.readline()
        else:
            term_1 = line_1.split('=')[0]
            term_2 = line_2.split('=')[0]
            if term_1 == term_2:
                merge_term(line_1,line_2,mfw)
                flag_1 = 1
                flag_2 = 1
            elif term_1 < term_2:
                flag_1 = 1
                flag_2 = 0
            else:
                mfw.write(line_2)
                flag_1 = 0
                flag_2 = 1
            if flag_1 == 1 and flag_2 == 1:
                line_1 = f1.readline()
                line_2 = f2.readline()
            elif flag_1 == 1:
                line_1 = f1.readline()
            elif flag_2 == 1:
                line_2 = f2.readline()
    f1.close()
    f2.close()
    os.remove(file_1)
    os.remove(file_2)

def merge_term(line_1, line_2, mfw):
    f1_tuple = line_1.split('=')
    f2_tuple = line_2.split('=')
    term = f1_tuple[0]
    d_block_1 = f1_tuple[1]
    d_block_2 = f2_tuple[1]
    d_block_1 = d_block_1.rstrip('\n')
    d_block = d_block_1 + "," + d_block_2
    mfw.write(term + "=" + d_block)

# step 1: go through all the documents in batches of 1000 and find the tuples and unique terms
if __name__ == "__main__":
    startTime = datetime.now()

    stopwords_path = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/stoplist.txt"
    with open(stopwords_path, 'r') as sf:
        stopList = sf.read().replace('\n', ' ')
    #print stopList

    # the stemmer object
    #ps = PorterStemmer();

    path = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/ap89_collection/"
    tupleFilePath = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/IndexNoStopNoStemmed/tuples/"

    fileCount = 0
    uniqueTerms = {}
    documents = {}
    doc_close_tag = "</DOC>"
    doc_id_tag = "<DOCNO>"
    text_open_tag = "<TEXT>"
    text_close_tag = "</TEXT>"
    text=""
    doc_count = 0
    uniqueTermCount=0
    totalCF = 0
    docNames = os.listdir(path)
    tupleFileCount = 0
    for i in range (0,364,50):
    #for i in range (0,14,2):
        tuples = {}
        tupleFileCount += 1
        tupleFileName = tupleFilePath + "tuple_"+str(tupleFileCount)
        tfn = open(tupleFileName,'a')
        if i+50 < 364:
        #if i+2 < 14:
            fileNames = docNames[i:i+50]
            #fileNames = docNames[i:i+2]
        else:
            fileNames = docNames[i:364]
            #fileNames = docNames[i:14]
        print fileNames
        term_block_map = {}
        for filename in fileNames:
            fileCount += 1
            print "Opening the file to read..%r, file number..%d" % (filename, fileCount)
            full_filename = path + "/" + filename
            text_flag = 0
            with open(full_filename) as lines:
                for line in iter(lines):
                    if doc_close_tag in line:
                        uniqueTermCount, term_block_map, totalCF = createUniqueTerms(text,doc_id,uniqueTermCount,term_block_map,totalCF,stopList)
                        text = ""
                    if doc_id_tag in line:
                        doc_id = line.split(" ")[1]
                        doc_count += 1
                        documents[doc_id] = doc_count
                    if text_close_tag in line:
                        text_flag = 0
                    if text_flag == 1:
                        text += line
                    if text_open_tag in line:
                        text_flag = 1
        writeTuples(tfn, term_block_map)
        tfn.close()

    ds_file = tupleFilePath+".DS_Store"
    if os.path.exists(ds_file):
        os.remove(ds_file)
    tupleFiles = os.listdir(tupleFilePath)
    mergeCount = 0

    while tupleFiles.__len__() > 1:
        mergeCount += 1
        for i in range(0,tupleFiles.__len__(),2):
            if (i+1) < tupleFiles.__len__():
                file_1 = tupleFilePath + tupleFiles[i]
                file_2 = tupleFilePath + tupleFiles[i+1]
                mergeFiles(file_1, file_2, mergeCount,i)
        if os.path.exists(ds_file):
            os.remove(ds_file)
        tupleFiles = os.listdir(tupleFilePath)
    tupleFile = tupleFilePath + tupleFiles[0]
    tupleFileNew = tupleFilePath + "token.txt"
    os.rename(tupleFile,tupleFileNew)

    # file to store the vocabulary size and Total CF
    indexParamsFile = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/IndexNoStopNoStemmed/indexParams.txt"
    ipf = open(indexParamsFile, 'w')
    ipf.write("VocabularySize \t" + str(uniqueTermCount) + "\n")
    ipf.write("TotalCF \t" + str(totalCF))
    ipf.close()

    # file that stores (term, term_id) pair
    uniqueTermFile = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/IndexNoStopNoStemmed/uniqueTerms.txt"
    # file that stores the (document, doc_id) pair
    docNameFile = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/IndexNoStopNoStemmed/docNames.txt"

    tf = open(uniqueTermFile, 'w')
    df = open(docNameFile,'w')

    uniqueTerms_sorted = sorted(uniqueTerms.items(), key=lambda x:x[0])
    documents_sorted = sorted(documents.items(), key=lambda x:x[1])

    for word in uniqueTerms_sorted:
        tf.write(str(word[0]) +"\t"+ str(word[1]) +"\n")
    for doc in documents_sorted:
        df.write(str(doc[1]) +"\t"+  str(doc[0])+"\n")

    df.close()
    tf.close()

    total_time = datetime.now() - startTime
    print total_time


