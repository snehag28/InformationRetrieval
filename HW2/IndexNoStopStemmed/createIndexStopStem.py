__author__ = 'snehagaikwad'

from datetime import datetime
import os
import re

# the file takes the token file and parses it to get the term, document and positions
# it then calculates the TF(term frequency in a document), TTF(total term frequency in all the documents)
# the index file stores Term, TF, TTF, documents that have the term, positions of the terms in the documents
# an offset file is maintained for each term -> (term, offset) for easy access to the main file
# the offset is retrieved by using f.tell() and the term can be found using the f.seek(offset) method
# we concatenate the offset file by adding the current offset to the offset of EOF of the previous file

# Algorithm:
# get the content of uniqueTerms in a hashmap -> (Term, term_id) pair
# sort the map to get the terms in ascending order
# process 1000 terms at a time
# for each term, read a new line from the token file
# split the line to get the parameters for the term.
# store the term and it's parameters in the inverted index file, and store it's corresponding offset in the catalogue
# At the end of the iteration we get one index file and one catalogue file for 1000 terms
# we then merge the index files and the catalogue files to get the final index and catalogue

# the index file looks like
# term-
def makeTermIndex(line,inf,cf):
    fields = line.split("=")  #token=doc_id:pos1|pos2,doc_id:pos1|pos2 ...
    term = fields[0]
    d_blocks = fields[1].split(",") #docid:pos1|pos2
    totalTF = 0
    d_blocks_str = ""
    for d_block in d_blocks:
        d_block_arr = d_block.split(":")
        doc_id = d_block_arr[0]
        positions = d_block_arr[1].split("|")
        tf = positions.__len__()
        totalTF += tf
        d_block_str = doc_id + ":" + str(tf) + "|" + d_block_arr[1]
        d_blocks_str += d_block_str + ","
    d_blocks_str = d_blocks_str.rstrip(",")
    #print term +" " + str(totalTF)
    index_str = term + "-" + str(totalTF) +"-"+ d_blocks_str
    term_offset = inf.tell()
    inf.write(index_str)
    cf.write(term + ":" + str(term_offset)+"\n")

def mergeIndexFiles(file_1, file_2, mergeCount,i):
    mergeFile = indexPath+ "indexMerge_" + str(mergeCount) + "_" + str(i) + ".txt"
    mf = open(file_1,'a')
    mf.write("\n")
    with open(file_2) as lines:
        for line in iter(lines):
            mf.write(line)
    offset = mf.tell()
    mf.close()
    os.rename(file_1,mergeFile)
    #print os.listdir(indexPath)
    #os.remove(file_1)
    os.remove(file_2)
    return (offset, mergeFile)

def mergeCatalogueFiles(file_1,file_2,mergeCount,i,eof_offset):
    mergeFile = cataloguePath+ "catalogueMerge_" + str(mergeCount) + "_" + str(i) +".txt"
    f1 = open(file_1,'a')
    with open(file_2) as lines:
        for line in lines:
            if not line == "":
                fields = line.split(":")
                term = fields[0]
                offset = int(fields[1])
                new_offset = offset + eof_offset
                f1.write(term + ":" + str(new_offset) + "\n")
    f1.close()
    os.rename(file_1,mergeFile)
    os.remove(file_2)
    return mergeFile

if __name__ == '__main__':
    startTime = datetime.now()
    uniqueTermFile = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/uniqueTerms.txt"
    tokenFile = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/indexStemStop/tuples/token.txt"
    indexParamsFile = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/indexStemStop/indexParams.txt"
    indexPath = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/indexStemStop/index/"
    cataloguePath = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/indexStemStop/catalogue/"

    # get the terms in a map
    termIDMap = {}
    with open(uniqueTermFile) as lines:
        for line in iter(lines):
            fields = line.split("\t")
            termIDMap[fields[0]] = fields[1]
    # sort the terms
    termIDMap_sorted = sorted(termIDMap.items(), key=lambda x:x[1])

    numberOfTerms = 0
    ipf = open(indexParamsFile,'r')
    param = ipf.readline()
    params = param.split("\t")
    numberOfTerms = int(params[1])

    #print "number of unique terms is: " + str(numberOfTerms)

    index_catalogue_map = {}
    term_offset_map = {}
    with open(tokenFile) as lines:
        indexFileCount = 0
        for i in range(1,numberOfTerms,50000):
            indexFileCount += 1
            indexFile = indexPath+"invertedIndex_"+str(indexFileCount)+".txt"
            catalogueFile = cataloguePath+"catalogue_"+str(indexFileCount)+".txt"
            index_catalogue_map[indexFile] = catalogueFile
            print "processing batch "+ str(indexFileCount) +" of terms..."
            inf = open(indexFile,'w')
            cf = open(catalogueFile,'w')
            iterLines = []
            for j in range (i,i+50000):
                iterLines.append(lines.readline())
            for line in iterLines:
                if not line == "":
                    makeTermIndex(line,inf,cf)
            inf.close()
            cf.close()

    ds_file_index = indexPath+".DS_Store"
    ds_file_offset = cataloguePath+".DS_Store"
    if os.path.exists(ds_file_index):
        os.remove(ds_file_index)
    if os.path.exists(ds_file_offset):
        os.remove(ds_file_offset)
    indexFiles = os.listdir(indexPath)
    mergeCount = 0

    while indexFiles.__len__() > 1:
        mergeCount += 1
        #print indexFiles

        for i in range(0,indexFiles.__len__(),2):
            if (i+1) < indexFiles.__len__():
                indexFile_1 = indexPath + indexFiles[i]
                indexFile_2 = indexPath + indexFiles[i+1]
                catalogueFile_1 = index_catalogue_map[indexFile_1]
                catalogueFile_2 = index_catalogue_map[indexFile_2]
                eof_offset, mergedIndexFile = mergeIndexFiles(indexFile_1, indexFile_2, mergeCount,i)
                mergedOffsetFile = mergeCatalogueFiles(catalogueFile_1,catalogueFile_2,mergeCount,i, eof_offset)
                index_catalogue_map[mergedIndexFile] = mergedOffsetFile
        print "getting new files..."
        if os.path.exists(ds_file_index):
            os.remove(ds_file_index)
        indexFiles = os.listdir(indexPath)
        print "number of file: " + str(indexFiles.__len__())

    indexFile = indexPath + indexFiles[0]
    indexFileNew = indexPath + "invertedIndex.txt"

    if os.path.exists(ds_file_offset):
        os.remove(ds_file_offset)
    catalogueFiles = os.listdir(cataloguePath)
    catalogueFile = cataloguePath + catalogueFiles[0]
    catalogueFileNew = cataloguePath + "catalogue.txt"

    os.rename(indexFile,indexFileNew)
    os.rename(catalogueFile,catalogueFileNew)

    total_time = datetime.now() - startTime
    print total_time