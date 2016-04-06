__author__ = 'snehagaikwad'

from elasticsearch import Elasticsearch
from datetime import datetime
import os

es = Elasticsearch()

#we write the document length into a file to avoid calculating it repeatedly
output_file = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/document_length.txt"
f = open(output_file,'w')

start_time = datetime.now()

doc_count = 84679
result = es.search(index="ir_hw1", doc_type="test",
                   body={
                       "query": {
                        "match_all": {}
                        }
                   },
                   size=doc_count)
documents=[doc['_id'] for doc in result['hits']['hits']]

for i in documents:
    doc_terms= es.termvector(index="ir_hw1", doc_type="test", id=i,
    body = {
    "fields" : ["text"],
    "offsets" : True,
    "payloads" : True,
    "positions" : True,
    "term_statistics" : True,
    "field_statistics" : True
    })

    doc_len = 0
    for doc in doc_terms["term_vectors"]["text"]["terms"]:
        tf=doc_terms["term_vectors"]["text"]["terms"][doc]["term_freq"]
        doc_len=doc_len+tf

    f.write(str(i) + "\t" + str(doc_len) + "\n")

total_time = datetime.now() - start_time
print total_time