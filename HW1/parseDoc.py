__author__ = 'snehagaikwad'

from elasticsearch import Elasticsearch
from datetime import datetime
import os



def create_index(client, index):
    # create empty index
    client.indices.create(
        index=index,
        body={
            "settings": {
                # just one shard, no replicas for testing
                "number_of_shards": 1,
                "number_of_replicas": 0,
                # custom analyzer for analyzing file paths
                "analysis": {
                    "analyzer": {
                        "my_analyzer": {
                            "type": "english",
                            "stopwords_path": "stoplist.txt"
                        }
                    }
                }
            }
        },
        ignore=400
    )
    client.indices.put_mapping(
        index=index,
        doc_type="test",
        body={
            "test": {
                "properties": {
                    "text": {
                        "type": "string",
                        "store": True,
                        "index": "analyzed",
                        "term_vector": "with_positions_offsets_payloads",
                        "analyzer": "my_analyzer"
                    }
                }
            }
        }
    )

if __name__== "__main__":
    # instantiate es client, connects to localhost:9200 by default
    es = Elasticsearch()

    start_time = datetime.now()

    create_index(es,'ir_hw1')
    path = "/Users/snehagaikwad/Documents/IR_data/AP_DATA/ap89_collection/"

    doc_close_tag = "</DOC>"
    doc_id_tag = "<DOCNO>"
    text_open_tag = "<TEXT>"
    text_close_tag = "</TEXT>"
    text=""
    count = 0
    for filename in os.listdir(path):
        print "Opening the file to read..%r" % filename
        full_filename = path + "/" + filename
        text_flag = 0

        with open(full_filename) as lines:
            for line in iter(lines):
                if doc_close_tag in line:
                    #count+=1
                    #print count
                    op = es.index(index="ir_hw1", doc_type="test", id=doc_id, body={"doc_no":doc_id,"text": text})
                    text=""
                if doc_id_tag in line:
                    doc_id = line.split(" ")[1]
                if text_close_tag in line:
                    #print "</text>"
                    text_flag = 0
                if text_open_tag in line:
                    #print "<text>"
                    text_flag = 1
                if text_flag == 1:
                    text += line

    total_time = datetime.now() - start_time
    print total_time