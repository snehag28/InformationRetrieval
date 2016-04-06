__author__ = 'snehagaikwad'

def create_index(client):
    #client.indices.delete(index=["ir-hw3"],ignore=404)
    client.indices.create(index="sneha_1", body={
            "settings": {
                "index": {
                    "store": {
                        "type": "default"
                    },
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "analysis": {
                    "analyzer": {
                        "my_english": {
                            "type": "english",
                            "stopwords_path": "stoplist.txt"
                        }
                    }

                }
            }
        })
    client.indices.put_mapping(index="sneha_1",doc_type="documents",body={
  "documents": {
    "properties": {
      "clean_text": {
        "type": "string",
        "store": True,
        "index": "analyzed",
        "term_vector": "with_positions_offsets_payloads",
        "analyzer": "my_english"
      },
      "raw_html":{
        "type": "string",
        "store": True,
        "index": "no"
      },
      "outlinks":{
          "type":"string",
          "store":True,
          "index":"not_analyzed"
      },
      "inlinks":{
          "type":"string",
          "store":True,
          "index":"not_analyzed"
      }
    }
  }
})
    print "Index Setup"
