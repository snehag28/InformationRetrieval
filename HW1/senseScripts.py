__author__ = 'snehagaikwad'

from elasticsearch import Elasticsearch

es = Elasticsearch()

es.put_script(lang="groovy", id="getTF", body={
        "script": "_index[field][term].tf()"
    })
es.put_script(lang="groovy", id="getTTF", body={
        "script": "_index[field][term].ttf()"
    })
