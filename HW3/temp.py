__author__ = 'snehagaikwad'

import urllib2
import bs4
import nltk
import elasticsearch
import json
from Canonicalization import base_url, Canonicalize

body = {}

html = urllib2.urlopen("http://en.wikipedia.org/wiki/List_of_career_achievements_by_Kobe_Bryant").read()
base = base_url("http://en.wikipedia.org/wiki/List_of_career_achievements_by_Kobe_Bryant")
curl = "http://en.wikipedia.org/wiki/List_of_career_achievements_by_Kobe_Bryant"

soup = bs4.BeautifulSoup(html)
for script in soup(["script", "style"]):
    script.extract()
body["clean_text"] = soup.get_text()
body["raw_html"] = unicode(html,errors="ignore")
body["outlinks"] = set()
body["inlinks"] = set()
for links in soup.find_all("a",href=True):
    link = Canonicalize(links["href"],base)
    body["outlinks"].add(link)

client = elasticsearch.Elasticsearch()

res = client.search(index="basketball",doc_type="page",body={
    "query": {
    "match": {
    "outlinks":"http://en.wikipedia.org/wiki/List_of_career_achievements_by_Kobe_Bryant"
    }
    },
    "_source": {
    "exclude": ["clean_text","raw_html","outlinks","inlinks"]
    }
})

for hit in res["hits"]["hits"]:
    body["inlinks"].add(hit["_id"])


body["outlinks"] = list(body["outlinks"])
body["inlinks"] = list(body["inlinks"])
client.index(index="basketball",doc_type="page",id=curl,body=json.dumps(body))