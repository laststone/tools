# -*- coding: utf-8 -*-

# using python2 and pip install elasticsearch==5.4.0 

import os
import json
import hashlib
from elasticsearch import Elasticsearch
import elasticsearch.helpers


es = Elasticsearch(['192.168.1.206:9200'])


for file in sorted([x for x in os.listdir('.') if x.endswith(".json")]):
    i = 0
    with open(file, 'r') as f:
        try:
            print('read file: ' + file)
            doc_list=[]
            docs = json.load(f)
            for doc in docs:
                if len(doc_list) < 1000:
                    action = {
                        "_index" : 'knowledgebase',
                        "_type" : 'article',
                        "_id" : hashlib.sha256(doc['source_url'].encode('utf-8')).hexdigest(),
                        "_source" : {
                            "service" : doc["service"],
                            "title" : doc["title"],
                            "symptom_plain_text" : doc["symptom_plain_text"],
                            "symptom" : doc["symptom"],
                            "solution_plain_text" : doc["solution_plain_text"],
                            "solution" : doc["solution"],
                            "score" : 100,
                            "source_url" : doc["source_url"],
                            "tags" : doc["tags"],
                            "language" : doc["language"],
                            "type": 0,
                            "org_id":0,
                            "sys_id":0,
                            "vote":0

                        }
                    }
                    doc_list.append(action)
                else:
                    action = {
                        "_index" : 'knowledgebase',
                        "_type" : 'article',
                        "_id" : hashlib.sha256(doc['source_url'].encode('utf-8')).hexdigest(),
                        "_source" : {
                            "service" : doc["service"],
                            "title" : doc["title"],
                            "symptom_plain_text" : doc["symptom_plain_text"],
                            "symptom" : doc["symptom"],
                            "solution_plain_text" : doc["solution_plain_text"],
                            "solution" : doc["solution"],
                            "score" : 100,
                            "source_url" : doc["source_url"],
                            "tags" : doc["tags"],
                            "language" : doc["language"],
                            "type": 0,
                            "org_id":0,
                            "sys_id":0,
                            "vote":0

                        }
                    }
                    doc_list.append(action)
                    elasticsearch.helpers.bulk( es, doc_list )
                    doc_list=[]

            if  len(doc_list) != 0:
                result = elasticsearch.helpers.bulk( es, doc_list )
                doc_list = []
        except Exception as e:
            print(e)



