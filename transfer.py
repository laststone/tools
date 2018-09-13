#! /usr/bin/env python
# coding:utf-8


import pymongo

client=pymongo.MongoClient("123.207.148.247",27017)
db=client.zookeeper_db
answers=db.answers
cur=answers.find({},{"_id": 0},no_cursor_timeout = True)
#cur=answers.getCollection('answers').find({})

out_client=pymongo.MongoClient("188.131.131.58",27017)
out_db=out_client.output_db
out_cur=out_db.answers

for i in range(100000):
    info = cur.next()
    symptom = info['symptom']
    symptom = eval(symptom)
    symptom = symptom.decode('utf-8')

    solution = info['solution']
    solution = eval(solution)
    solution = solution.decode('utf-8')

    item={}

    item['title']=info['title']
    item['language']=info['language']
    item['source_url']=info['source_url']
    item['solution']= solution
    item['solution_plain_text']=info['solution_plain_text']
    item['symptom']= symptom
    item['symptom_plain_text']=info['symptom_plain_text']
    item['tags']=info['tags']
    item['service']=info['service']
    item['created_at']=info['created_at']

    #insert_db=client.output_db
    #new_cur=insert_db.answers
    #new_cur.save(dict(item))
    #new_cur.insert_one(dict(item))
    out_cur.insert_one(dict(item))
    print (i)


