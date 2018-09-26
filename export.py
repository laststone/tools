# -*- coding: utf-8 -*-
from datetime import datetime
import json
import pymongo


MAX_ITEM_PER_FILE = 1000

MONGO_URI = 'localhost:27017'
MONGO_DATABASE = 'spider_db'

start_date = datetime(2018, 5, 13, 12, 0, 0, 0)
end_date = datetime(2019, 6, 13, 15, 0, 0, 0)


def get_all_items():
    with pymongo.MongoClient(MONGO_URI) as client:
        db = client[MONGO_DATABASE]
        for d in db['answers'].find({'created_at': {'$gte': start_date, '$lt': end_date}}, {'_id': 0, 'created_at': 0}):
            yield d


def write_to_file(index, data):
    with open('spider_db.answers.{}.json'.format(index), 'w', encoding='utf-8') as f:
        json.dump(data, f)


data = []
index = 0

for i in get_all_items():
    if len(data) < MAX_ITEM_PER_FILE:
        data.append(i)
    else:
        write_to_file(index, data)
        data = []
        index += 1

if len(data) > 0:
    write_to_file(index, data)


