# some test exercise

```
(1) copy data json file and python2.7 with elasticsearch to the ES host
(2) if you want you can delete the ES indext first : 
    curl -XDELETE http://192.168.1.207:9200/knowledgebase
(3) execute the create_index.sh script
    bash create_index.sh
(4) execute the new_post2es.py in the data.json directory using python2.7
    /opt/cloudwiz-agent/altenv/bin/python2.7 new_post2es.py
```