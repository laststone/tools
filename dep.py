#!/usr/bin/python

import datetime
import gzip
import json
import os
import re
import sys
import time
import struct
import requests


hosts = set()
services = set()
dependencies = set()
depfile = '/data/es-data/dep.txt'

regex = re.compile('[^a-zA-Z0-9\-_./]')

def main(argv):
    root_dir = sys.argv[1]

    if os.path.isfile(depfile):
        with open(depfile, 'r') as df:
            for d in df:
                dependencies.add(d)

    for subdir, dirs, files in os.walk(root_dir):
        for json_file in files:
            data = read_json(os.path.join(subdir, json_file))
            parse_json(data)

#    with open('/data/es-data/dep.txt', 'a') as df:
#        for d in dependencies:
#            df.write(d + "\n")


def read_json(json_file):
    with gzip.open(json_file, 'rb') as f:
        data = json.load(f)
    return data


def sanitize(name):
    if name is None:
        return ''
    if name:
        name = name.split('?')[0]
    if name:
        if name.startswith('/'):
            tokens = name.split('/')[1:3]
        else:
            tokens = name.split('.')[:2]
        name = '.'.join(tokens)
    name = regex.sub('_', name)
    return name


def parse_source(_source):

    global hosts
    global services
    global dependencies

    timestamp = None
    src_ip = None
    tgt_ip = None
    src_svc = None
    tgt_svc = None
    trace = None
    url = None
    status_code = None
    execute_time = -1
    method = None
    queue_name = None

    if '@timestamp' in _source:
        timestamp = _source['@timestamp']
        timestamp = time.mktime(datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ').timetuple())   # <---将UTC时间"2018-09-12T20:12:390Z"格式转换为时间戳(秒单位)
        timestamp = int(round(timestamp * 1000000))   #<--------使用round()方法正确显示将秒单位的时间戳转换为微秒单位的时间戳
    if 'fields' in _source:
        fields = _source['fields']
        if 'ip' in fields:
            src_ip = fields['ip']
    if 'source' in _source:
        source = _source['source']
        log_file = os.path.basename(source)
        if log_file:
            src_svc = log_file.split('-')[0]
    if 'trace' in _source:
        trace = _source['trace']
    if 'msg' in _source:
        msg = _source['msg']
        if 'targetHost' in msg:
            tgt_ip = msg['targetHost']
        if 'url' in msg:
            url = msg['url']
        if 'statusCode' in msg:
            status_code = msg['statusCode']
        if 'execute-time' in msg:
            execute_time = int(msg['execute-time'])
            execute_time = int(round(execute_time * 1000))
        if 'service' in msg:
            tgt_svc = msg['service']
        if 'method' in msg:
            method = msg['method']
        if 'queueName' in msg:
            queue_name = msg['queueName']

        

    if src_ip:
        hosts.add(src_ip)
    if tgt_ip:
        hosts.add(tgt_ip)
    if src_svc:
        services.add(src_svc)
    if tgt_svc:
        services.add(tgt_svc)

    if src_ip and tgt_ip and src_svc and tgt_svc and len(trace) != 0:
        #dependencies.add("%s %s %s %s" % (src_ip.lower(), src_svc.lower(), tgt_ip.lower(), tgt_svc.lower()))
        parentId = genId(str(trace),str(src_svc).lower())
        id = genId(str(trace),str(tgt_svc).lower())
        print src_ip.lower()
        print src_svc.lower()
        print tgt_ip.lower()
        print tgt_svc.lower()
        print 'timestamp--->' + str(timestamp) 
        print 'src_ip------>' + src_ip
        print 'tag_ip------>' + tgt_ip
        print 'src_svc----->' + src_svc
        print 'tgt_svc----->' + tgt_svc
        print 'traceId----->' + trace
        print 'parentId---->' + parentId
        print 'Id---------->' + id
        print 'url--------->' + str(url)
        print 'status_code->' + str(status_code)
        print 'execute_time->' + str(execute_time)
        print 'method------->' + str(method)
        print '-------------------------------'

        # generate the post json
        outer_list = []
        inner_dic = {}
        annotations_list = []
        sr_dic={}
        ss_dic={}
        inner_dic["traceId"] = str(trace)
        inner_dic["id"] = str(id)
        inner_dic["parentId"] = str(parentId)
        inner_dic["name"] = str(url)
        inner_dic["timestamp"] = str(timestamp)
        inner_dic["duration"] = str(execute_time)
        sr_dic["timestamp"] = str(timestamp)
        sr_dic["value"] = "sr"
        sr_dic["endpoint"] = {"serviceName": str(tgt_svc), "ipv4": str(tgt_ip)}
        ss_dic["timestamp"] = str(timestamp - execute_time)
        ss_dic["value"] = "ss"
        ss_dic["endpoint"] = {"serviceName": str(tgt_svc), "ipv4": str(tgt_ip)}
        annotations_list.append(sr_dic)
        annotations_list.append(ss_dic)
        inner_dic["annotations"] = annotations_list
        inner_dic["binaryAnnotations"] = []
        outer_list.append(inner_dic)

        # send to zipkin
        headers = {'content-type': 'application/json'}
        formdata = json.dumps(outer_list)
        print formdata
        post_url = "http://140.143.184.96:9411/api/v1/spans"
        post_resp = requests.post(post_url,data=formdata,headers=headers)
        if post_resp.status_code == 200:
            print ">>>>> sucdessfully post to zipkin :" + formdata
        else:
            print "<<<<< failed post to zipkin:" + formdata
        time.sleep(1)


    else:
        print '---------------------------------------------------------------------------->date no satisfy!!'
        



    #if not method:
    #    method = queue_name

    #method = sanitize(method)

    #if trace is None or trace == "":
    #    trace = "unknown"

    #ts = time.mktime(datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ').timetuple())
    #print("%s.%s %s %d host=%s url=%s" % (tgt_ip, tgt_svc, int(ts), execute_time, src_ip, method))
    #print("ts=%s src_ip=%s tgt_ip=%s src_svc=%s tgt_svc=%s trace=%s status_code=%s exec_time=%d method=%s url=%s" % (timestamp, src_ip, tgt_ip, src_svc, tgt_svc, trace, status_code, execute_time, method, url))



def genId(trace_str,string):
    trace_num = struct.unpack('q', struct.pack('Q', int(trace_str, 16)))[0]
    sum = 0
    for i in str(string):
       sum = sum + int(ord(i))
    new_num = trace_num + sum
    hex_string = hex(struct.unpack('Q', struct.pack('q', new_num))[0])[2:]
    if hex_string.endswith('L'):
       return hex_string[:-1]
    return hex_string


def parse_json(data):

    if 'hits' in data:
        hits = data['hits']
        if 'total' in hits:
            total = hits['total']
            #print(total)
        if 'hits' in hits:
            hits = hits['hits']
            for hit in hits:
                if '_source' in hit:
                    _source = hit['_source']
                    parse_source(_source)

    #for host in hosts:
        #print(host)
    #for svc in services:
        #print(svc)
    #for dep in dependencies:
        #print(dep)


if __name__ == "__main__":
    main(sys.argv)


# 启动： /opt/cloudwiz-agent/alt/env/bin/python dep.py /data/es-data/   <---指明zip数据包所在的目录，注意该py文件不能放置在zip数据包目录中，否则启动失败