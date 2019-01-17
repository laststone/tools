#! /usr/bin/env python
#coding:utf-8

import requests
import json
import time
import datetime

start_time = 1000    # <——— 设置删除数据的起止时间（天）
end_time = 1

start_ts = int(time.mktime(time.strptime(str(datetime.date.today()-datetime.timedelta(days=start_time)), '%Y-%m-%d')))
end_ts = int(time.mktime(time.strptime(str(datetime.date.today()-datetime.timedelta(days=end_time)), '%Y-%m-%d'))) + 7200
start_time_point=time.strftime("%Y/%m/%d-%H:%M:%S",time.localtime(start_ts))
end_time_point=time.strftime("%Y/%m/%d-%H:%M:%S",time.localtime(end_ts))
metric_url = "http://172.20.238.127:4244/api/suggest?max=10000000&type=metrics&q=3.4.”                                                                 #  《———————这里： 需要更改opentsdb地址和sys+org


metric_list = json.loads(requests.get(metric_url).text)
for sys_metric in metric_list:
    metric_full_name=sys_metric
    sys_metric_list = metric_full_name.split('.')[2:]
    metric_name = '.'.join(sys_metric_list)
    start_time_point=time.strftime("%Y/%m/%d-%H:%M:%S",time.localtime(start_ts))
    end_time_point=time.strftime("%Y/%m/%d-%H:%M:%S",time.localtime(end_ts))

    delete_url = "http://172.20.238.127:4244/api/query?start=" + start_time_point + '&end=' + end_time_point + '&m=avg:1s-avg:'+ metric_name +'&token='+ “YPs3e4uha5ugw55l354b60thci1jz35108105230”    #《——这里需要添加opentsdb地址+Token
    status_code = requests.delete(delete_url).status_code
    resp_body = str(requests.delete(delete_url).text)
    print status_code
    if status_code==200:
        print 'successfully delete metric : ' + metric_name
    else:
        print 'delet error:' + metric_name
