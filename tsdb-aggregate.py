#!/usr/bin/env python
# coding:utf-8

import configparser
import requests
import json
import time,datetime
import pymysql
import os

class Tsdb_Aggregare():
    def __init__(self,config):
        self.reget_tags = config.get('base','reget_tags')
        self.metric_startwith = config.get('base','metric_startwith')
        self.start_time = int(config.get('base','start_time'))
        self.end_time = int(config.get('base','end_time'))
        self.interval = config.get('base','interval')
        self.downsample = config.get('base','downsample')
        self.opentsdb_url = config.get('base','opentsdb_url')
        self.mysql_host = str(config.get('base','mysql_host'))
        self.mysql_port = int(config.get('base','mysql_port'))
        self.mysql_user = str(config.get('base','mysql_user'))
        self.mysql_pass = str(config.get('base','mysql_pass'))
        self.conn_db = str(config.get('base','conn_db'))
        self.cloud = config.get('base','cloud')
        self.start_ts = config.get('base','start_ts')
        self.end_ts = config.get('base','end_ts')
        self.tokent = config.get('base','token')

    def __call__(self):
        if self.reget_tags == 'True':
            metric_list=self.get_metris_list(self.metric_startwith)
            self.save_tag(metric_list)

        metric_tag_dic = self.conf_parse2dic()
        sys_org_list = self.query_sys_list()
        os.system('mv ../aggregate_data.json ../aggregate_data.json_old')
        for sys in sys_org_list:
            metric_startwith = str(sys[0])+'.'+str(sys[1])+'.'
            sys_token = sys[2]
            sys_m_list = self.get_metris_list(metric_startwith)

            self.aggregate_data_to_file(metric_startwith,metric_tag_dic,sys_m_list,sys_token)

            # 3.3 附带上(start_with和token)对每个系统35天之前数据进行删除 ==> 调用如下函数对每个系统中的每个metrics进行删除(出来anomaly那些)
            self.delete_old_data(sys_m_list,sys_token)

        # 4.上述聚合和删除操作完毕后,最后将聚合后的数据post回opentsdb
        self.post_back()


    # 1.get方式获取所有metrics的列表
    def get_metris_list(self,metric_startwith):
        metric_url = self.opentsdb_url + '/api/suggest?max=10000000&type=metrics&q=' + metric_startwith
        print metric_url
        metric_list = json.loads(requests.get(metric_url).text)
        return metric_list

    # 3.获取每个metric-tag对应关系并以json格式存入文件
    def save_tag(self,metric_list):
        start_ts=int(time.time()-86400*1000) # 为了尽可能多的获取到指标与tag,这里固定获取1000天之前的当前时间点的时间戳作为获取tag的起点
        #end_ts=start_ts+86400*30                  # 从3秒区间内获取全部指标对应的tag的列表
        end_ts=int(time.time())
        t_url=self.opentsdb_url + "/api/query"
        t_json={}
        for i in range(len(metric_list)):
            metric_name=metric_list[i].encode('utf-8')
            tag_formdata={"start":start_ts,"end":end_ts,"queries":[{"aggregator":"avg","metric":metric_name,"downsample":self.downsample}]}
            tag_response=json.loads(requests.post(t_url,data=json.dumps(tag_formdata)).text)
            if len(tag_response) == 0:
                #continue
                print str(i)+': not get tag ================================>'+metric_name
            else:
                try:
                    aggregate_tag_list = tag_response[0]['aggregateTags']
                    tags_tag_list = tag_response[0]['tags']
                except KeyError:
                    print '>>>>>>>>>>>>>>>>>>>>>KEY_ERROR'
                    continue
                if len(aggregate_tag_list) != 0:
                    tag_list = []
                    for tag in aggregate_tag_list:
                        tag_list.append(tag.encode('utf-8'))
                elif len(tags_tag_list) != 0:
                    tag_list = []
                    for tag in tags_tag_list:
                        tag_list.append(tag.encode('utf-8'))

                metric_name_list = metric_name.split('.')[2:]
                metric_real_name = '.'.join(metric_name_list)

                t_json[metric_real_name] = tag_list
                print ">>>>>>>>>>>>>>>>>>>"+metric_real_name+'---->'+str(tag_list)
                tag_list = []
            print 'getting tag:'+str(i)
        t_json = json.dumps(t_json)
        with open('../metric_tag.json','w') as f:
            f.write(t_json)


    # 4.从json文件读取tag并转为dic类型
    def conf_parse2dic(self):
        with open('../metric_tag.json','r') as f:
            metric_tag_dic=json.load(f)
        return metric_tag_dic


    # 2.查询mysql获取所有的sys与org
    def query_sys_list(self):
        conn = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user, passwd=self.mysql_pass, db=self.conn_db)
        if self.cloud == 'aws':
            sql='select api_key.org_id, name, `key`  from api_key join org_permit on api_key.org_id = org_permit.org_id join systems on api_key.name = systems.id where api_key.deleted = 0 and deadline > now() and systems.deleted = 0 and org_permit.data_center = "aws";'
        elif self.cloud == 'ucloud':
            sql='select api_key.org_id, name, `key`  from api_key join org_permit on api_key.org_id = org_permit.org_id join systems on api_key.name = systems.id where api_key.deleted = 0 and deadline > now() and systems.deleted = 0 and org_permit.data_center = "ucloud";'
        else:
            sql='select api_key.org_id, name, `key`  from api_key join org_permit on api_key.org_id = org_permit.org_id join systems on api_key.name = systems.id where api_key.deleted = 0 and deadline > now() and systems.deleted = 0;'
        try:
            cur=conn.cursor()
            cur.execute(sql)
            results = cur.fetchall()
            sys_list=[]
            for i in range(1,len(results)):
                sys_list.append(results[i])
            print '----->'+str(sys_list)
            return sys_list
        except Exception as e:
            raise e
        finally:
            conn.close()


    # 5.将metric和各个tag_key拼凑成各组合,去进行聚合,并对聚合后的数据按时间戳输出
    def aggregate_data_to_file(self,metric_startwith,metric_tag_dic,sys_m_list,sys_token):
        aggregate_url=self.opentsdb_url + "/api/query"
        self.start_ts = int(time.time()-86400*int(self.start_time))
        self.end_ts = int(time.time()-86400*int(self.end_time))
        #os.system('mv ../aggregate_data.json ../aggregate_data.json_old')
        f = open('../aggregate_data.json','a+')
        for sys_metric in sys_m_list:
            sys_metric_list = sys_metric.split('.')[2:]
            metric_name = '.'.join(sys_metric_list)

            if metric_name.endswith('anomaly') or metric_name.endswith('health') or metric_name.endswith('noise') or metric_name.endswith('periodMinutes') or metric_name.endswith('prediction') or metric_name.endswith('prediction.max') or metric_name.endswith('prediction.min') or metric_name.endswith('seasonal') or metric_name.endswith('trend'):
                continue
            else:
                try:
                    tag_list = metric_tag_dic[metric_name]
                except KeyError:
                    continue

                if len(tag_list) == 0:
                    continue
                else:
                    tag_dic={}
                    for tag in tag_list:
                        tag_dic[tag.encode('utf-8')]='*'
                metric_full_name=metric_startwith+metric_name
                aggregate_formdata={ "start": self.start_ts, "end": self.end_ts, "queries": [ { "aggregator": "avg", "metric": metric_full_name, "downsample":self.downsample, "tags": tag_dic}]}
                try:
                    aggregate_response = requests.post(aggregate_url,data=json.dumps(aggregate_formdata))
                    aggregate_list=json.loads(aggregate_response.text)
                    aggregate_code=aggregate_response.status_code
                    if aggregate_code == 400 and str(aggregate_list).find("No such name for 'metrics'") :
                        print 'no such metric ---->' + metric_full_name
                    elif aggregate_code == 200:
                        for a_dic in aggregate_list:
                            print a_dic
                            if len(a_dic['dps']) == 0:
                                continue
                            else:
                                outer_dic={}
                                middle_list=[]

                                for time_point in a_dic['dps'].keys():
                                    iner_dic={}
                                    time_value=a_dic['dps'][time_point]
                                    t_dic={}
                                    for tag_name,tag_value in a_dic['tags'].items():
                                        t_dic[tag_name]=tag_value
                                    iner_dic["tags"]=t_dic
                                    iner_dic["metric"]=metric_name    # 注意:对带有token的post塞回请求不需要
                                    iner_dic["value"]=time_value
                                    iner_dic["timestamp"]=time_point
                                    middle_list.append(iner_dic)
                                    if len(middle_list) == 100:       # 设置body中列表包含metrics的条数最大为100条
                                        outer_dic["metrics"]=middle_list
                                        outer_dic["token"]=sys_token
                                        final_str=json.dumps(outer_dic)
                                        print '******** write string data : *********'+str(final_str)
                                        f.write(final_str + '\n')
                                        middle_list=[]

                                if len(middle_list) != 0:
                                    outer_dic["metrics"]=middle_list
                                    outer_dic["token"]=sys_token
                                    final_str=json.dumps(outer_dic)
                                    print '******** write string data : *********'+str(final_str)
                                    f.write(final_str + '\n')

                                outer_dic["metrics"]=middle_list
                                outer_dic["token"]=sys_token

                                final_str=json.dumps(outer_dic)
                                print '******** write string data : *********'+str(final_str)
                                f.write(final_str + '\n')
                    else:
                        print "unknown metric type!!"
                except TypeError:
                    continue
        f.close()


    def delete_old_data(self,sys_m_list,sys_token):
        for sys_metric in sys_m_list:
            metric_full_name=sys_metric
            sys_metric_list = metric_full_name.split('.')[2:]
            metric_name = '.'.join(sys_metric_list)
            start_time_point=time.strftime("%Y/%m/%d-%H:%M:%S",time.localtime(self.start_ts))
            end_time_point=time.strftime("%Y/%m/%d-%H:%M:%S",time.localtime(self.end_ts))
            if metric_name.endswith('anomaly') or metric_name.endswith('health') or metric_name.endswith('noise') or metric_name.endswith('periodMinutes') or metric_name.endswith('prediction') or metric_name.endswith('prediction.max') or metric_name.endswith('prediction.min') or metric_name.endswith('seasonal') or metric_name.endswith('trend'):
                print 'skip delete metric : ' + metric_full_name
                continue
            else:
                delete_url=self.opentsdb_url + "/api/query?start=" + start_time_point + '&end=' + end_time_point + '&m=avg:1s-avg:'+ metric_name +'&token='+ sys_token
                print delete_url

                status_code = requests.delete(delete_url).status_code
                resp_body = str(requests.delete(delete_url).text)

                print status_code
                if status_code==200:
                    print 'successfully delete metric : ' + metric_full_name
                elif status_code == 400 and resp_body.find("No such name for 'metrics'") :
                    print 'no such metic ---->' + metric_full_name
                    continue
                else:
                    error_msg='an error occur when delete metric : ' + metric_full_name+'\n'
                    print error_msg
                    with open('../delete_error.log','a+') as f:
                        f.write(error_msg)


    def post_back(self):
        post_url=self.opentsdb_url+"/api/put?details"

        with open('../aggregate_data.json','r') as f:
            for line in f:
                post_formdata = line.strip('\n')
                post_response = requests.post(post_url,data=post_formdata)
                post_code=post_response.status_code

                if post_code == 200:
                    print 'successfully post metris back to opentsdb :' + str(post_formdata)
                else:
                    print 'error: >>>>>>>>>>>>>>> post metric back to opentsdb failed: '+str(post_formdata)
                    with open('../post_back_failed.txt','w') as fl:
                        fl.write(post_formdata + '\n')
                    continue




if __name__ == '__main__':
    config=configparser.SafeConfigParser()
    config.read('config.ini')
    ts=Tsdb_Aggregare(config)
    ts.__call__()