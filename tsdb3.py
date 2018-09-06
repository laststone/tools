#!/usr/bin/env python
# coding:utf-8

import requests
import json
import time,datetime
import pymysql
import shutil
import os


# 1.设置可选择参数
# 该参数用于获取最新的metrics对应的tags,根据系统变化,通常获取一次后不用再次获取,一般与reget_tags=True参数一同使用
metric_startwith="2.2."
reget_tags=False


#start_time即从多少天之前开始聚合
start_time=600
#end_time即聚合停止在多少天之前
end_time=35
interval=20
#downsample="1d-avg"
downsample="2h-avg"
opentsdb_url="http://192.168.1.204:4242"


# mysql configuration
mysql_host='192.168.1.204'
mysql_port=3306
mysql_user='CloudInsight'
mysql_pass='Cloud'
conn_db='grafana'
cloud='offline'

#以下全局变量默认为空,不要随意修改
start_ts=''
end_ts=''
token=''


# 使用须知:
# (1) 需要更改对应的SQL语句
# (2) 若需要重新获取metrics-tags对应表需要根据系统情况添加metric_startwith
# (3) 添加数据库访问路径和opentsdb路径


# 1.get方式获取所有metrics的列表
def get_metris_list(m_url,m_starwith):
    m_url=m_url+"/api/suggest?max=10000000&type=metrics&q="+m_starwith
    m_list=json.loads(requests.get(m_url).text)
    return m_list


# 2.查询mysql获取所有的sys与org
def query_sys_list(host,port,user,passwd,db):
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)
    if cloud == 'aws':
        sql='select api_key.org_id, name, api_key.`key` from api_key, org_permit where api_key.org_id = org_permit.org_id and org_permit.data_center = "aws";'
    elif cloud == 'ucloud':
        sql='select api_key.org_id, name, api_key.`key` from api_key, org_permit where api_key.org_id = org_permit.org_id and org_permit.data_center = "ucloud";'
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


# 3.获取每个metric-tag对应关系并以json格式存入文件
#def save_tag(t_url,m_list,star_time):
def save_tag(t_url,m_list):
    start_ts=int(time.time()-86400*1000) # 获取30天之前的当前时间点的时间戳作为获取tag的起点
    #end_ts=start_ts+86400*30                  # 从3秒区间内获取全部指标对应的tag的列表
    end_ts=int(time.time())
    t_url=t_url+"/api/query"
    t_json={}
    for i in range(len(m_list)):
        metric_name=m_list[i].encode('utf-8')
        t_formdata={"start":start_ts,"end":end_ts,"queries":[{"aggregator":"avg","metric":metric_name,"downsample":downsample}]}
        t_response=json.loads(requests.post(t_url,data=json.dumps(t_formdata)).text)
        if len(t_response) == 0:
            #continue
            print str(i)+': not get tag ================================================>'+metric_name
        else:
            try:
                aggregate_tag_list=t_response[0]['aggregateTags']
                tags_tag_list=t_response[0]['tags']
            except KeyError:
                print '+++++++++++++++++++++++++++++++++++++++++++++++++++>>>>>>KEY_ERROR'
                continue
            if len(aggregate_tag_list) != 0:
                tag_list=[]
                for tag in aggregate_tag_list:
                    tag_list.append(tag.encode('utf-8'))
            elif len(tags_tag_list) != 0:
                tag_list=[]
                for tag in tags_tag_list:
                    tag_list.append(tag.encode('utf-8'))

            metric_name_list=metric_name.split('.')[2:]
            metric_real_name='.'.join(metric_name_list)

            t_json[metric_real_name]=tag_list
            print ">>>>>>>>>>>>>>>>>>>   "+metric_real_name+'---->'+str(tag_list)
            tag_list=[]
        print 'getting tag:'+str(i)
    t_json=json.dumps(t_json)
    with open('../metric_tag.json','w') as f:
        f.write(t_json)
        print "========================="
        print len(t_json)
        print "========================="


# 4.从json文件读取tag并转为dic类型
def conf_parse2dic():
    with open('../metric_tag.json','r') as f:
        metric_tag_dic=json.load(f)
    return metric_tag_dic

# 5.将metric和各个tag_key拼凑成各组合,去进行聚合,并对聚合后的数据按时间戳输出
def aggregate_data_to_file(a_url,metric_tag_dic,start_time,end_time=35):
    a_url=a_url+"/api/query"
    global start_ts
    global end_ts
    start_ts=int(time.time()-86400*start_time)
    end_ts=int(time.time()-86400*end_time)

    os.rename('../aggregate_data.txt','../aggregate_data.old')
    #shutil.copyfile('../aggregate_data.txt','../aggregate_data.old')

    with open('../aggregate_data.txt','w+') as f:
        for key in metric_tag_dic.keys():
            if key.endswith('anomaly') or key.endswith('health') or key.endswith('noise') or key.endswith('periodMinutes') or key.endswith('prediction') or key.endswith('prediction.max') or key.endswith('prediction.min') or key.endswith('seasonal') or key.endswith('trend'):
                continue
            else:
                metric_name=key
                tag_list=metric_tag_dic[metric_name]
                if len(tag_list) == 0:
                    continue
                else:
                    tag_dic={}
                    for tag in tag_list:
                        tag_dic[tag.encode('utf-8')]='*'
                    print tag_dic
                metric_full_name=metric_startwith+metric_name
                a_formdata={ "start": start_ts, "end": end_ts, "queries": [ { "aggregator": "avg", "metric": metric_full_name, "downsample":downsample, "tags": tag_dic}]}
                try:
                    a_list=json.loads(requests.post(a_url,data=json.dumps(a_formdata)).text)
                    a_code=requests.post(a_url,data=json.dumps(a_formdata)).status_code
                    aaa=a_code
                    if a_code == 400 and str(a_list).find("No such name for 'metrics'") :
                        print 'no such metric ---->' + metric_full_name
                    elif a_code == 200:
                        for a_dic in a_list:
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

                                outer_dic["metrics"]=middle_list
                                outer_dic["token"]=sys_token

                                final_str=json.dumps(outer_dic)
                                print final_str
                                f.write(final_str + '\n')
                    else:
                        print "unknown metric type!!"
                except TypeError:
                        continue


def delete_old_data(d_url,metric_tag_dic,start_ts,end_ts):

    for key in metric_tag_dic.keys():
        metric_name=key
        start_time_point=time.strftime("%Y/%m/%d-%H:%M:%S",time.localtime(start_ts))
        end_time_point=time.strftime("%Y/%m/%d-%H:%M:%S",time.localtime(end_ts))
        metric_full_name=metric_startwith+key
        if key.endswith('anomaly') or key.endswith('health') or key.endswith('noise') or key.endswith('periodMinutes') or key.endswith('prediction') or key.endswith('prediction.max') or key.endswith('prediction.min') or key.endswith('seasonal') or key.endswith('trend'):
            print 'skip delete metric : ' + metric_full_name
            continue
        else:
            delete_url=d_url+"/api/query?start="+start_time_point+'&end='+end_time_point+'&m=avg:1s-avg:'+metric_name+'&token='+sys_token
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

def post_back(p_url):
        p_url=p_url+"/api/put?details"

        #with open('../aggregate_data.txt','r') as f:
        with open('../aaaaaaa.txt','r') as f:
            for line in f:
                p_formdata=line.strip('\n')
                p_code=requests.post(p_url,data=p_formdata).status_code
                p_body=requests.post(p_url,data=p_formdata).text

                aaa='cccc'

                if p_code == 200:
                    print 'successfully post metris back to opentsdb :' + str(p_formdata)
                else:
                    print 'error: >>>>>>>>>>>>>>> post metric back to opentsdb failed: '+str(p_formdata)
                    with open('../post_back_failed.txt','w') as fl:
                        fl.write(p_formdata + '\n')
                    continue





if __name__ == '__main__':

    # 1.从数据库中查所有的sys_org组合,并取出第一组拼接成metric的头部字符串
    sys_org_list = query_sys_list(mysql_host,mysql_port,mysql_user,mysql_pass,conn_db)
    first_sys_tuple = sys_org_list[0]
    metric_startwith = str(first_sys_tuple[0]) + '.' + str(first_sys_tuple[1]) + '.'

    # 2.判断是否要重新获取metrics与相应tags的对应关系
    # 如果需要重新获取则,执行下述操作将重新获取到的metrics与tags的对应关系存入metric_tag.json
    if reget_tags:
        m_list=get_metris_list(opentsdb_url,metric_startwith)
        save_tag(opentsdb_url,m_list)

    # 3.从metric_tag.json文件中读取metric-tag对应关系为字典,并结合所有metrics的列表,进行for循环遍历,遍历出所有metic-tag组合执行如下操作:
    m_list = get_metris_list(opentsdb_url,metric_startwith)
    m_tag_dic = conf_parse2dic()

    # 3.1 遍历所有sys_org列表,取出每个元素的(start_with和token)
    for sys in sys_org_list:
        metric_startwith = str(sys[0])+'.'+str(sys[1])+'.'
        sys_token = sys[2]

        bb = metric_startwith
        #metric_startwith = '2.2.'
        #sys_token = '9c43fce52bcf44dcc55e8ee6a4288c8ccbf29125'



        print '------the end---------'
        print len(m_tag_dic)


        # 3.2 附带上(start_with和token)对每个系统进行聚合 ==> 调用如下函数对每个系统中的每个(metric+tags)组合进行聚合,将聚合数据追加写入aggregate_data.txt (除了anomaly那些)
        aggregate_data_to_file(opentsdb_url,m_tag_dic,start_time,end_time)

        a = start_ts
        b = end_ts
        c = metric_startwith

        #test_dic={"kafka.bytesrate.BytesOutPerSec.MeanRate": ["host"], "hbase.regionserver.info.storefileSizeMB": ["host"], "mysql.com_show_fields.prediction.max": ["host"], "hbase.regionserver.Threading.PeakThreadCount.prediction.max": ["host"], "hbase.master.ipc.TotalCallTime_max.prediction.min": ["host"], "azure.storage.blob.anonymous.success.periodMinutes": ["host", "topN"], "azure.web.application.site.private.bytes.prediction.min": ["host"]}
        #delete_old_data(opentsdb_url,test_dic,start_ts,end_ts)


        # 3.3 附带上(start_with和token)对每个系统35天之前数据进行删除 ==> 调用如下函数对每个系统中的每个metrics进行删除(出来anomaly那些)
        delete_old_data(opentsdb_url,m_tag_dic,start_ts,end_ts)

        cc=m_tag_dic


    # 4.上述聚合和删除操作完毕后,最后将聚合后的数据post回opentsdb
    #############################测试完成改回缩进######################################################################################################
        post_back(opentsdb_url)

        dd='aaa'


