#!/usr/bin/env python
#coding:utf-8

import json
import requests
import time 
import datetime
import pytz
from influxdb import InfluxDBClient



class influxdbExec():
	def __init__(self):
		self.start_day=10   # 多少天之间开始查询，查询之前数据默认每天查询一次
		self.interval=60	# 单位秒

		self.influxdb_host='123.207.148.247'
		self.influxdb_port=8086
		self.influxdb_username='root'
		self.influxdb_password='root'
		self.influxdb_database='cloudapi_uat'
		self.influxdb_table_list=['FX_Ext_PRD_KFC_PreOrder_WX_443','FX_Ext_PRD_KFC_PreOrder_WX_443']
		
		self.opentsdb_url = 'http://192.168.1.204:4242'
		self.token = 'sajdkgjasdkgjaskl'

	def __call__(self):
		start_ts = int(time.mktime(time.strptime(str(datetime.date.today()-datetime.timedelta(days=self.start_day)), '%Y-%m-%d')))
		#current_ts = int(time.time())
		stop_ts = start_ts + 3600
		while stop_ts <= int(time.time()):
			print "do query per_day:" + str(start_ts) + "----->" + str(stop_ts)
			self.parse_query(self.influxdb_table_list,start_ts,stop_ts)
			start_ts = stop_ts
			stop_ts = stop_ts + 36000
		# when reach current query by interval
		current_ts = int(time.time())
		start_ts = current_ts
		while True:
			stop_ts = start_ts + self.interval
			print "do query by interval:" + str(start_ts) + "----->" + str(stop_ts)
			real_start_ts = start_ts + 120
			real_stop_ts = stop_ts - 120
			self.parse_query(self.influxdb_table_list,real_start_ts,real_stop_ts)
			start_ts = stop_ts
			time.sleep(self.interval)


	# 本地时间转换为UTC  传入的本地时间戳 1531411200
	def local_to_utc(self, local_ts, utc_format='%Y-%m-%dT%H:%M:%SZ'):
		local_tz = pytz.timezone('Asia/Chongqing')    #定义本地时区
		local_format = "%Y-%m-%d %H:%M:%S"               #定义本地时间format
		time_str = time.strftime(local_format, time.localtime(local_ts))    #首先将本地时间戳转化为时间元组，用strftime格式化成字符串
		dt = datetime.datetime.strptime(time_str, local_format)             #将字符串用strptime 转为为datetime中 datetime格式
		local_dt = local_tz.localize(dt, is_dst=None)                       #给时间添加时区，等价于 dt.replace(tzinfo=pytz.timezone('Asia/Chongqing'))
		utc_dt = local_dt.astimezone(pytz.utc)                              #astimezone切换时区
		return utc_dt.strftime(utc_format)

	def conn_influxdb(self):
		#return InfluxDBClient(host='123.207.148.247',port=8086,username='root',password='root',database='cloudapi_uat')
		return InfluxDBClient(host=self.influxdb_host,port=self.influxdb_port,username=self.influxdb_username,password=self.influxdb_password,database=self.influxdb_database)

	def query_influxdb(self,table,start_ts,stop_ts):
		start_utc = self.local_to_utc(start_ts)
		stop_utc = self.local_to_utc(stop_ts)
		sql = "select * from %s where  time >= %s and time <= %s" % ('"'+table+'"', "'"+start_utc+"'", "'" + stop_utc + "'")
		#sql = 'select * from "FX_Ext_PRD_KFC_PreOrder_WX_443" where  time >= \'2019-01-06T16:00:00Z\' and time <= \'2019-01-06T16:01:00Z\''
		print sql
		client = self.conn_influxdb()
		result = client.query(sql,epoch="s")
		if len(result) != 0:
			result_list = result.raw['series'][0]['values']
			return result_list
		else:
			return []

	
	def parse_query(self,table_list,start_ts,stop_ts):
		print table_list
		for table in table_list:
			result_list = self.query_influxdb(table,start_ts,stop_ts)
			'''
			for message in result_list:
				print "------------------------------------"
				print "time: "+str(message[0]) 
				print "type: "+str(message[1])
				print "value: "+str(message[2])
			'''
			if len(result_list) == 0:
				print "query result: no data !!"
			else:
				outer_dic = {}
				middle_list = []
				for metric in result_list:
					inner_dic = {}
					tags = {"host":"InfluxdbTestHost01"}
					inner_dic["timestamp"] = metric[0]
					inner_dic["metric"] = str(table) + "." + str(metric[1].encode('raw_unicode_escape'))
					inner_dic["value"] = metric[2]
					inner_dic["tags"] = tags
					middle_list.append(inner_dic)
					if len(middle_list) == 100:
						outer_dic["metrics"] = middle_list
						outer_dic["token"] = self.token
						self.send2tsdb(outer_dic)
						print outer_dic
						middle_list = []
				if len(middle_list) != 0:
					outer_dic["metrics"] = middle_list
					outer_dic["token"] = self.token
					self.send2tsdb(outer_dic)
					print outer_dic


	def send2tsdb(self,formdata):
		formdata = json.dumps(formdata)
		post_url = self.opentsdb_url+"/api/put?details"
		post_resp = requests.post(post_url,data=formdata)
		if post_resp.status_code == 200:
			print ">>>>> sucdessfully post to opentsdb :" + formdata
		else:
			print "<<<<< failed post to opentsdb:" + formdata


if __name__=='__main__':
	influx = influxdbExec()
	#influx.parse_query()
	influx.__call__()







