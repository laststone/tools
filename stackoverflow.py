# /usr/bin/env python
# coding:utf-8

import requests
import random
from lxml import etree,html
from lxml.html.clean import clean_html
import datetime
import time
import pymongo

key_word='system'
start_page=1
page_size=30

class Spider:
    def __init__(self,key_word,start_page,page_size):
        self.key_word=key_word
        self.start_page=start_page
        self.page_size=page_size
        self.detail_url=""

        self.user_agent_list=[
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:60.0) Gecko/20100101 Firefox/60.0",
            "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; Touch; rv:11.0) like Gecko",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:61.0)",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36"
        ]

    def get_list_page(self,start_page):

        self.url='https://stackoverflow.com/questions/tagged/'+self.key_word+'?sort=newest&pageSize='+str(self.page_size)+'&page='+str(start_page)
        user_agent=random.choice(self.user_agent_list)
        headers={"User-Agent":user_agent}
        list_page=requests.get(self.url,headers=headers).text
        list_page_dom=etree.HTML(list_page)
        detail_page_url_list=list_page_dom.xpath('//div[@class="question-summary"]//div[@class="summary"]/h3/a/@href')
        for detail_url in detail_page_url_list:
            detail_url='https://stackoverflow.com'+detail_url
            self.get_detail_page(detail_url)
            time.sleep(2)

    def get_detail_page(self,detail_url):
        self.detail_url=detail_url
        user_agent=random.choice(self.user_agent_list)
        headers={"User-Agent":user_agent}
        detail_page=requests.get(self.detail_url,headers=headers).text
        self.parse_page(detail_page)

    def parse_page(self,detail_page):
        detail_page_dom=etree.HTML(detail_page)
        title = detail_page_dom.xpath('//div[@id="question-header"]//h1//a')[0].text
        symptom = detail_page_dom.xpath('//div[@class="question"]//div[@class="post-text"]')[0]
        symptom = etree.tostring(symptom)
        symptom = symptom.decode()
        symptom_plain_text = self.get_plain_text(symptom)
        tags = detail_page_dom.xpath('//div[@class="post-taglist"]/a[@class="post-tag"]/text()')
        answers = detail_page_dom.xpath('//div[starts-with(@id, "answer-")]')
        if len(answers) > 0:

            client=pymongo.MongoClient('mongodb://123.207.148.247:27017/system_db')
            db=client.system_db
            collection=db.answers

            for answer in answers:
                solution = answer.xpath('.//div[contains(@class, "answercell")]//div[@class="post-text"]')[0]
                solution = etree.tostring(solution)
                solution = solution.decode()
                solution_plain_text = self.get_plain_text(solution)

                item = {}
                item['title'] = title
                item['symptom'] = str(symptom)
                item['symptom_plain_text'] = symptom_plain_text
                item['source_url'] = self.detail_url
                item['tags'] = tags
                item['language'] = ['en']
                item['service'] = self.key_word
                item['created_at'] =datetime.datetime.now()
                item['solution'] = str(solution)
                item['solution_plain_text'] = solution_plain_text

                collection.insert_one(item)
                print (item)

            #with open('../crawl_data.txt','w+') as f:
            #    f.write(str(item))
        else:
            print ('no answer')


    def get_plain_text(self,symptom):
        tree = html.fromstring(symptom)
        clean_tree = clean_html(tree)
        return clean_tree.text_content().strip()


    def start_work(self):
        for page in range(1,4001):
            start_page = page
            self.get_list_page(start_page)



if __name__=='__main__':
    mySpider=Spider(key_word,start_page,page_size)
    mySpider.start_work()
