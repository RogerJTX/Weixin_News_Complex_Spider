#coding=utf8
# """
# 微信搜狗
# url：https://weixin.sogou.com/weixin?query=%E9%98%BF%E9%87%8C%E5%B7%B4%E5%B7%B4&_sug_type_=&sut=2907&lkt=1%2C1594956457942%2C1594956457942&s_from=input&_sug_=y&type=2&sst0=1594956458043&page=1&ie=utf8&w=01019900&dr=1
# author: jtx
# """

import sys,os
sys.path.append('')

import sys
import os
import re
from lxml import etree
from bs4 import BeautifulSoup
import logging
import pymongo
import base64
import urllib
import time, requests
import datetime, random
from etl.utils.log_conf import configure_logging
import traceback
from etl.data_gather.settings import SAVE_MONGO_CONFIG2, RESOURCE_DIR
from etl.common_spider.donwloader import Downloader
from etl.data_gather.record_format import News, CompanyKey



class ListDetailSpider(object):
    def __init__(self, config, proj_dir=None, logger=None, coll_name = 'news01'):
        config

        self.start_down_time = datetime.datetime.now()
        self.down_retry = 3
        if not logger:
            configure_logging("WX_processive_basic01.log")
            self.logger = logging.getLogger("spider")
        else:
            self.logger = logger
        self.downloader = Downloader(self.logger, need_proxy=True)  # 注意是否需要使用代理更改参数
        self.headers = {
                        "Host": "weixin.sogou.com",
                        # "Cookie": "sgmid=1cd1868034033465881; sgxid=2805342a15c13a06922e1656eb5b1daba16f; usid=_yhv74PjYwQi4_Ua; IPLOC=CN3301; SUV=00FF6D3173C8FFDB5F0EC82088B35928; FREQUENCY=1594804259469_1; front_screen_resolution=1080*2160; gpsloc=%E6%B5%99%E6%B1%9F%E7%9C%81%09%E6%9D%AD%E5%B7%9E%E5%B8%82; ssuid=1920472152; ABTEST=0|1594804271|v1; SUID=DBFFC8733F18960A000000005F0EC82F; SNUID=694E7AC1B2B718C5FECE4231B237BF6D; JSESSIONID=aaasJ68o_eaNndlx5Smnx; SUID=DBFFC8732B12960A000000005F0EC84A",
                        "User-Agent": "Mozilla/5.0 (Linux; Android 10; BLA-AL00 Build/HUAWEIBLA-AL00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/72.0.3626.121 Mobile Safari/537.36 SCore/315 SGInfo/1080/2040/3.0 SogouSearch Android1.0 version3.0 AppVersion/7512 NoHead",
                        "Referer": "https://weixin.sogou.com/weixinwap?type=2&ie=utf8&pid=sogou-clse-ddcbe25988981920&w=1580&uID=1cd1868034033465881&mid=1cd1868034033465881&xid=2805342a15c13a06922e1656eb5b1daba16f&query=%E6%88%91%E4%BB%AC&dp=1",
    }
        self.headers2 = {
            "Host": "weixin.sogou.com",
            "User-Agent": "Mozilla/5.0 (Linux; Android 10; BLA-AL00 Build/HUAWEIBLA-AL00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/72.0.3626.121 Mobile Safari/537.36 SCore/315 SGInfo/1080/2040/3.0 SogouSearch Android1.0 version3.0 AppVersion/7512 NoHead",
            "Referer": "https://weixin.sogou.com/weixinwap?type=2&ie=utf8&pid=sogou-clse-ddcbe25988981920&w=1580&uID=1cd1868034033465881&mid=1cd1868034033465881&xid=2805342a15c13a06922e1656eb5b1daba16f&query=%E6%88%91%E4%BB%AC&dp=1",
        }

        self.model = News(self.mongo_coll, self.logger)
        News.pk = {"title", "publish_time"}
        self.format = self.model.temp

    def get_mongo(self, host, port, db, username, password):
        if username and password:
            url = "mongodb://%s:%s@%s:%s/%s" % (username, password, host, port, db)
        else:
            url = "mongodb://%s:%s" % (host, port)
        return pymongo.MongoClient(url)

    def save_record(self, record):
        ret = self.model.save(record)
        if ret:
            self.save_each_company += 1
            self.save += 1

    def get_k_h(self, url):
        b = int(random.random() * 100) + 1
        a = url.find("url=")
        url = url + "&k=" + str(b) + "&h=" + url[a + 4 + 21 + b: a + 4 + 21 + b + 1]
        return url

    def format_cookie(self, cookie_dict):
        c_list = []
        for k, v in cookie_dict.items():
            c_list.append("%s=%s" % (k, v))
        cookie_str = ";".join(c_list)
        return cookie_str

    def judge_date(self, last_crawl_time, crawl_frequency, company_name):
        judge_start_date = datetime.datetime.strptime(last_crawl_time, "%Y-%m-%d")
        time_now = datetime.datetime.now()

        delta = datetime.timedelta(days=-int(crawl_frequency))
        n_days = time_now + delta
        str_time_now = str(n_days)[:10]
        judge_time = str(str_time_now)
        judge_time = datetime.datetime.strptime(judge_time, "%Y-%m-%d")
        if judge_time >= judge_start_date:
            self.logger.info('该公司需要采集, 日期符合要求:%s' %  company_name)
            flag = 1
            return flag
        else:
            self.logger.info('该公司不需要采集, 日期不符合要求:%s' % company_name)
            flag = 0
            return flag

    def update_mongo(self, _id, col_name):
        myquery = {"_id": _id}
        newvalues = {"$set": {"last_crawl_num": self.save_each_company,
                              "last_crawl_time": str(datetime.datetime.now())[:10]}}
        col_name.update_one(myquery, newvalues)
        self.logger.info('更新成功, last_crawl_num:%s' % str(self.save_each_company))

    def update_mongo_publish_date(self, _id, col_name, x):
        ls = list(x)
        # 用了冒泡排序来排序，其他方法效果一样
        for j in range(len(ls) - 1):
            for i in range(len(ls) - j - 1):
                lower = ls[i].split('-')
                upper = ls[i + 1].split('-')
                for s in range(3):
                    if int(lower[s]) > int(upper[s]):
                        ls[i], ls[i + 1] = ls[i + 1], ls[i]
                        break
                    elif int(lower[s]) < int(upper[s]):
                        break
        ar = tuple(ls)
        last_publish_time = ar[-1]
        myquery = {"_id": _id}
        newvalues = {"$set": {"last_publish_time": last_publish_time}}
        col_name.update_one(myquery, newvalues)
        self.logger.info('更新成功, last_publish_time:%s' % last_publish_time)

    def update_mongo_publish_date_sort_function(self, _id, col_name, x):
        # ls = list(x)
        # print(x)
        x.sort()
        last_publish_time = x[-1]
        myquery = {"_id": _id}
        newvalues = {"$set": {"last_publish_time": last_publish_time}}
        col_name.update_one(myquery, newvalues)
        self.logger.info('更新成功, last_publish_time:%s' % last_publish_time)



    def start_run(self, begin_key=None, start_date="2014-01-01", end_date="", max_page=10, range_idx=None, start_page=1, industry=None, q=None):
        """
        数据采集主入口
        :return:
        """
        self.logger.info("Begin Run")
        # ============主页面获取==============================

        page_no = start_page
        total_page = max_page




        each_mongo = {}
        each_mongo['company_name'] = '德清 王琴英'
        each_mongo['search_keys'] = '德清 王琴英'
        list_mongo = [each_mongo]

        if list_mongo:
            flag = 0
            for num_first, name_c in enumerate(list_mongo):
                # 选取从哪个公司开始
                publish_time_list = []

                company_name = name_c['company_name']
                self.logger.info('第几个公司:%s, name:%s'%(num_first, company_name))
                # if name == '广东超华科技股份有限公司':
                #     flag = 1
                # _id = name_c['_id']
                search_keys = name_c['search_keys']
                # last_crawl_time = name_c['last_crawl_time']
                # last_crawl_num = name_c['last_crawl_num']
                # crawl_frequency = name_c['crawl_frequency']

                # 判断是否采集，通过日期
                flag = 1

                if flag == 1:
                    self.save_each_company = 0
                    name = search_keys
                    for num in range(start_page, total_page+1):
                        # time_judge1 = datetime.datetime.now()

                        # 这里调用ip代理


                        url = "https://weixin.sogou.com/weixinwap?ie=utf8&s_from=input&type=2&t=1594880241&pg=webSearchList&_sug_=y&_sug_type_=&query=" + name + "&page=" + str(
                            num)
                        self.logger.info('list_page_url:%s' % url)
                        resp = self.downloader.crawl_data(url, None, self.headers, "get", None, None, None)
                        if resp:
                            resp.encoding = "utf8"
                            content = resp.text
                            if '用户您好，我们的系统检测到您网络中存在异常访问请求' in content:
                                self.logger.info('跳出验证码')
                                p = self.downloader.get_proxy(True)
                                self.logger.info(p)
                                resp = self.downloader.crawl_data(url, None, self.headers, "get", None, None, None)
                            if '访问过于频繁，请用微信扫描二维码进行访问' in content:
                                self.logger.info('请用微信扫描二维码进行访问')
                                p = self.downloader.get_proxy(True)
                                self.logger.info(p)
                                resp = self.downloader.crawl_data(url, None, self.headers, "get", None, None, None)

                            soup = BeautifulSoup(content, "lxml")
                            tag_news_box = soup.find('div', {'class':'results'})
                            if tag_news_box:
                                list_news_box = tag_news_box.find_all('li')
                                if list_news_box:
                                    for each_li in list_news_box:
                                        try:

                                            # tag_time_flag = 0
                                            # tag_time = each_li.find('div',{'class':'list-txt'})
                                            # if tag_time:
                                            #     tag_time_flag = 1
                                            # if tag_time_flag == 1:
                                            tag_time = each_li.find('span', {'class':'s3'})
                                            if tag_time:
                                                publish_time = tag_time.get_text().strip()


                                                if ('天前' in publish_time):
                                                    day = publish_time[0]
                                                    now = datetime.datetime.now()
                                                    delta = datetime.timedelta(days=int(day))
                                                    publish_time = str(now - delta)[:10]
                                                if ('小时' in publish_time):
                                                    hours = publish_time[0]
                                                    now = datetime.datetime.now()
                                                    delta = datetime.timedelta(hours=int(hours))
                                                    publish_time = str(now - delta)[:10]
                                                if ('分钟' in publish_time):
                                                    hours = 1
                                                    now = datetime.datetime.now()
                                                    delta = datetime.timedelta(hours=int(hours))
                                                    publish_time = str(now - delta)[:10]
                                                judge_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
                                                judge_time = str(publish_time)
                                                judge_time = datetime.datetime.strptime(judge_time, "%Y-%m-%d")
                                                # print("2017-11-02大于2017-01-04：", judge_time > end_date)
                                                if judge_time >= judge_start_date:
                                                    flag_time = 1
                                                else:
                                                    flag_time = 0
                                                    self.logger.info("过期数据, pass")


                                                if flag_time == 1:
                                                    time.sleep(0.5)
                                                    # 日期规范部分
                                                    publish_time_tag = datetime.datetime.strptime(publish_time,
                                                                                                  "%Y-%m-%d")
                                                    publish_time_tag2 = publish_time_tag.strftime('%Y-%m-%d')
                                                    self.logger.info(publish_time_tag2)
                                                    publish_time = publish_time_tag2
                                                    if publish_time:
                                                        publish_time_list.append(publish_time)
                                                    else:
                                                        continue


                                                    new_cookie = {}
                                                    for k, v in resp.cookies.items():
                                                        new_cookie[k] = v
                                                    # url2 = "http://mp.weixin.qq.com/s?src=11&timestamp=1594869661&ver=2462&signature=oDJAWNu6BI7lLUQ2YC*foUCznzLntcLpLP*H4thyrv7X9nPjwL2LIUaR59YoHQ7-HSB6zdGMMkfVizi9PK0nLykfS2R7*dU7fF5geG89n6wMi-1dt3sgxEMXwnd9FiwX&new=1"
                                                    self.headers2["Cookie"] = self.format_cookie(new_cookie)
                                                    print(self.headers2['Cookie'])

                                                    # r2 = requests.get(url2, headers=headers)
                                                    # print(r2.text)
                                                    t = each_li.find('h4')
                                                    title = t.get_text().strip()
                                                    print("title:%s" % t.get_text().strip())
                                                    if '聘' not in title:
                                                        self.headers2["Host"] = "weixin.sogou.com"
                                                        cur_url = "https://weixin.sogou.com" + t.find('a')["href"]
                                                        # print(cur_url)
                                                        detail_resp = self.downloader.crawl_data(cur_url, None, self.headers2, "get")
                                                        # detail_resp = requests.get(cur_url, headers=self.headers2)
                                                        detail_resp.encoding = "utf8"
                                                        # print(detail_resp.text)

                                                        b = re.findall(r"var url = '';(.+?)url\.replace", str(detail_resp.text), re.S)
                                                        # print(b)
                                                        if b:
                                                            url_pin = ''
                                                            b_list = b[0].split('\n')
                                                            # print(b_list)
                                                            for i in b_list:
                                                                i = i.replace(' ', '').replace('\t', '')
                                                                # print(i)
                                                                if len(i) > 2:
                                                                    each_url = re.findall(r"url\+='(.+?)';", str(i))
                                                                    # print(each_url)
                                                                    url_pin += each_url[0]
                                                            print(url_pin)

                                                            if url_pin:
                                                                cur_url_pinjie = self.get_k_h(url_pin)
                                                                self.headers2["Host"] = "mp.weixin.qq.com"
                                                                detail_resp_pin = self.downloader.crawl_data(cur_url_pinjie, None, self.headers2,
                                                                                                  "get")
                                                                # detail_resp_pin = requests.get(cur_url_pinjie, headers=self.headers2)
                                                                detail_resp_pin.encoding = 'utf-8'
                                                                content = detail_resp_pin.text

                                                                record = self.parse_detail(content, cur_url, publish_time, name, title)
                                                                if record:
                                                                    self.save_record(record)


                                                    print('save:', self.save)
                                            else:
                                                self.logger.info('没有tag_time, 跳过')
                                        except Exception as e_li:
                                            self.logger.info('Error e_li:%s' % e_li)
                            else:
                                self.logger.info('没有找到li')
                        else:
                            self.logger.info('页面未响应, 公司搜索未响应, 跳过')

                        # time_judge2 = datetime.datetime.now()

                    # self.update_mongo(_id, self.mongo_col2)
                    self.logger.info('save:%s' % str(self.save))
                else:
                    continue

                # try:
                #     if publish_time_list:
                #         while '' in publish_time_list:
                #             publish_time_list.remove('')
                #         # print(publish_time_list)
                #         if publish_time_list:
                #             self.update_mongo_publish_date_sort_function(_id, self.mongo_col2, publish_time_list)
                #     # self.update_mongo_publish_date(_id, self.mongo_col2, pulish_time_list)
                # except Exception as update_error:
                #     self.logger.info('Update Error:%s' % update_error)

        self.logger.info("Finish Run")
        return self.save


    def parse_detail(self, detail_content, detail_url, publish_time, name, title):
        record = {}
        record["html"] = ""
        record['industry'] = ""
        record["title"] = title
        record["publish_time"] = publish_time
        record["crawl_time"] = datetime.datetime.now()
        record["source"] = self.host_name
        site_id = "https://weixin.sogou.com/"
        record["site_id"] = site_id
        record["tag"] = ''
        record["channel"] = '资讯'
        record["language"] = 'zh'
        record["search_key"] = name
        record["tag"] = []
        record["url"] = detail_url
        img_url_list = []
        content = ''

        try:

            soup = BeautifulSoup(detail_content, "lxml")
            # TODO 取详情页中有用的信息，注意确保数据结构完整,字段统一,做好容错处理
            # print soup
            try:
                # TODO content
                content_tag = soup.find(name='div', attrs={'class': 'rich_media_content'})
                # print content
                content = content_tag.get_text().strip()
                # print content   # 去掉多的换行
            except:
                self.logger.error("详情页抓取正文报错:%s%s" % (traceback.format_exc(), detail_url))

            try:
                # TODO img_url
                img_tag = soup.find(name='div', attrs={'class': 'rich_media_content'})
                img_all = img_tag.find_all('img')
                # print img_all
                if img_all:
                    for i in img_all:
                        img = None
                        try:
                            img = i['data-src']
                        except:
                            pass
                        img_url_list.append(img)
                # print (img_url_list)
            except:
                self.logger.error("详情页抓取图片报错:%s%s" % (traceback.format_exc(), detail_url))
                img_url_list = None

            author = ''
            # TODO author
            author_tag = soup.find(name='div', attrs={'class': 'rich_media_meta_list'})
            if author_tag:
                author = author_tag.find('a').get_text().strip()
                print(author)
            else:
                self.logger.info("详情页抓取作者报错:%s%s" % (traceback.format_exc(), detail_url))

            record["content"] = content
            record["img_url"] = img_url_list
            record["author"] = author

        except:
            self.logger.error("详情页出错:%s%s" % (traceback.format_exc(), detail_url))

        return record


def run(db, coll_name, last_run_time, logger, *args):
    SAVE_MONGO_CONFIG2["db"] = db
    bp = ListDetailSpider(SAVE_MONGO_CONFIG2, logger=logger, coll_name=coll_name)
    last_run_time = last_run_time.split(" ")[0]  # e.g.  2019-10-19
    # industry = "lz_data_gather_ai"  # 临时修改
    count = bp.start_run("", start_date=last_run_time, max_page=10)
    return count


if __name__ == '__main__':
    db = "res_kb"
    coll_name = "deqing_news"
    last_run_time = "2014-01-01"
    logger = None
    args = []
    run(db, coll_name, last_run_time, logger, *args)
    # bp = ListDetailSpider(SAVE_MONGO_CONFIG)
    # bp.start_run(start_page=1, max_page=-1)