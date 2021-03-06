# """
# 微信搜狗 第二次跑的版本
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
from urllib.parse import quote


class ListDetailSpider(object):
    def __init__(self, config, proj_dir=None, logger=None, coll_name = 'news01'):
        config

        self.start_down_time = datetime.datetime.now()
        self.down_retry = 3
        if not logger:
            configure_logging("WX_processive_subscription.log")
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

    # def get_mongo(self, host, port, db, username, password):
    #     if username and password:
    #         url = "mongodb://%s:%s@%s:%s/%s" % (username, password, host, port, db)
    #     else:
    #         url = "mongodb://%s:%s" % (host, port)
    #     return pymongo.MongoClient(url)

    def get_mongo(self, host, port, db, username, password, auth_db=""):
        """
        简单快速创建mongo对象
        :param host:
        :param port:
        :param db:
        :param username:
        :param password:
        :return:
        """
        if username and password:
            if auth_db:
                db = auth_db
            if username.find("@") >= 0:
                username = quote(username)
            if password.find("@") >= 0:
                password = quote(password)
            url = "mongodb://%s:%s@%s:%s/%s" % (username, password, host, port, db)
        else:
            url = "mongodb://%s:%s" % (host, port)
        return pymongo.MongoClient(url)

    def save_record(self, record, _id):
        ret = self.model.save(record)
        if ret:
            self.save_each_company += 1
            self.save += 1
            self.update_mongo_last_crawl_title(_id, self.mongo_col2, record['title'])

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

    def update_mongo_last_crawl_title(self, _id, col_name, title):
        myquery = {"_id": _id}
        newvalues = {"$set": {"last_crawl_title": title}}
        col_name.update_one(myquery, newvalues)
        self.logger.info('更新成功, last_crawl_title:%s' % str(title))

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



    def start_run(self, begin_key=None, start_date="", end_date="", max_page=10, range_idx=None, start_page=1, industry=None, q=None):
        """
        数据采集主入口
        :return:
        """
        self.logger.info("Begin Run")
        # ============主页面获取==============================

        page_no = start_page
        total_page = max_page

        while True:


            list_mongo = []
            for each_mongo in self.mongo_col2.find({'type':'public_code'}).sort("_id", -1):
                record_each_mongo = {}
                _id = each_mongo['_id']
                _id_four = str(_id)[-4:]
                _id_four_10 = int(_id_four, 16)  # 十进制
                # print(_id)
                # print(_id_four)
                # print(int(_id_four, 16))
                company_name = each_mongo['company_name']
                search_keys = each_mongo['search_keys']
                last_crawl_time = each_mongo['last_crawl_time']
                last_crawl_num = each_mongo['last_crawl_num']
                crawl_frequency = each_mongo['crawl_frequency']
                last_publish_time = each_mongo.get('last_publish_time', '')
                last_crawl_title = each_mongo.get('last_crawl_title')


                record_each_mongo['last_publish_time'] = last_publish_time
                record_each_mongo['company_name'] = company_name
                record_each_mongo['search_keys'] = search_keys
                record_each_mongo['last_crawl_time'] = last_crawl_time
                record_each_mongo['crawl_frequency'] = crawl_frequency
                record_each_mongo['last_crawl_num'] = last_crawl_num
                record_each_mongo['_id'] = _id
                record_each_mongo['last_crawl_title'] = last_crawl_title
                # if _id_four_10 % 2 != 0:
                list_mongo.append(record_each_mongo)


            if list_mongo:
                flag = 0
                for num_first, name_c in enumerate(list_mongo):
                    # 选取从哪个公司开始
                    publish_time_list = []


                    company_name = name_c['company_name'].encode('utf-8')
                    self.logger.info('第几个公司:%s, name:%s'%(num_first, company_name))
                    # if company_name == '量知数据':
                    #     flag = 1
                    _id = name_c['_id']
                    search_keys = name_c['search_keys']
                    last_crawl_time = name_c['last_crawl_time']
                    last_crawl_num = name_c['last_crawl_num']
                    crawl_frequency = name_c['crawl_frequency']
                    last_publish_time = name_c['last_publish_time']
                    last_crawl_title = name_c['last_crawl_title']
                    if last_publish_time:
                        start_date = last_publish_time

                    # 判断是否采集，通过日期
                    # flag = self.judge_date(last_crawl_time, crawl_frequency, company_name)

                    # if flag == 1:
                    self.save_each_company = 0
                    for name in search_keys:
                        flag_company_name_in_content = 0
                        # for num in range(start_page, total_page+1):
                        #     # time_judge1 = datetime.datetime.now()
                        #     if (num != 1) and (flag_company_name_in_content != 1):
                        #         self.logger.info('第一页没有出现相关新闻，跳过新闻列表页:%s' % num)
                        #         continue


                        # 这里调用ip代理
                        url = "https://weixin.sogou.com/weixinwap?ie=utf8&s_from=input&type=1&t=1594880241&pg=webSearchList&_sug_=y&_sug_type_=&query=" + name + "&page=1"
                        self.logger.info('list_page_url:%s' % url)
                        resp = self.downloader.crawl_data(url, None, self.headers, "get", None, None, None)
                        if resp:
                            resp.encoding = "utf8"
                            content = resp.text
                            if '暂无与' in content:
                                self.logger.info('没有这个公众号')
                                time.sleep(1)
                                continue
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
                                        # try:
                                            tag_dl_s = each_li.find_all('dl')
                                            for tag_dl in tag_dl_s:
                                                if tag_dl and '最近文章：' in str(tag_dl):
                                                    url = tag_dl.find('a')['href']
                                                    tag_time = tag_dl.find('span')
                                                    publish_time_js = tag_time.get_text().strip()
                                                    title = tag_dl.find('a').get_text().strip().replace(publish_time_js, '')
                                                    if publish_time_js:
                                                        publish_time_j = re.findall(r"timeConvert\('(.+?)'\)",str(publish_time_js))
                                                        if publish_time_j:
                                                            # 时间戳转换成时间
                                                            print(publish_time_j)
                                                            timeArray = time.localtime(int(publish_time_j[0]))
                                                            publish_time = time.strftime("%Y-%m-%d", timeArray)
                                                            self.logger.info('找到publish_time: %s' % publish_time)
                                                        else:
                                                            continue
                                                    else:
                                                        continue

                                                    print(publish_time)
                                                    # if ('天前' in publish_time):
                                                    #     day = publish_time[0]
                                                    #     now = datetime.datetime.now()
                                                    #     delta = datetime.timedelta(days=int(day))
                                                    #     publish_time = str(now - delta)[:10]
                                                    # if ('小时' in publish_time):
                                                    #     hours = publish_time[0]
                                                    #     now = datetime.datetime.now()
                                                    #     delta = datetime.timedelta(hours=int(hours))
                                                    #     publish_time = str(now - delta)[:10]
                                                    # if ('分钟' in publish_time):
                                                    #     hours = 1
                                                    #     now = datetime.datetime.now()
                                                    #     delta = datetime.timedelta(hours=int(hours))
                                                    #     publish_time = str(now - delta)[:10]
                                                    # judge_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
                                                    # judge_time = str(publish_time)
                                                    # judge_time = datetime.datetime.strptime(judge_time, "%Y-%m-%d")
                                                    # print("2017-11-02大于2017-01-04：", judge_time > end_date)

                                                    # t = each_li.find('h4')
                                                    # title = t.get_text().strip()
                                                    if title != last_crawl_title:
                                                        flag_time = 1
                                                    else:
                                                        flag_time = 0
                                                        self.logger.info("此微信公众号的最新文章未更新，已采集过")


                                                    # if judge_time >= judge_start_date:
                                                    #     flag_time = 1
                                                    # else:
                                                    #     flag_time = 0
                                                    #     self.logger.info("过期数据, pass")

                                                    if flag_time == 1:
                                                        time.sleep(0.5)
                                                        # 日期规范部分
                                                        publish_time_tag = datetime.datetime.strptime(publish_time,
                                                                                                      "%Y-%m-%d")
                                                        publish_time_tag2 = publish_time_tag.strftime('%Y-%m-%d')
                                                        self.logger.info("pulish_time:%s" % publish_time_tag2)
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
                                                        # print(self.headers2['Cookie'])

                                                        # r2 = requests.get(url2, headers=headers)
                                                        # print(r2.text)

                                                        # print("title:%s" % t.get_text().strip())
                                                        if '聘' not in title:
                                                            self.headers2["Host"] = "weixin.sogou.com"
                                                            cur_url = "https://weixin.sogou.com" + url
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
                                                                # print(url_pin)

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

                                                                        # if (name in record['content']) or (name in record['title']):
                                                                        #     flag_company_name_in_content = 1
                                                                        #     self.logger.info('此新闻是公司新闻相关的新闻')
                                                                        self.save_record(record, _id)
                                                                        # else:
                                                                        #     self.logger.info('非此新闻是公司新闻相关的新闻')


                                                        print('save:', self.save)
                                                else:
                                                    self.logger.info('没有tag_time, 跳过')
                                        # except Exception as e_li:
                                        #     self.logger.info('Error e_li:%s' % e_li)
                            else:
                                self.logger.info('没有找到li')
                        else:
                            self.logger.info('页面未响应, 公司搜索未响应, 跳过')

                        # time_judge2 = datetime.datetime.now()

                    self.update_mongo(_id, self.mongo_col2)
                    self.logger.info('save:%s' % str(self.save))
                    # else:
                    #     continue

                    try:
                        if publish_time_list:
                            while '' in publish_time_list:
                                publish_time_list.remove('')
                            # print(publish_time_list)
                            if publish_time_list:
                                self.update_mongo_publish_date_sort_function(_id, self.mongo_col2, publish_time_list)
                        # self.update_mongo_publish_date(_id, self.mongo_col2, pulish_time_list)
                    except Exception as update_error:
                        self.logger.info('Update Error:%s' % update_error)

                self.logger.info("Finish Run, time.sleep(600), time.sleep(5秒)")
                time.sleep(5)
            self.logger.info("Finish Run, time.sleep(3600), time.sleep(一小时)")
            time.sleep(43200)
            
        # return self.save


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
        record["channel"] = '公众号'
        record["language"] = 'zh'
        record["search_key"] = name
        record["tag"] = []
        record["url"] = detail_url
        img_url_list = []
        content = ''

        # try:

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

        # except:
        #     self.logger.error("详情页出错:%s%s" % (traceback.format_exc(), detail_url))

        return record


def run(db, coll_name, last_run_time, logger, *args):
    SAVE_MONGO_CONFIG2["db"] = db
    bp = ListDetailSpider(SAVE_MONGO_CONFIG2, logger=logger, coll_name=coll_name)
    last_run_time = last_run_time.split(" ")[0]  # e.g.  2019-10-19
    # industry = "lz_data_gather_ai"  # 临时修改
    bp.start_run("", start_date=last_run_time, max_page=10)
    # return count


if __name__ == '__main__':
    db = "raw_res_doc"
    coll_name = "news"
    last_run_time = "2020-01-01"
    logger = None
    args = []
    run(db, coll_name, last_run_time, logger, *args)
    # bp = ListDetailSpider(SAVE_MONGO_CONFIG)
    # bp.start_run(start_page=1, max_page=-1)