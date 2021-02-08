# coding=utf8
"""
项目数据格式声明
"""

import datetime
import time

class RecordModel(object):
    """
    数据格式的基类
    """
    save_coll = ""
    # 常见的通用字段
    common_formatted = {
        "crawl_time": None,
        "source": "",
        "search_key": "",
        "tag": "",
        "url": "",
        "meta": None,
        "html": "",
    }
    # 该格式数据的业务字段
    special_formatted = {
    }
    pk = ["_id"]
    check_keys = []
    show_id = None
    date_keys = []  # 时间字段说明，在此列表中的时间字段都会在保存前进行检测，该时间字符串格式是否符合预期规范
    date_format = "%Y-%m-%d"  # 用于检测时间字段是否规范时将时间字段转为datetime对象的格式化字符串
    field_type_dict = {}  # 对于某些特定字段特殊格式的声明，在此声明的字段格式会在存储前检测实际值是否符合声明的格式

    def __init__(self, mongo, logger):
        self.mongo = mongo
        self.record = None
        self.logger = logger
        self.temp = {}
        self.init()

    def init(self):
        self.temp = self.common_formatted
        self.temp.update(self.special_formatted)
        if not self.show_id:
            self.show_id = self.pk[0]

    def exists(self, data, q=None):
        if data:
            record = data
        else:
            record = self.record
        update_q = {}
        if not q and self.pk:
            for k in self.pk:
                update_q[k] = record[k]
        elif q:
            update_q = q
        c = self.mongo.count(update_q)
        if c > 0:
            return True
        else:
            return False

    def save(self, data=None, update=False):
        if data:
            record = data
        else:
            record = self.record
        valid = self.check_record_format(record)
        if not valid:
            return False
        is_exist = False
        update_q = {}
        if self.pk:
            for k in self.pk:
                update_q[k] = record[k]
                # print(update_q)
            c = self.mongo.find(update_q).count()
            if c > 0:
                is_exist = True
        if is_exist and update:
            ret = self.mongo.update_one(update_q, {"$set": record})
            self.logger.info("Save record(%s) upsert_id=%s " % (record[self.show_id], str(ret.upserted_id)))
            return ret
        elif not is_exist:
            ret = self.mongo.insert(record)
            self.logger.info("Save record(%s) insert_id=%s " % (record[self.show_id], str(ret)))
            return ret
        elif is_exist and not update:
            self.logger.info("Ignore record(%s)" % record[self.show_id])

    def get_records(self, q, lazy=True):
        records = self.mongo.find(q)
        result = []
        if not lazy:
            for r in records:
                # r.pop("_id")
                result.append(r)
        else:
            result = records
        return result

    def check_record_format(self, record):
        """
        对于传入的数据进行格式检测，保证存入数据库的数据是预期的格式
        1.检测是否和预定期的模版字段完全一致，没有少字段和多字段
        2.检测指定的日期字段格式是否符合规范
        3.检测多值字段的分隔符是否合法,默认分隔符为;  # TODO 暂未实现该类型字段的检测功能
        4.检测指定字段是否有值，不能为空
        5.检测指定字段的实际值的类型
        :return: 默认为True表示通过检测，发现不符合预定义的字段时为False表示不通过检测
        """
        ret = True  # 表示检测结果
        keys = record.keys()
        temp_keys = self.temp.keys()
        for key in keys:
            # 检测record是否存在多余字段
            try:
                v = self.temp[key]
            except:
                self.logger.error("Record Field(%s) not in Model Temp" % key)
                ret = False

        for t_key in temp_keys:
            # 检测record是否缺少预定义的字段
            try:
                v = record[t_key]
                if not v and t_key in self.check_keys:
                    # 检测非空字段是否为空或者None
                    self.logger.warning("Field(%s):value is required and can't be empty" % t_key)
                    ret = False
                if v and t_key in self.date_keys:
                    try:
                        # 检测日期格式字段的日期格式是否和预定义的date_format匹配
                        date_obj = datetime.datetime.strptime(v, self.date_format)
                    except:
                        self.logger.warning(
                            "Field(%s): value=%s not match date format(%s)" % (t_key, v, self.date_format))
                        ret = False
                # 检测指定字段实际值的数据类型与预定义的类型一致
                field_type = self.field_type_dict.get(t_key, None)
                if field_type and type(v) != field_type:
                    self.logger.warning("Field(%s): value type(%s) not match type(%s)" % (t_key, type(v), field_type))
                    ret = False
            except:
                self.logger.error("Model Field(%s) not in Record" % t_key)
                ret = False
        return ret


class Financing(RecordModel):
    """
    投融资数据格式
    """
    save_coll = "financing"
    special_formatted = {
        "industry": "",
        "investor": None,
        "tag": "",
        "financing_round": "",
        "price": "",
        "financing_date": "",
        "product_name": "",
        "news": "",
        "company_name": "",
        "proportion": "",
        "short_name": "",
    }

    common_formatted = {
        "url": "",
        "html": "",
        "source": "",
        "search_key": "",
        "crawl_time": None,
    }
    pk = ["url", "financing_date"]
    date_keys = ["financing_date"]
    field_type_dict = {
        "investor": list,
    }


class CompanyBasicInfo(RecordModel):
    """
    企业工商信息数据格式
    """
    save_coll = "company_basic_info"
    common_formatted = {
        "crawl_time": None,
        "source": "",
        "search_key": "",
        "tag": "",
        "url": "",
        "html": "",
    }
    special_formatted = {
        "company_name": "",
        "seed_id": "",
        "profession": "",
        "label": None,
        "used_name": "",
        "registration": "",
        "register_status": "",
        "description": "",
        "registration_id": "",
        "registered_capital": "",
        "area": "",
        "staff_size": "",
        "social_credit_code": "",
        "legal_representative": "",
        "business_term": "",
        "email": "",
        "website": "",
        "img_logo": "",
        "paid_capital": "",
        "company_type": "",
        "business_scope": "",
        "shareholder": None,
        "organization_code": "",
        "address": "",
        "name_en": "",
        "phone": "",
        "insured_number": "",
        "taxpayer_id": "",
        "leader": "",
        "establish_date": "",
        "source": "",
        "approve_date": "",
    }
    pk = ["seed_id"]
    check_keys = ["company_name", "seed_id", "url"]
    field_type_dict = {
        "html": str,
        "leader": list,
        "shareholder": list,
    }


class CompanyKey(RecordModel):
    """
    企业名单关键词表中数据格式
    """
    save_coll = "company_tags"
    common_formatted = {
    }
    # 该格式数据的业务字段
    special_formatted = {
        "company_name": "",
        "short_name": "",
        "tag": "",
        "seed_id": None,
        "is_crawled": "0",
    }
    pk = ["company_name"]
    check_keys = ["company_name"]


# 驰名商标
class FamousTrademark(RecordModel):
    save_coll = "company_famous_trademark"
    special_formatted = {
        "company_name": "",
        "address": "",
        "website": "",
        "legal_representative": "",
        "phone": "",
        "zipcode": "",
        "department": "",
        "contact": "",
        "trademark_name": "",
        "approve_date": "",
        "category": "",
        "trademark_number": "",
        "registration_date": "",
        "goods_category": "",
        "goods": "",
        "registrant": "",
        "description": "",
        "assess_organization": "",
        "email": "",
        "img_url": "",

        "search_key": "",
        "tag": "",
        "crawl_time": "",
        "url": "",
        "html": "",
        "source": "",
    }
    pk = ["trademark_number", "url"]
    check_keys = ["trademark_number", "trademark_name", "company_name"]


# 新闻信息
class News(RecordModel):
    save_coll = "news"
    special_formatted = {
        "channel": "",
        "img_url": None,
        "language": "",
        "author": "",
        "title": "",
        "content": "",
        "publish_time": "",
        "site_id": "",
        "industry": "",
    }
    common_formatted = {
        "crawl_time": None,
        "source": "",
        "search_key": "",
        "tag": None,
        "url": "",
        "html": "",
    }
    pk = ["url"]
    check_keys = ["title", "publish_time", "content"]
    date_keys = ["publish_time"]
    field_type_dict = {
        "html": str,
        "tag": list,
        "img_url": list,
    }


# 劳动仲裁
class LaborArbitration(RecordModel):
    save_coll = "labor_arbitration"
    special_formatted = {
        "court_time": "",
        "case_number": "",
        "case_type": "",
        "applicant": "",
        "respondent": "",
        "main_cause": "",
        "arbitrator": "",
        "clerk": "",
        "address": "",
        "publish_department": "",
    }
    pk = ["case_number"]
    check_keys = ["case_number", "respondent"]
    # 由于历史原因，新旧字段映射
    key_map = {

    }
    meta_map = {
        "court_time": "开庭时间",
        "case_number": "案　号",
        "case_type": "公告类型",
        "applicant": "申请人",
        "respondent": "被申请人",
        "main_cause": "主案由",
        "arbitrator": "仲裁员",
        "clerk": "书记员",
        "address": "地　点",
        "publish_department": "发布机构",
    }


# 招聘信息
class Recruit(RecordModel):
    save_coll = "recruit"
    special_formatted = {
        "recruit_name": "",
        "url": "",
        "create_time": "",
        "job_title": "",
        "job_class": "",
        "job_detail": "",
        "job_salary": "",
        "edu_require": "",
        "experience_require": "",
        "recruit_number": "",
        "skill_require": "",
        "company_name": "",
        "scale": "",
        "address": "",
    }
    pk = ["url"]
    check_keys = ["job_title", "company_name"]
    # 由于历史原因，新旧字段映射
    key_map = {

    }


# 专利信息
class Patent(RecordModel):
    save_coll = "patent"
    common_formatted = {
        "crawl_time": None,
        "source": "",
        "search_key": "",
        "url": "",
        "meta": None,
        "html": "",
    }
    special_formatted = {
        "patent_name": "",
        "patent_type": "",
        "apply_number": "",
        "apply_date": "",
        "public_id": "",
        "public_date": "",
        "ipc_category": "",
        "applicant": "",
        "inventor": "",
        "agent": "",
        "agency": "",
        "priority_id": "",
        "priority_date": "",
        "applicant_addr": "",
        "applicant_zip": "",
        "cpc_category": "",
        "abstract": "",
        "img_url": "",
        "law_status": "",
        "citation": "",
        "cognate": "",
        "content": "",
    }
    pk = ["patent_name", "public_id"]
    check_keys = ["patent_name", "url"]
    date_keys = ["public_date", "apply_date", "priority_date"]

    meta_map = {
        "apply_number": "申请号",
        "apply_date": "申请日",
        "public_id": "公开（公告）号",
        "public_date": "公开（公告）日",
        "applicant": "申请（专利权）人",
        "inventor": "发明（设计）人",
        "ipc_category": "国际专利分类号",
        "applicant_addr": "地址",
        "agency": "专利代理机构",
        "agent": "代理人",
    }  # 已过期不再使用


# 专家信息
class Expert(RecordModel):
    save_coll = "expert"
    common_formatted = {
        "crawl_time": None,
        "meta": None,
        "source": "",
        "search_key": "",
    }
    special_formatted = {
        "expert_name": "",
        "ISNI": "",
        "organization": "",
        "subject": "",
        "label": "",
    }
    pk = ["expert_name", "ISNI", "organization", "subject", "label"]
    check_keys = ["expert_name"]
    meta_map = {

    }


# 研报
class ResearchReport(RecordModel):
    save_coll = "research_report"
    special_formatted = {
        "publish_time": "",
        "industry_category": "",
        "up_down_range": "",
        "title": "",
        "rating_category": "",
        "rating_change": "",
        "institution_name": "",
        "content": "",
        "pdf_id": "",
        "file_key": "",
        "down_url": "",
    }
    pk = ["url"]
    check_keys = ["title", "url", "publish_time"]
    meta_map = {
    }


class Policy(RecordModel):
    """
    行业政策 数据格式更新于2020-3-23
    """
    save_coll = "industry_policy"
    special_formatted = {
        "sector": "",
        "location": "",
        # "policy_category": "",
        # "index": "",
        "title": "",
        # "file_no": "",
        # "summary": "",
        "publish_date": "",
        "content": "",
        "pdf_url": "",
        # "file_path": "",
        # "content_html": "",
        # "has_down": "",
    }
    common_formatted = {
        "crawl_time": None,
        "source": "",
        "search_key": "",
        "url": "",
        "html": "",
    }
    pk = ["url"]
    check_keys = ["title", "url", "publish_date"]
    field_type_dict = {
        "html": str,
    }


class CompanyProduct(RecordModel):
    """
    公司产品
    """
    save_coll = "company_product"
    special_formatted = {
        "company_name": "",
        "product_name": "",
        "introduction": "",
        "product_url": ""
    }
    common_formatted = {
        "crawl_time": None,
        "source": "",
        "search_key": "",
        "url": "",
        "html": "",
        # "meta": None,
    }
    pk = ["product_name", "company_name", ]
    check_keys = ["company_name", "product_name"]


class Conference(RecordModel):
    """
    会议记录
    """
    save_coll = "res_kb_conference"
    special_formatted = {
        "conference_name": "",
        "address": "",
        "start_time": "",
        "end_time": "",
        "conference_details": "",
        "participants": "",
        "img_url": ""
    }
    common_formatted = {
        "crawl_time": None,
        "source": "",
        "url": "",
        "html": "",
        # "meta": None,
    }
    pk = ["url"]
    check_keys = ["conference_name", "conference_details"]
    field_type_dict = {
        "img_url": list,  # 表示申明 img_url字段的值的类型是list
        "html": str,
    }
