import random
from concurrent.futures import ThreadPoolExecutor

import requests
import logging
import time
import json
import re
from config import REDIS_CONFIG, SIGN_CONFIG, CRAWL_CONFIG,MYSQL_CONFIG
from utils.proxy_pool import ProxyPool
from storage.db_handler import DBHandler
from storage.csv_handler import CSVHandler
from utils.tb_utils import get_product_data,get_detail_comment ,extract_text,encrypt_with_js
from urllib.parse import quote
logger = logging.getLogger('TaskProcessor')


class TaskProcessor:
    def __init__(self):
        self.proxy_pool = ProxyPool(MYSQL_CONFIG)
        self.db_handler = DBHandler()
        self.csv_handler = CSVHandler()
        self.headers = CRAWL_CONFIG
        self.detail_page=CRAWL_CONFIG["detail_page"]
        self.proxy_list=[]
        self.token = self._extract_token()
        self.thread_num=CRAWL_CONFIG["thread_num"]
    def fetch_page(self, url):
        """发起请求（替代Scrapy的下载器）"""
        # if self.proxy_pool.check_proxy_num():
        #     proxy_info=self.proxy_pool.get_next_proxy()
        #     print(proxy_info)
        #     scheme = (proxy_info[2] or 'http').lower()  # 用scheme更准确，避免与http协议名混淆
        #
        #     # 2. 提取IP和端口（确保键名与数据一致）
        #     try:
        #         ip = proxy_info[0]
        #         port = proxy_info[1]
        #     except KeyError as e:
        #         logger.error(f"代理数据缺少必要字段：{e}，跳过该代理")
        #
        #
        #     # 3. 构建代理URL（先拼接字符串，再赋值给字典）
        #     proxy_url = f"{scheme}://{ip}:{port}"  # 先定义代理URL
        #
        #     # 4. 构建requests要求的proxies字典（键必须是协议名，值是代理URL）
        #     proxies_dict = {
        #         "http": proxy_url,  # http协议统一用该代理
        #         "https": proxy_url  # https协议也用该代理（通常代理同时支持两种协议）
        #     }
        for _ in range(CRAWL_CONFIG["max_retry"]):
            try:
                # print('准备进入get请求的url：',url)
                # print('headers:',self.headers,type(self.headers))
                resp = requests.get(
                    url,
                    headers=self.headers['headers'],
                    # proxies=proxies_dict,
                    timeout=CRAWL_CONFIG["timeout"],
                    verify=False
                )
                if resp.status_code == 200:
                    return resp.content.decode('utf-8')

                logger.warning(f"状态码异常：{resp.status_code}，URL：{url[:100]}")
            except Exception as e:
                logger.error(f"请求失败：{str(e)}")
                # if proxies_dict:
                #     self.proxy_pool.remove_proxy(ip,port)
                #     proxies=self.proxy_pool.get_next_proxy()
                #
                #     scheme = (proxies.get('http') or 'http').lower()
                #     ip = proxies['ip']
                #     port = proxies['port']
                #     proxy_url = f"{scheme}://{ip}:{port}"
                #     proxies_dict = {
                #         "http": proxy_url,  # http协议统一用该代理
                #         "https": proxy_url  # https协议也用该代理（通常代理同时支持两种协议）
                #     }
                time.sleep(1)
                continue


        logger.info(f"加载了 {len(self.proxy_list)} 个代理IP")
        print('fetch_page函数接受的url:', url)
    def fetch_detail_page(self, url):
        """发起请求（替代Scrapy的下载器）"""
        # if self.proxy_pool.check_proxy_num():
        #     proxy_info=self.proxy_pool.get_next_proxy()
        #     print(proxy_info)
        #     scheme = (proxy_info[2] or 'http').lower()  # 用scheme更准确，避免与http协议名混淆
        #
        #     # 2. 提取IP和端口（确保键名与数据一致）
        #     try:
        #         ip = proxy_info[0]
        #         port = proxy_info[1]
        #     except KeyError as e:
        #         logger.error(f"代理数据缺少必要字段：{e}，跳过该代理")
        #
        #
        #     # 3. 构建代理URL（先拼接字符串，再赋值给字典）
        #     proxy_url = f"{scheme}://{ip}:{port}"  # 先定义代理URL
        #
        #     # 4. 构建requests要求的proxies字典（键必须是协议名，值是代理URL）
        #     proxies_dict = {
        #         "http": proxy_url,  # http协议统一用该代理
        #         "https": proxy_url  # https协议也用该代理（通常代理同时支持两种协议）
        #     }
        for _ in range(CRAWL_CONFIG["max_retry"]):
            try:
                time.sleep(random.uniform(0, 2))
                # print('准备进入get请求的url：',url)
                # print('headers:',self.headers,type(self.headers))
                resp = requests.get(
                    url,
                    headers=self.headers['detail_headers'],
                    # proxies=proxies_dict,
                    timeout=CRAWL_CONFIG["timeout"],
                    verify=False
                )
                if resp.status_code == 200:

                    return resp.content.decode('utf-8')

                logger.warning(f"状态码异常：{resp.status_code}，URL：{url[:100]}")
            except Exception as e:
                logger.error(f"请求失败：{str(e)}")
                # if proxies_dict:
                #     self.proxy_pool.remove_proxy(ip,port)
                #     proxies=self.proxy_pool.get_next_proxy()
                #
                #     scheme = (proxies.get('http') or 'http').lower()
                #     ip = proxies['ip']
                #     port = proxies['port']
                #     proxy_url = f"{scheme}://{ip}:{port}"
                #     proxies_dict = {
                #         "http": proxy_url,  # http协议统一用该代理
                #         "https": proxy_url  # https协议也用该代理（通常代理同时支持两种协议）
                #     }
                time.sleep(1)
                continue


        logger.info(f"加载了 {len(self.proxy_list)} 个代理IP")
        print('fetch_page函数接受的url:', url)
    def thread_detail(self,url,product_id):
        """<UNK>"""
        html = self.fetch_detail_page(url)
        detail_comments=get_detail_comment(html,product_id)
        if detail_comments:
            for item in detail_comments:
                self.db_handler.save_comment(item)




    def parse_products(self, html):
        """解析商品数据（复用你的解析逻辑）"""
        products = []
        try:
            result = get_product_data(html)
            for product in result:
                product_item = {
                    'product_id': product.get('product_id'),
                    'title': extract_text(product.get('title')),
                    'price': product.get('price'),
                    'product_source': product.get('product_source', ''),
                    'product_situation': product.get('product_situation', ''),
                    'crawl_time': time.strftime('%Y-%m-%d %H:%M:%S')
                }
                products.append(product_item)
        except Exception as e:
            logger.error(f"解析商品失败：{str(e)}")
        return products





    def process_page(self, url, page):
        """处理单个URL任务"""
        logger.info(f"处理第{page}页：{url[-100:]}")
        html = self.fetch_page(url)
        if not html:
            logger.error(f"获取页面失败：{url[-100:]}")
            return

        products = self.parse_products(html)
        if products:
            logger.info(f"第{page}页解析到{len(products)}个商品")
            for item in products:
                self.db_handler.save_product(item)
                self.csv_handler.save_product(item)
                product_id=item['product_id']
                data={"showTrueCount":'false',"auctionNumId":"760517023196","pageNo":1,"pageSize":20,"rateType":"","searchImpr":"-8","orderType":"","expression":"","rateSrc":"pc_rate_list"}
                data['showTrueCount'] = False
                data['auctionNumId'] = product_id
                # with ThreadPoolExecutor(max_workers=self.thread_num) as executor:
                #     futures = []
                for detail_page in range(self.detail_page+1):
                    json_str = data
                    print(json_str, type(json_str))
                    json_str['pageNo'] = detail_page
                    # json_str['showTrueCount'] = json_str['showTrueCount'].replace('__FALSE__', 'false')
                    print(json_str, type(json_str))
                    json_new = json.dumps(json_str, separators=(',', ':'))
                    print('json_new:',json_new, type(json_new))
                    print(self.token)
                    timestamp = int(time.time() * 1000)
                    sign = encrypt_with_js(
                        token=self.token,
                        timestamp=timestamp,
                        appkey=SIGN_CONFIG["appkey"],
                        data_dict=json_new,
                        js_path=SIGN_CONFIG["js_path"]
                    )
                    if not sign:
                        logger.error(f"第{page}页sign生成失败")
                        continue
                    detail_url = 'https://h5api.m.tmall.com/h5/mtop.taobao.rate.detaillist.get/6.0/'
                    url = f'{detail_url}?jsv=2.7.4&appKey=12574478&t={timestamp}&sign={sign}&api=mtop.taobao.rate.detaillist.get&v=6.0&isSec=0&ecode=1&timeout=20000&jsonpIncPrefix=pcdetail&type=jsonp&dataType=jsonp&callback=mtopjsonppcdetail4&data={quote(json_new)}'
                    print('构建的url:',url)
                    try:
                        self.thread_detail(url,product_id)
                    except Exception as e:
                        print('详情页请求出现问题:',e)

                    # future = executor.submit(self.thread_detail, url, product_id)
                    # futures.append(future)
                    # for future in futures:
                    #     try:
                    #         future.result()  # 可以设置超时时间 阻塞主线程直至目前线程完成并且又返回值
                    #     except Exception as e:
                    #         logger.error(f"详情页处理失败: {str(e)}")




        else:
            logger.warning(f"第{page}页未解析到商品")

    def _extract_token(self):
        try:
            print(self.headers.get("detail_headers").get('cookie'))
            token_match = re.findall(
                f'{SIGN_CONFIG["token_cookie_key"]}=(.*?)_',
                self.headers.get("detail_headers").get('cookie')
            )
            if not token_match:
                raise Exception("Cookie中未找到token")
            return token_match[0]
        except Exception as e:
            logger.error(f"提取token失败：{str(e)}")
            raise


