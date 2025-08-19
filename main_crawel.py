import json
import time
from pathlib import Path

from urllib3.util.retry import Retry
from urllib.parse import urljoin
from services.text_processing import clean_text_newlines,parse_with_dateutil,extract_source,html_to_clean_text,is_today
from services.requesterror_process import setup_spider_logger
from services.Write_csv import WriteCSV
import requests
from requests.adapters import HTTPAdapter
import ssl
import urllib3
from services.db_service import RedisClient,MySQLClient
from lxml import etree

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# 初始化日志记录器，指定日志文件
logger = setup_spider_logger()
class CustomHTTPAdapter(HTTPAdapter):
    """自定义HTTP适配器，处理SSL问题"""

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)



class DataCrawler(WriteCSV):  # 继承WriteCSV以使用写入方法
    """主爬虫类，负责列表页爬取和数据整合"""

    def __init__(self,channelname,url,main_url,id,start_page,final_page,content_xpath,source_xpath,ip,port,http):
        super().__init__(channelname=channelname)  # 初始化父类
        self.channelname = channelname
        self.url=url#接收主页面url
        self.main_url=main_url#接收抓包得到的接口url
        self.id=id
        self.start_page = start_page
        self.final_page = final_page
        self.content_xpath = content_xpath
        self.source_xpath = source_xpath
        self.batch_size = 100  # 缩小批次大小，避免内存占用过高
        self.batch_items = []  # 存储待写入的批次数据（列表嵌套列表）
        self.stats = {"items_processed": 0, "batches_written": 0, "errors": 0}
        self.detail_fetcher = Detail_Requests(crawler=self)  # 实例化详情页处理器
        self.session = requests.Session()
        self.ip=ip
        self.port=port
        self.http=http
        db_config_path=Path(__file__).parent / "config" / "db_config.json"
        with open(db_config_path,"r",encoding="utf-8") as f:
            db_config = json.load(f)
        self.redis_client=RedisClient(db_config['redis'])
        self.mysql_client=MySQLClient(db_config['mysql'])
        retry_strategy = Retry(
            total=3,  # 总重试次数
            backoff_factor=1,  # 重试间隔（1s, 2s, 4s...）
            status_forcelist=[429, 500, 502, 503, 504]  # 需要重试的状态码
        )
        proxies = {
            "http": f"{self.http}://{self.ip}:{self.port}",
            "https": f"{self.http}://{self.ip}:{self.port}"
        }

        self.session.proxies=proxies
        adapter = CustomHTTPAdapter(max_retries=retry_strategy)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        self.session.headers= {
                "accept": "application/json",
                "accept-encoding": "gzip, deflate, br, zstd",
                "accept-language": "zh-CN,zh;q=0.9",
                "cache-control": "no-cache",
                "client-type": "3",
                "connection": "keep-alive",
                "content-type": "application/json",
                "device-imei": "share",
                "host": "api.dzplus.dzng.com",
                "origin": "https://dz.dzng.com",
                "pragma": "no-cache",
                "referer": "https://dz.dzng.com/",
                "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Google Chrome\";v=\"132\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"macOS\"",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-site",
                "token": "",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
                "version": "share"
            }

    #列表页数据请求
    def fetch_page(self, page):
        """获取指定页码的列表页数据"""
        try:
            timestamp = int(time.time() * 1000)
            url = f"{self.main_url}?timestamp={timestamp}"
            logger.info(f"正在爬取第{page}页")

            request_data = {
                "id": self.id,
                "newsType": "",
                "pageNo": page,
                "pageSize": 20,
                "showType": "",
                "tab": "index",
                "type": "channel"
            }

            # 创建会话并设置自定义适配器（处理SSL）


            response = self.session.post(
                url,
                data=json.dumps(request_data),
                timeout=30,
                verify=False
            )

            if response.status_code == 200:
                return response.text
            else:
                logger.error(f"页面 {page} 请求失败，状态码: {response.status_code}")
                self.stats["errors"] += 1
                return None

        except Exception as e:
            logger.error(f"页面 {page} 请求出错: {e}")
            self.stats["errors"] += 1
            return None
    #解析列表页数据
    def parse_get_data(self, json_str):
        """解析列表页数据，并调用详情页接口补充正文和来源"""
        try:
            data = json.loads(json_str)
            # 提取列表数据（根据实际接口返回结构调整）
            items = data.get('data', [{}])

            for item in items:
                is_processed = False
                datas = item.get('data', {})
                if datas and not is_processed:
                    title = datas.get('title', '')
                    publish_time = parse_with_dateutil(datas.get('publishTime', ''))
                    if not is_today(publish_time):
                        logger.info(f"跳过非今日数据: {title}")
                        continue
                    related_id = datas.get('newsId', '')  # 用于详情页爬取的ID
                    # 2. 调用详情页处理器获取正文和来源
                    content = ""
                    source = ""
                    if related_id:
                        # 直接用related_id构建详情页URL（优化：无需传入整个JSON）
                        detail_url = self.detail_fetcher.get_detail_page_url(related_id)
                        if self.redis_client.is_duplicate(detail_url):
                            logger.info(f"已爬取数据，跳过: {detail_url}")
                            continue

                        if detail_url:
                            print(f"正在采集详情页: {detail_url}")
                            detail_data = self.detail_fetcher.fetch_detail_page_data(detail_url)
                            content = clean_text_newlines(detail_data.get('content', '')) if detail_data else ''
                            if content is not None and content.strip() != "":
                                print(f"成功获取详情页: {detail_url}")
                                logger.info(f"成功获取详情页: {detail_url}")
                            elif content is None and content == "" and datas.get('newsType') == "VIDEO":
                                content = html_to_clean_text(datas.get('summary'))

                            else:
                                logger.warning(f"详情页内容为空或未获取到: {detail_url}")
                                continue
                            source = detail_data.get('source', '') if detail_data else datas.get('headName', '')
                            source = extract_source(source)
                            time.sleep(0.1)  # 控制详情页请求频率，避免反爬
                        else:
                            logger.warning(f"跳过详情页（related_id无效）: {related_id}")
                        is_processed = True
                    else:
                        logger.warning(f"{title}无related_id，跳过详情页爬取")
                        continue
                    if is_processed:
                        self.redis_client.mark_crawled(detail_url)
                        self.batch_items.append({
                            '频道': self.channelname,
                            '标题': title,
                            '文号': '',
                            '有效性': '',
                            '链接': detail_url,
                            '发布时间': publish_time.strftime("%Y-%m-%d %H:%M:%S") if publish_time else '',
                            '信息来源': source,
                            '作者': '',
                            '正文': content
                        })
                        self.stats["items_processed"] += 1

                datalists=item.get('datalists', [])
                if datalists and not is_processed:
                    for datalist in datalists:
                        # 1. 提取列表页基础信息
                        title = datalist.get('title', '')
                        publish_time = parse_with_dateutil(datalist.get('startTime', ''))
                        if not is_today(publish_time):
                            logger.info(f"跳过非今日数据: {title}")
                            continue
                        related_id = datalist.get('relatedId', '')  # 用于详情页爬取的ID
                        # 2. 调用详情页处理器获取正文和来源
                        content = ""
                        source = ""
                        if related_id:
                            # 直接用related_id构建详情页URL（优化：无需传入整个JSON）
                            detail_url = self.detail_fetcher.get_detail_page_url(related_id)
                            if self.redis_client.is_duplicate(detail_url):
                                logger.info(f"已爬取数据，跳过: {detail_url}")
                                continue
                            if detail_url:
                                print(f"正在采集详情页: {detail_url}")
                                detail_data = self.detail_fetcher.fetch_detail_page_data(detail_url)
                                content = clean_text_newlines(detail_data.get('content', '')) if detail_data else ''
                                if content is not None and content.strip() != "":
                                    print(f"成功获取详情页: {detail_url}")
                                    logger.info(f"成功获取详情页: {detail_url}")
                                elif content is None and content == "" and datas.get('newsType') == "VIDEO":
                                    content = html_to_clean_text(datas.get('summary'))
                                source = detail_data.get('source', '') if detail_data else ''
                                source = extract_source(source)
                                time.sleep(0.1)  # 控制详情页请求频率，避免反爬
                                is_processed = True
                            else:
                                if datalist.get('summary') and datalist.get('newsType') == 'VIDEO':
                                    content=html_to_clean_text(datalist.get('summary', '')) if datalist else ''
                                print(f"跳过详情页（related_id无效）: {related_id}")
                                logger.warning(f"跳过详情页（related_id无效）: {related_id}")
                        else:
                            print("无related_id，跳过详情页爬取")
                            logger.warning(f"{title}无related_id，跳过详情页爬取")
                        if is_processed:
                            self.redis_client.mark_crawled(detail_url)
                            self.batch_items.append({
                                '频道': self.channelname,
                                '标题': title,
                                '文号': '',
                                '有效性': '',
                                '链接': detail_url,
                                '发布时间': publish_time.strftime("%Y-%m-%d %H:%M:%S") if publish_time else '',
                                '信息来源': source,
                                '作者': '',
                                '正文': content
                            })
                            self.stats["items_processed"] += 1


            return self.batch_items

        except json.JSONDecodeError:
            logger.warning(f"无法解析JSON响应")
            self.stats["errors"] += 1
        except Exception as e:
            logger.warning(f"解析数据出错: {e}")
            self.stats["errors"] += 1

#详情页处理类
class Detail_Requests():
    """详情页处理类，负责构建详情页URL和提取内容"""
    def __init__(self,crawler):
        self.crawler = crawler

    def get_detail_page_url(self, news_id):
        """根据列表页item中的relatedId构建详情页URL"""
        actual_url=urljoin(self.crawler.url,news_id)
        return actual_url

    def fetch_detail_page_data(self, url):
        """爬取详情页并提取正文和来源（需补充XPath表达式）"""
        try:

            # 使用自定义适配器处理SSL

            response = self.crawler.session.get(url,  timeout=15, verify=False)
            response.raise_for_status()  # 抛出HTTP错误

            # 解析HTML（关键：根据实际网页结构修改XPath）
            html = etree.HTML(response.text)

            # ==== 需根据目标网站实际结构修改以下XPath ====
            # 示例：假设正文在class为"article-content"的div下的p标签中
            contents = html.xpath(self.crawler.content_xpath)

            source_infos = html.xpath(self.crawler.source_xpath)
            # ==========================================
            content='\n'.join(str(content) for content in contents)
            source='\n'.join(str(source_info) for source_info in source_infos)


            return {"content": content, "source": source}

        except requests.exceptions.RequestException as e:
            logger.error(f"详情页请求错误 {url}: {e}")

            return {"content": "", "source": ""}  # 错误时返回空内容
        except Exception as e:
            logger.error(f"详情页解析错误 {url}: {e}")
            return {"content": "", "source": ""}


