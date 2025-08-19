import json
import threading
import pandas as pd
import requests
from lxml import etree
from requests.exceptions import RequestException
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3
from services.db_service import MySQLProxyClient
import os
from services.requesterror_process import setup_spider_logger
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = setup_spider_logger()





class ProxyIP:
    """代理IP爬取和验证类"""

    def __init__(self):
        self.url = 'https://proxy.scdn.io/get_proxies.php'
        self.ip_page = 3  # 爬取3页代理
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
        }
        self.results = []  # 存储有效代理

    def check_proxy(self, ip, port, http, timeout=10):
        """验证代理是否可用"""
        test_url = 'http://httpbin.org/get'
        proxies = {
            "http": f"{http}://{ip}:{port}",
            "https": f"{http}://{ip}:{port}"
        }
        try:
            response = requests.get(
                test_url,
                proxies=proxies,
                timeout=timeout,
                verify=False
            )
            if response.status_code == 200:
                logger.info(f"代理可用：{proxies}")
                return True
            else:
                logger.warning(f"代理状态码异常：{response.status_code}，{proxies}")
                return False
        except RequestException as e:
            logger.warning(f"代理不可用：{str(e)}，{proxies}")
            return False

    def get_ip_port(self):
        """爬取并验证代理（并行验证提升效率）"""
        all_proxies = []  # 临时存储所有爬取到的代理

        # 1. 爬取代理列表
        for page in range(self.ip_page):
            try:
                params = {
                    'protocol': ' ',
                    'country': ' ',
                    'per_page': 10,
                    'page': page
                }
                res = requests.get(
                    self.url,
                    params=params,
                    headers=self.headers,
                    verify=False,
                    timeout=15
                )
                res.raise_for_status()  # 抛出HTTP错误状态码
                ip_infos = json.loads(res.text)
                proxies_html = ip_infos.get('table_html', '')
                if not proxies_html:
                    logger.warning(f"第{page}页未获取到代理HTML")
                    continue

                # 解析HTML提取代理信息
                html = etree.HTML(proxies_html)
                ips = html.xpath('//tr/td[1]/text()')
                ports = html.xpath('//tr/td[2]/text()')
                protocols = html.xpath('//tr/td[3]/span/text()')
                sources = html.xpath('//tr/td[4]/text()')

                # 收集代理信息
                for ip, port, http, source in zip(ips, ports, protocols, sources):
                    all_proxies.append((ip.strip(), port.strip(), http.strip(), source.strip()))

            except Exception as e:
                logger.error(f"爬取第{page}页代理失败：{str(e)}")
                continue

        # 2. 并行验证代理（最多5个线程）
        if all_proxies:
            with ThreadPoolExecutor(max_workers=5) as executor:
                # 提交所有验证任务
                futures = {
                    executor.submit(self.check_proxy, ip, port, http): (ip, port, http, source)
                    for ip, port, http, source in all_proxies
                }
                # 获取验证结果
                for future in as_completed(futures):
                    ip, port, http, source = futures[future]
                    try:
                        if future.result():  # 验证通过
                            self.results.append({
                                "ip": ip,
                                "port": port,
                                "http": http,
                                "source": source
                            })
                    except Exception as e:
                        logger.error(f"验证代理{ip}:{port}时出错：{str(e)}")

        logger.info(f"共获取有效代理{len(self.results)}个")


class IP_tojson(ProxyIP):
    """代理IP处理并存储到JSON和MySQL"""

    def __init__(self, mysql_config):
        super().__init__()
        self.mysql_client = MySQLProxyClient(mysql_config)

    def tojson(self, save_path='proxy_ips/proxy_ips.json'):
        """生成代理JSON文件并保存到MySQL"""
        self.get_ip_port()  # 同步执行，确保获取完成

        # 保存到MySQL
        if self.results:
            self.mysql_client.batch_insert(self.results)

        # 同时保存到JSON作为备份
        if self.results:
            df = pd.DataFrame(self.results)
            # 确保保存目录存在
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            df.to_json(save_path, orient='records', force_ascii=False)
            logger.info(f"代理已保存至{save_path}，共{len(self.results)}条")
        else:
            logger.warning("未获取到有效代理，不生成JSON文件")

    def close(self):
        """关闭数据库连接"""
        self.mysql_client.close()


class ProxyPool:
    """代理池管理类，支持从MySQL读取和更新代理"""

    def __init__(self, mysql_config):
        self.lock = threading.Lock()
        self.db_client = MySQLProxyClient(mysql_config)
        self.checker = ProxyIP()
        self.proxies = self._get_valid_proxies()
        self.current_index = 0

    def _get_valid_proxies(self):
        """从数据库获取有效代理"""
        proxies = self.db_client.get_valid_proxies()
        logger.info(f"从数据库加载有效代理数量: {len(proxies)}")
        return proxies

    def refresh_proxies(self):
        """刷新代理池（重新爬取并更新数据库）"""
        logger.info("开始刷新代理池...")
        try:
            ip_crawler = IP_tojson(self.db_client.config)
            ip_crawler.tojson()
            ip_crawler.close()
            self.proxies = self._get_valid_proxies()
            return len(self.proxies) > 0
        except Exception as e:
            logger.error(f"刷新代理池失败: {str(e)}")
            return False

    def get_next_proxy(self):
        """轮换获取下一个可用代理"""
        with self.lock:
            if not self.proxies:
                return None
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
            return proxy

    def remove_proxy(self, ip, port):
        """标记代理为无效并从代理池移除"""
        with self.lock:
            # 1. 数据库中标记为无效
            self.db_client.mark_invalid(ip, port)

            # 2. 从内存代理池中移除
            for i, proxy in enumerate(self.proxies):
                if proxy['ip'] == ip and proxy['port'] == port:
                    self.proxies.pop(i)
                    self.current_index = self.current_index % max(1, len(self.proxies))
                    logger.warning(f"移除失败代理 {ip}:{port}，剩余代理数: {len(self.proxies)}")
                    self._update_proxy_json()  # 新增：更新存储
                    break

    def _update_proxy_json(self):
        """将当前内存中的有效代理写回JSON文件"""
        proxy_json_path = "proxy_ips/proxy_ips.json"  # JSON文件路径
        try:
            with open(proxy_json_path, 'w', encoding='utf-8') as f:
                json.dump(self.proxies, f, ensure_ascii=False, indent=2)
            logger.info(f"代理JSON文件已更新，保留 {len(self.proxies)} 个有效代理")
        except Exception as e:
            logger.error(f"更新代理JSON文件失败: {str(e)}")

    def is_empty(self):
        return len(self.proxies) == 0

    def close(self):
        """关闭数据库连接"""
        self.db_client.close()
