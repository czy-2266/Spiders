import threading
import time
import json
import os
from pathlib import Path

import pandas as pd
import schedule
from concurrent.futures import ThreadPoolExecutor, as_completed
from main_crawel import DataCrawler
from services.requesterror_process import setup_spider_logger
from services.Write_csv import WriteCSV
from proxy_ips.check_ip import IP_tojson, ProxyPool
from services.Clear_Json import clear_non_empty_json
from services.db_service import MySQLClient

# ===================== 日志配置 =====================
logger = setup_spider_logger()
csv_lock = threading.Lock()
MAX_WORKERS = 5
MAX_RETRY = 3  # 单页最大重试次数

# ===================== 数据库类 =====================
def load_db_config():
    """加载数据库配置"""
    config_path = Path(__file__).parent / "config" / "db_config.json"
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

db_config = load_db_config()
mysql_client=MySQLClient(db_config['mysql'])

# ===================== 爬虫运行函数 =====================
def run(crawler, proxy_pool):
    """单个爬虫任务"""
    try:
        logger.info(f"开始爬取 {crawler.channelname}，从第{crawler.start_page}页到第{crawler.final_page}页")
        csv_writer = WriteCSV(channelname=crawler.channelname)
        results = []

        for page in range(crawler.start_page, crawler.final_page + 1):
            retry_count = 0
            while retry_count < MAX_RETRY:
                proxy = proxy_pool.get_next_proxy()
                if not proxy:
                    logger.error("代理池已空，无法继续爬取")
                    return

                crawler.ip = proxy['ip']
                crawler.port = proxy['port']
                crawler.http = proxy['http']
                crawler.session.proxies = {
                    "http": f"{proxy['http']}://{proxy['ip']}:{proxy['port']}",
                    "https": f"{proxy['http']}://{proxy['ip']}:{proxy['port']}"
                }

                try:
                    logger.info(f"使用代理 {proxy['ip']}:{proxy['port']} 爬取第 {page} 页 - {crawler.channelname}")
                    json_str = crawler.fetch_page(page)
                    if json_str:
                        result = crawler.parse_get_data(json_str)
                        results.extend(result)
                    time.sleep(1)
                    break  # 成功后退出重试循环
                except Exception as e:
                    logger.error(f"代理 {proxy['ip']}:{proxy['port']} 爬取失败: {str(e)}")
                    proxy_pool.remove_proxy(proxy['ip'], proxy['port'])
                    retry_count += 1
                    if retry_count < MAX_RETRY:
                        logger.info(f"重试第 {page} 页，尝试 {retry_count + 1} 次")
                    else:
                        logger.error(f"第 {page} 页重试 {MAX_RETRY} 次失败，跳过")

        # 线程安全写入 CSV
        with csv_lock:
            csv_writer.write_batch(results)
            mysql_client.batch_insert(results)

        # 保存统计信息
        os.makedirs('Infos', exist_ok=True)
        stats_file = os.path.join('Infos', f'stats--{crawler.channelname}-{crawler.start_page}-{crawler.final_page}.json')
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(crawler.stats, f, ensure_ascii=False, indent=2)

        logger.info(f"{crawler.channelname} 爬取完成，共处理 {crawler.stats['items_processed']} 条数据，错误 {crawler.stats['errors']} 个")

    except Exception as e:
        logger.error(f"{crawler.channelname} 爬取失败: {str(e)}")

# ===================== 多线程处理 =====================
def process_crawlers_with_proxies(df, proxy_pool):
    """多线程处理爬虫任务"""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for _, row in df.iterrows():
            crawler = DataCrawler(
                channelname=row['channelname'],
                url=row['url'],
                main_url=row['main_url'],
                id=row['id'],
                start_page=row['start_page'],
                final_page=row['final_page'],
                content_xpath=row['content_xpath'],
                source_xpath=row['source_xpath'],
                ip='',
                port='',
                http=''
            )
            futures.append(executor.submit(run, crawler, proxy_pool))
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"线程任务执行失败: {str(e)}")

# ===================== 主调度函数 =====================
def run_all_crawlers():
    """主调度函数"""
    try:
        # 清理旧代理数据
        clear_non_empty_json()

        proxy_pool = ProxyPool(db_config['mysql'])

        # 如果代理池为空，尝试刷新代理
        if proxy_pool.is_empty():
            logger.warning("代理池为空，尝试重新爬取代理...")
            if not proxy_pool.refresh_proxies():
                logger.error("刷新代理失败，无法执行爬取任务")
                return
        df = pd.read_json('config/config.json')
        # proxy_df = pd.read_json('proxy_ips/proxy_ips.json')
        #
        # if proxy_df.empty:
        #     logger.warning("未获取到有效代理IP，无法执行爬取任务")
        #     return

        # proxy_pool = ProxyPool(proxy_df.to_dict('records'))
        # if proxy_pool.is_empty():
        #     logger.warning("所有代理均不可用，无法执行爬取任务")
        #     return

        process_crawlers_with_proxies(df, proxy_pool)
        logger.info("所有爬虫任务已完成")

    except Exception as e:
        logger.error(f"定时任务执行失败: {str(e)}")

# ===================== 主程序入口 =====================
if __name__ == "__main__":
    # schedule.every().day.at("00:00").do(run_all_crawlers)
    run_all_crawlers()  # 立即执行一次
    logger.info("定时爬虫启动，开始监听任务")

    # while True:
    #     schedule.run_pending()
    #     time.sleep(60)
