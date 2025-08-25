import csv
import os
import logging
from datetime import datetime
import time

logger = logging.getLogger('CSVHandler')

class CSVHandler:
    def __init__(self):
        self.product_file = f"products_{datetime.now().strftime('%Y%m%d%')}_{int(time.time() * 1000)}.csv"
        self._init_csv()

    def _init_csv(self):
        """初始化CSV文件（添加表头）"""
        if not os.path.exists(self.product_file):
            with open(self.product_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'product_id', 'title', 'price', 'product_source',
                    'product_situation', 'crawl_time'
                ])
                writer.writeheader()
            logger.info(f"CSV文件初始化：{self.product_file}")

    def save_product(self, item):
        """保存商品到CSV"""
        try:
            with open(self.product_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=item.keys())
                writer.writerow(item)
        except Exception as e:
            logger.error(f"保存CSV失败：{str(e)}")
            