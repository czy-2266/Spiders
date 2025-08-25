from datetime import datetime

import pymysql
import logging
from config import MYSQL_CONFIG

logger = logging.getLogger('DBHandler')

class DBHandler:
    def __init__(self):
        self.conn = self._init_db()
        self._create_tables()
        self.create_comments_tables()

    def _init_db(self):
        try:
            conn = pymysql.connect(
                host=MYSQL_CONFIG["host"],
                port=MYSQL_CONFIG["port"],
                user=MYSQL_CONFIG["user"],
                password=MYSQL_CONFIG["password"],
                database=MYSQL_CONFIG["database"],
                charset=MYSQL_CONFIG["charset"]
            )
            logger.info("MySQL连接成功")
            return conn
        except Exception as e:
            logger.error(f"MySQL连接失败：{str(e)}")
            raise

    def _create_tables(self):
        """创建商品表（根据你的需求调整）"""
        try:
            with self.conn.cursor() as cursor:
                # 商品表
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    product_id VARCHAR(50) NOT NULL UNIQUE,
                    title TEXT,
                    price VARCHAR(20),
                    product_source VARCHAR(100),
                    product_situation TEXT,
                    crawl_time DATETIME,
                    INDEX idx_product_id (product_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """)
                self.conn.commit()
                logger.info("数据产品表检查/创建成功")
        except Exception as e:
            logger.error(f"创建产品表失败：{str(e)}")
            self.conn.rollback()
    def create_comments_tables(self):
        """创建商品表（根据你的需求调整）"""
        try:
            with self.conn.cursor() as cursor:
                # 商品表
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS comments (
                    id INT AUTO_INCREMENT PRIMARY KEY, 
                     product_id VARCHAR(50) NOT NULL UNIQUE,
                    product_title TEXT,
                    comment_id VARCHAR(50) NOT NULL UNIQUE,
                    comment_info TEXT,
                    commnet_time VARCHAR(50),
                    comment_product TEXT,
                    crawl_time DATETIME,
                    INDEX idx_product_id (product_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """)
                self.conn.commit()
                logger.info("数据产品表检查/创建成功")
        except Exception as e:
            logger.error(f"创建产品表失败：{str(e)}")
            self.conn.rollback()



    def save_product(self, item):
        """保存商品数据"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                INSERT IGNORE INTO products 
                (product_id, title, price, product_source, product_situation, crawl_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    item['product_id'],
                    item['title'],
                    item['price'],
                    item['product_source'],
                    item['product_situation'],
                    item['crawl_time']
                ))
                self.conn.commit()
        except Exception as e:
            logger.error(f"保存商品失败：{str(e)}")
            self.conn.rollback()
    def save_comment(self, item):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                INSERT IGNORE INTO comments 
                (product_id, product_title, comment_id, comment_info, commnet_time, comment_product,crawl_time)
                VALUES (%s, %s, %s, %s, %s, %s,%s)
                """, (
                    item['product_id'],
                    item['product_title'],
                    item['comment_id'],
                    item['comment_info'],
                    item['commnet_time'],
                    item['comment_product'],
                    item['crawl_time'],
                ))
                self.conn.commit()
        except Exception as e:
            logger.error(f"保存商品失败：{str(e)}")
            self.conn.rollback()

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("MySQL连接已关闭")


class MySQLProxyClient:
    """代理IP的MySQL数据库客户端"""

    def __init__(self, config):
        self.config = config
        self.connection = None
        self._init_db()

    def _init_db(self):
        """初始化数据库连接，创建代理表"""
        # 连接到数据库
        self.connection = pymysql.connect(
            host=self.config['host'],
            port=self.config['port'],
            user=self.config['user'],
            password=self.config['password'],
            db=self.config['database'],
            charset='utf8mb4'
        )


        # 创建代理表（如果不存在）
        with self.connection.cursor() as cursor:
            create_table_sql = """
                               CREATE TABLE IF NOT EXISTS proxy_ips \
                               ( \
                                   id \
                                   INT \
                                   AUTO_INCREMENT \
                                   PRIMARY \
                                   KEY, \
                                   ip \
                                   VARCHAR \
                               ( \
                                   50 \
                               ) NOT NULL,
                                   port VARCHAR \
                               ( \
                                   10 \
                               ) NOT NULL,
                                   http VARCHAR \
                               ( \
                                   10 \
                               ) NOT NULL,
                                   source VARCHAR \
                               ( \
                                   100 \
                               ) DEFAULT '',
                                   is_valid TINYINT \
                               ( \
                                   1 \
                               ) DEFAULT 1,
                                   last_checked DATETIME,
                                   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                   UNIQUE KEY unique_ip_port \
                               ( \
                                   ip, \
                                   port \
                               )
                                   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; \
                               """
            cursor.execute(create_table_sql)
        self.connection.commit()

    def batch_insert(self, proxies):
        """批量插入代理IP，已存在的则更新状态"""
        if not proxies:
            return

        try:
            with self.connection.cursor() as cursor:
                sql = """
                      INSERT INTO proxy_ips (ip, port, http, source, is_valid, last_checked)
                      VALUES (%s, %s, %s, %s, 1, %s) ON DUPLICATE KEY \
                      UPDATE \
                          http= \
                      VALUES (http), source = \
                      VALUES (source), is_valid=1, last_checked= \
                      VALUES (last_checked) \
                      """
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                data = [
                    (p['ip'], p['port'], p['http'], p['source'], now)
                    for p in proxies
                ]
                cursor.executemany(sql, data)
            self.connection.commit()
            logger.info(f"成功插入/更新 {len(proxies)} 个代理IP到数据库")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"代理IP批量插入失败: {str(e)}")
            raise

    def get_valid_proxies(self):
        """获取所有有效的代理IP"""
        try:
            with self.connection.cursor() as cursor:
                # 查询最近24小时内验证有效的代理
                sql = """
                      SELECT ip, port, http, source
                      FROM proxy_ips
                      WHERE is_valid = 1
                        AND last_checked >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                      ORDER BY last_checked DESC \
                      """
                cursor.execute(sql)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"获取有效代理失败: {str(e)}")
            return []

        # 新增：检查有效代理数量是否大于10
    def has_enough_proxies(self, min_count=10):
        """
        检查数据库中有效代理数量是否满足最小要求
        :param min_count: 最小代理数量（默认10）
        :return: bool - 数量达标返回True，否则返回False
        """
        try:
            with self.connection.cursor() as cursor:
                # 仅查询数量（count(*) 比 fetchall() 更高效）
                sql = """
                      SELECT COUNT(*) AS proxy_count
                      FROM proxy_ips
                      WHERE is_valid = 1
                        AND last_checked >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                      """
                cursor.execute(sql)
                result = cursor.fetchone()  # 获取计数结果（字典或元组，取决于数据库驱动）

                # 兼容不同数据库驱动的返回格式（有的返回字典，有的返回元组）
                proxy_count = result['proxy_count'] if isinstance(result, dict) else result[0]
                logger.info(f"当前有效代理数量: {proxy_count}，最小要求: {min_count}")

                return proxy_count > min_count  # 数量大于10返回True
        except Exception as e:
            logger.error(f"检查代理数量失败: {str(e)}")
            return False  # 异常时默认返回“数量不足”，避免误判

    def mark_invalid(self, ip, port):
        """标记代理为无效"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                      UPDATE proxy_ips
                      SET is_valid     = 0,
                          last_checked = %s
                      WHERE ip = %s \
                        AND port = %s \
                      """
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(sql, (now, ip, port))
            self.connection.commit()
            logger.info(f"标记代理 {ip}:{port} 为无效")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"标记代理无效失败: {str(e)}")

    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()