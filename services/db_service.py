import redis
import pymysql
from pymysql.cursors import DictCursor
from datetime import datetime
from .requesterror_process import  setup_spider_logger
logger = setup_spider_logger()


class RedisClient:
    """Redis分布式去重客户端"""

    def __init__(self, config):
        self.client = redis.Redis(
            host=config['host'],
            port=config['port'],
            password=config['password'],
            db=config['db'],
            decode_responses=True
        )
        self.duplicate_key = "crawled_news_url"  # 去重集合键名

    def is_duplicate(self, news_url):
        """检查新闻ID是否已爬取"""
        return self.client.sismember(self.duplicate_key, news_url)

    def mark_crawled(self, news_url):
        """标记新闻ID为已爬取"""
        self.client.sadd(self.duplicate_key, news_url)


class MySQLClient:
    """MySQL数据库客户端（支持自动建库建表）"""

    def __init__(self, config):
        self.config = config
        self.connection = None
        self._init_db()  # 自动初始化数据库

    def _init_db(self):
        """初始化数据库连接，自动创建库和表"""
        # 1. 先连接到默认数据库（无库名）
        # 3. 连接到目标数据库
        self.connection = pymysql.connect(
            host=self.config['host'],
            port=self.config['port'],
            user=self.config['user'],
            password=self.config['password'],
            db=self.config['db_name'],
            charset='utf8mb4',
            cursorclass=DictCursor
        )

        # 4. 创建新闻表（如果不存在）
        with self.connection.cursor() as cursor:
            create_table_sql = """
                               CREATE TABLE IF NOT EXISTS news \
                               ( \
                                   id \
                                   INT \
                                   AUTO_INCREMENT \
                                   PRIMARY \
                                   KEY, \
                                   channel \
                                   VARCHAR \
                               ( \
                                   100 \
                               ) NOT NULL,
                                   title VARCHAR \
                               ( \
                                   255 \
                               ) NOT NULL,
                                   doc_number VARCHAR \
                               ( \
                                   50 \
                               ) DEFAULT '',
                                   validity VARCHAR \
                               ( \
                                   20 \
                               ) DEFAULT '',
                                   url VARCHAR \
                               ( \
                                   255 \
                               ) UNIQUE NOT NULL,
                                   publish_time DATETIME,
                                   source VARCHAR \
                               ( \
                                   100 \
                               ) DEFAULT '',
                                   author VARCHAR \
                               ( \
                                   50 \
                               ) DEFAULT '',
                                   content TEXT,
                                   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; \
                               """
            cursor.execute(create_table_sql)
        self.connection.commit()

    def batch_insert(self, items):
        """批量插入数据"""
        if not items:
            return

        try:
            with self.connection.cursor() as cursor:
                sql = """
                      INSERT INTO news (channel, title, doc_number, validity, url, \
                                        publish_time, source, author, content) \
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY \
                      UPDATE \
                          channel= \
                      VALUES (channel), title= \
                      VALUES (title), content= \
                      VALUES (content) \
                      """
                # 转换数据格式（处理datetime对象）
                data = [
                    (
                        item['频道'], item['标题'], item['文号'], item['有效性'],
                        item['链接'], item['发布时间'], item['信息来源'],
                        item['作者'], item['正文']
                    ) for item in items
                ]
                cursor.executemany(sql, data)
            self.connection.commit()
            logger.info(f"成功插入 {len(items)} 条数据到MySQL")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"MySQL批量插入失败: {str(e)}")
            raise

    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()




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
            db=self.config['db_name'],
            charset='utf8mb4',
            cursorclass=DictCursor
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