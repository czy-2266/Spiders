import redis
import threading
import time
import json
import logging
import re
from urllib.parse import quote

from config import REDIS_CONFIG, SIGN_CONFIG, CRAWL_CONFIG
from utils.tb_utils import encrypt_with_js, handle_error
from utils.user_agent import get_random_ua
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('RedisListener')

class RedisContinuousListener:
    def __init__(self):
        self.redis_client = self._init_redis()
        self.headers = CRAWL_CONFIG
        self.token = self._extract_token()
        self.listening = False
        self.listener_thread = None
        self.task_processor = None
        self.total_page = CRAWL_CONFIG["total_page"]

    def _init_redis(self):
        try:
            client = redis.Redis(
                host=REDIS_CONFIG['host'],
                port=REDIS_CONFIG['port'],
                db=REDIS_CONFIG['db'],
                password=REDIS_CONFIG['password'],
                decode_responses=True,
                socket_keepalive=True,
                socket_timeout=30
            )
            client.ping()
            logger.info(f"成功连接Redis数据库（编号：{REDIS_CONFIG['db']}）")
            return client
        except Exception as e:
            logger.error(f"Redis连接失败：{str(e)}")
            raise

    def _extract_token(self):
        try:
            print(self.headers.get("headers").get('cookie'))
            token_match = re.findall(
                f'{SIGN_CONFIG["token_cookie_key"]}=(.*?)_',
                self.headers.get("headers").get('cookie')
            )
            if not token_match:
                raise Exception("Cookie中未找到token")
            return token_match[0]
        except Exception as e:
            logger.error(f"提取token失败：{str(e)}")
            raise

    def set_task_processor(self, processor):
        self.task_processor = processor

    def start_listening(self):
        if self.listening:
            logger.warning("已在监听中")
            return

        self.listening = True
        self.listener_thread = threading.Thread(
            target=self._listen_loop,
            name="RedisListener",
            daemon=True
        )
        self.listener_thread.start()
        logger.info("Redis监听已启动")

    def stop_listening(self):
        self.listening = False
        if self.listener_thread and self.listener_thread.is_alive():
            self.listener_thread.join(timeout=5)
        logger.info("Redis监听已停止")

    def _listen_loop(self):
        while self.listening:
            try:
                data = self.redis_client.blpop(REDIS_CONFIG["queue_name"], timeout=30)
                if data:
                    self._process_redis_data(data)
            except redis.ConnectionError:
                logger.error("Redis连接断开，尝试重连...")
                self.redis_client = self._init_redis()
                time.sleep(5)
            except Exception as e:
                logger.error(f"监听循环异常：{str(e)}", exc_info=True)
                time.sleep(1)

    def _process_redis_data(self, data):
        key, value = data
        logger.info(f"处理任务：{key}")

        try:
            json_data = json.loads(value)
            app_id = json_data.get("appId")
            params = json_data.get("params", {})

            if not app_id or not params:
                logger.error("任务格式错误：缺少appId或params")
                return

            for page in range(1, self.total_page+1):
                try:
                    current_params = params.copy()
                    current_params["page"] = page

                    # 保留你的sign生成逻辑
                    params_str2 = json.dumps(current_params, separators=(',', ':'))
                    params_str = params_str2.replace('"', '\\"')
                    ep_data_str = f'{{"appId":"{app_id}","params":"{params_str}"}}'

                    timestamp = int(time.time() * 1000)
                    sign = encrypt_with_js(
                        token=self.token,
                        timestamp=timestamp,
                        appkey=SIGN_CONFIG["appkey"],
                        data_dict=ep_data_str,
                        js_path=SIGN_CONFIG["js_path"]
                    )

                    if not sign:
                        logger.error(f"第{page}页sign生成失败")
                        continue

                    # 构造请求URL
                    list_url = 'https://h5api.m.taobao.com/h5/mtop.relationrecommend.wirelessrecommend.recommend/2.0/'
                    url = f'{list_url}?jsv=2.7.2&appKey=12574478&t={timestamp}&sign={sign}&api=mtop.relationrecommend.wirelessrecommend.recommend&v=2.0&type=jsonp&dataType=jsonp&callback=mtopjsonp4&data={quote(ep_data_str)}'
                    # print('构建的url:',url)


                    if self.task_processor:
                        print('来到了task_processor',url)
                        self.task_processor.process_page(url, page)

                except Exception as e:
                    logger.error(f"第{page}页处理失败：{str(e)}")
                    handle_error(e)

        except json.JSONDecodeError:
            logger.error(f"JSON解析失败：{value[:100]}")
        except Exception as e:
            logger.error(f"任务处理异常：{str(e)}")