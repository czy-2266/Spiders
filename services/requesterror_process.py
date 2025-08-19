import time
import subprocess
from scrapy.http import HtmlResponse
from scrapy.exceptions import IgnoreRequest
import sys
import os
def handle_error(failure):  # 移除self参数，仅保留failure
    """增强的错误处理（修正参数和调用逻辑）"""
    request = failure.request
    # 从request的meta中获取spider实例（需在请求时通过meta传递）
    spider = request.meta.get('spider')

    # 使用spider的logger打印日志（避免依赖self）
    if spider:
        spider.logger.error(f"请求失败: {failure}，URL: {request.url}")
    else:
        print(f"请求失败: {failure}，URL: {request.url}")

    if failure.check(Exception):
        error_msg = str(failure.value)
        # SSL错误处理
        if 'SSL' in error_msg or 'certificate' in error_msg.lower() or 'bad ecpoint' in error_msg:
            if spider:
                spider.logger.warning(f"SSL错误: {error_msg}, URL: {request.url}")
            else:
                print(f"SSL错误: {error_msg}, URL: {request.url}")

            # bad ecpoint错误的curl后备方案
            if 'bad ecpoint' in error_msg:
                if spider:
                    spider.logger.info(f"尝试使用curl后备方案: {request.url}")
                else:
                    print(f"尝试使用curl后备方案: {request.url}")

                MAX_RETRIES = 3
                RETRY_DELAY = 5

                try:
                    # 从request中获取headers和请求数据（需在发送请求时存入meta）
                    headers = request.meta.get('headers', {})
                    request_data = request.meta.get('request_data', {})

                    for retry in range(MAX_RETRIES):
                        try:
                            # 构建curl命令
                            cmd = [
                                'curl', '-k', '-X', request.method,  # 使用原请求的方法（GET/POST）
                                '-H', f'User-Agent: {headers.get("User-Agent", "")}',
                                '-H',
                                f'Content-Type: {headers.get("Content-Type", "application/x-www-form-urlencoded")}',
                                '--connect-timeout', '30',
                                '--max-time', '60',
                                '--ciphers', 'ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS'
                            ]

                            # 添加请求数据（适配GET/POST）
                            if request.method == 'POST' and request_data:
                                for key, value in request_data.items():
                                    cmd.extend(['-d', f'{key}={value}'])
                            elif request.method == 'GET' and request_data:
                                # GET参数拼接在URL后
                                params = '&'.join([f'{k}={v}' for k, v in request_data.items()])
                                request.url = f"{request.url}?{params}"

                            cmd.append(request.url)

                            # 执行curl
                            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                            if result.returncode == 0:
                                # 创建虚拟响应并调用原请求的回调函数
                                response = HtmlResponse(
                                    url=request.url,
                                    body=result.stdout.encode('utf-8'),
                                    encoding='utf-8'
                                )
                                # 通过request的callback调用解析函数（需确保回调函数存在）
                                if request.callback:
                                    return request.callback(response)
                                break

                        except Exception as e:
                            if retry < MAX_RETRIES - 1:
                                if spider:
                                    spider.logger.warning(f"curl后备方案失败，第 {retry + 1} 次重试: {e}")
                                else:
                                    print(f"curl后备方案失败，第 {retry + 1} 次重试: {e}")
                                time.sleep(RETRY_DELAY)
                            else:
                                if spider:
                                    spider.logger.error(f"curl后备方案失败，达到最大重试次数: {e}")
                                else:
                                    print(f"curl后备方案失败，达到最大重试次数: {e}")

                except Exception as e:
                    if spider:
                        spider.logger.error(f"curl后备方案执行出错: {e}")
                    else:
                        print(f"curl后备方案执行出错: {e}")

            # 重新创建请求，禁用SSL验证
            retry_request = request.copy()
            retry_request.meta.update({
                'verify_ssl': False,
                'dont_cache': True,
                'handle_httpstatus_list': [301, 302, 403, 500, 502, 503, 504, 421],
                'retry_times': request.meta.get('retry_times', 0) + 1
            })
            # 限制重试次数
            if retry_request.meta['retry_times'] <= 2:
                return retry_request
        else:
            if spider:
                spider.logger.error(f"请求失败: {request.url}, 错误: {failure.value}")
            else:
                print(f"请求失败: {request.url}, 错误: {failure.value}")
    return None


import logging


def setup_spider_logger(log_file="spider.log"):
    """配置爬虫专用日志记录器"""
    # 创建日志记录器
    logger = logging.getLogger("spider_logger")
    logger.setLevel(logging.DEBUG)  # 捕获所有级别日志

    # 避免重复添加处理器
    if logger.handlers:
        return logger

    # 定义日志文件名
    log_dir = "logs"

    # 确保日志目录存在（核心修改）
    os.makedirs(log_dir, exist_ok=True)  # exist_ok=True 避免目录已存在时报错

    # 再创建FileHandler
    file_handler = logging.FileHandler(
        os.path.join(log_dir, log_file),  # 用os.path.join拼接路径，跨系统兼容
        mode='a',
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)  # 文件中记录INFO及以上级别

    # 创建控制台处理器（同时输出到控制台）
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # 控制台也显示INFO及以上级别

    # 定义日志格式（与示例格式一致）
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger