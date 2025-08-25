"""淘宝爬虫工具类（保留你的核心逻辑）"""
import logging
import execjs  # 用于调用JS加密
import json
import re
import time
from bs4 import BeautifulSoup
logger = logging.getLogger('TBUtils')


def encrypt_with_js(token, timestamp, appkey, data_dict, js_path):
    """调用本地JS文件生成sign（你的原逻辑）"""
    try:
        timestamp_str = str(timestamp)
        raw_str = f"{token}&{timestamp_str}&{appkey}&{data_dict}"
        with open(js_path, 'r', encoding='utf-8') as f:
            js_code = f.read()

        ctx = execjs.compile(js_code)
        # 假设你的JS文件中有一个generateSign函数
        sign = ctx.call(
            'c',  # JS函数名
            raw_str
        )
        return sign
    except Exception as e:
        logger.error(f"JS加密失败：{str(e)}")
        return None


def get_product_data(response_text):
    try:
        # 1. 提取 JSONP 中的纯 JSON 字符串（去除外层函数包裹）
        match = re.search(r'mtopjsonp\d+\((\{.*\})\)', response_text, re.DOTALL)
        json_str = match.group(1)
        # 2. 解析为 Python 字典
        print(type(json_str))
        data = json.loads(json_str)

        # 3. 逐层查看数据结构（根据实际字段调整，以下为示例）
        product_list = data.get('data', {}).get('itemsArray', [])  # 此处字段名需替换为实际存在的商品列表字段

        # 4. 提取商品信息（示例：假设每个商品包含 id、name、price 等字段）
        result = []
        for product in product_list:
            print(type(product))
            product_info = {
                "product_id": product.get('item_id'),
                "title": extract_text(product.get('title')),
                "price": product.get('price'),
                'product_source': product.get('procity', ''),
                'product_situation': f'{product.get('hotListInfo', {}).get('rank_short_text', '')},{product.get('realSales', '')},{product.get("shopTag", '')}',
            }
            print(type(product_info))
            print(product_info)
            result.append(product_info)

        return result

    except Exception as e:
        print(f"提取商品数据失败：{e}")
        return None


def get_detail_comment(response_text,product_id):
    try:
        # 1. 提取 JSONP 中的纯 JSON 字符串（去除外层函数包裹）
        match = re.search(r'mtopjsonppcdetail\d+\((\{.*\})\)', response_text, re.DOTALL)
        json_str = match.group(1)
        # 2. 解析为 Python 字典
        print(type(json_str))
        data = json.loads(json_str)

        # 3. 逐层查看数据结构（根据实际字段调整，以下为示例）
        comment_list = data.get('data', {}).get('rateList', [])  # 此处字段名需替换为实际存在的商品列表字段

        # 4. 提取商品信息（示例：假设每个商品包含 id、name、price 等字段）
        result = []
        for comment in comment_list:
            print(type(comment))
            commnet_info = {
                'product_id': product_id,
                'product_title':comment.get('auctionTitle'),
                "comment_id": comment.get('id'),
                "comment_info": extract_text(comment.get('feedback')),
                "commnet_time": comment.get('feedbackDate'),
                'comment_product': comment.get('skuValueStr', ''),
                 'crawl_time': time.strftime('%Y-%m-%d %H:%M:%S')
                    }
            print(type(commnet_info))
            print(commnet_info)
            result.append(commnet_info)

        return result

    except Exception as e:
        print(f"提取商品数据失败：{e}")
        return None




def extract_text(text):
    """提取纯文本内容"""
    if text == None:
        return ''

    # 移除HTML标签
    soup = BeautifulSoup(text, 'lxml')
    # 移除多余空白
    text = soup.get_text()
    return text

def handle_error(error):
    """错误处理（你的原逻辑）"""
    logger.error(f"处理错误：{str(error)}")