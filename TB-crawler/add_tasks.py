"""向Redis添加爬取任务"""
import redis
import json
from config import REDIS_CONFIG

def add_task(keyword):
    """添加关键词任务"""
    client = redis.Redis(
        host=REDIS_CONFIG['host'],
        port=REDIS_CONFIG['port'],
        db=REDIS_CONFIG['db'],
        password=REDIS_CONFIG['password'],
        decode_responses=True
    )

    # 任务数据格式（与你的Redis数据格式一致）
    task_data = {
        "appId": "34385",
        "params": {
            "device": "HMA-AL00",
            "isBeta": "false",
            "grayHair": "false",
            "from": "nt_history",
            "brand": "HUAWEI",
            "info": "wifi",
            "q": keyword,  # 搜索关键词
            "m": "pc",
            "n": 48,
            "sort": "_coefp",
            # 其他参数保持不变...
        }
    }

    client.rpush(REDIS_CONFIG["queue_name"], json.dumps(task_data))
    print(f"已添加关键词任务：{keyword}")

if __name__ == "__main__":
    # 添加示例任务
    keywords = ["文玩", "手串", "核桃"]
    for kw in keywords:
        add_task(kw)
    print("所有任务添加完成")