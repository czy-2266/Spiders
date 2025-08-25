"""启动分布式爬虫节点"""
from core.spider_node import SpiderNode

if __name__ == "__main__":
    print("淘宝分布式爬虫节点启动中...")
    node = SpiderNode()
    node.start()