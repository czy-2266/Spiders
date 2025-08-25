import logging
from .redis_listener import RedisContinuousListener
from .task_processor import TaskProcessor

logger = logging.getLogger('SpiderNode')

class SpiderNode:
    def __init__(self):
        self.listener = RedisContinuousListener()
        self.processor = TaskProcessor()
        self.listener.set_task_processor(self.processor)

    def start(self):
        """启动节点"""
        try:
            self.listener.start_listening()
            # 保持节点运行
            while True:
                input("按Ctrl+C停止节点...\n")
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            logger.error(f"节点运行异常：{str(e)}")
            self.stop()

    def stop(self):
        """停止节点"""
        self.listener.stop_listening()
        self.processor.db_handler.close()  # 关闭数据库连接
        logger.info("节点已停止")