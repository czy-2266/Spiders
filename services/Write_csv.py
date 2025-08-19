import pandas as pd
import os
import time
from .requesterror_process import setup_spider_logger
from datetime import datetime
logger = setup_spider_logger()


class WriteCSV:
    """CSV写入工具类（基于pandas，增强去重和容错）"""

    def __init__(self, channelname):
        self.channelname = channelname
        self.stats = {
            "batches_written": 0,
            "total_items": 0,
            "duplicates_skipped": 0  # 新增重复数据统计
        }

    def write_batch(self, batch_items, output_dir='results', chunk_size=1000):
        """
        批量写入数据到CSV（基于pandas）
        支持去重（基于"链接"字段）和分块写入
        """
        if not batch_items:
            logger.warning(f"[{self.channelname}] 批次数据为空，跳过写入")
            return False
        current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        csv_path = os.path.join(output_dir, f'{self.channelname}-{current_time}.csv')
        self.stats["total_items"] += len(batch_items)

        try:
            # 1. 转换批次数据为DataFrame
            new_df = pd.DataFrame(batch_items)
            # 检查必要字段是否存在
            required_fields = ['标题', '链接', '发布时间']
            missing_fields = [f for f in required_fields if f not in new_df.columns]
            if missing_fields:
                logger.error(f"[{self.channelname}] 缺少必要字段: {missing_fields}")
                return False

            # 2. 去重处理（基于"链接"字段）
            if os.path.exists(csv_path):
                # 读取已有数据并提取链接
                existing_df = pd.read_csv(csv_path, encoding='utf-8', usecols=['链接'])
                existing_links = set(existing_df['链接'].dropna())
                # 过滤新数据中已存在的链接
                new_df = new_df[~new_df['链接'].isin(existing_links)]
                duplicates = len(batch_items) - len(new_df)
                self.stats["duplicates_skipped"] += duplicates
                if duplicates > 0:
                    logger.info(f"[{self.channelname}] 过滤重复数据 {duplicates} 条")

            # 3. 无新数据时直接返回
            if new_df.empty:
                logger.info(f"[{self.channelname}] 无新数据可写入")
                return True

            # 4. 写入数据（分块处理大文件）
            is_first_write = not os.path.exists(csv_path)
            mode = 'w' if is_first_write else 'a'

            for i in range(0, len(new_df), chunk_size):
                chunk = new_df.iloc[i:i + chunk_size]
                # 仅首次写入且第一块带表头
                header = is_first_write and (i == 0)
                chunk.to_csv(
                    csv_path,
                    mode=mode,
                    header=header,
                    index=False,
                    encoding='utf-8',
                    # 处理特殊字符（如换行符）
                    # line_terminator='\n',
                    quoting=1  # 对字符串字段添加引号，避免逗号等符号导致列错乱
                )
                # 首次写入后后续块均不带表头
                if is_first_write:
                    is_first_write = False

            self.stats["batches_written"] += 1
            logger.info(
                f"[{self.channelname}] 写入成功 - "
                f"批次: {self.stats['batches_written']}, "
                f"本批新增: {len(new_df)}条, "
                f"累计跳过重复: {self.stats['duplicates_skipped']}条"
            )
            return True

        except Exception as e:
            logger.error(
                f"[{self.channelname}] 写入失败: {str(e)}",
                extra={"timestamp": time.time(), "batch_size": len(batch_items)}
            )
            return False