import os
import json
def clear_non_empty_json():
    file_path='proxy_ips/proxy_ips.json'
    # 1. 检查文件是否存在，不存在则跳过（或可选择创建空文件）
    if not os.path.exists(file_path):
        print(f"文件不存在：{file_path}，跳过操作")
        return

    # 2. 检查文件是否已为空（大小为0）
    if os.path.getsize(file_path) == 0:
        print(f"文件已为空：{file_path}，跳过操作")
        return

    # 3. 读取文件内容，判断是否有有效内容
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)

        # 若JSON内容为空（空字典、空列表等），则不处理
        if not content:
            print(f"JSON内容为空：{file_path}，跳过操作")
            return

    except json.JSONDecodeError:
        # 非法JSON视为“有内容”，需要清空
        pass
    except Exception as e:
        print(f"读取文件出错：{e}，跳过操作")
        return

    # 4. 清空文件内容（保留文件本身）
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write('')  # 写入空字符串，清空内容
    print(f"文件内容已清空：{file_path}")