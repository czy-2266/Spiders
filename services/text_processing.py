import html
import re
from datetime import datetime, date
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from .requesterror_process import setup_spider_logger
logger = setup_spider_logger()
# 下载 NLTK 的必要数据 这里是需要打开的
# nltk.download('stopwords')
# nltk.download('wordnet')

def preprocess_text(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    # 将文本转换为小写
    text = text.lower()
    # 去除特殊字符
    text = re.sub(r'\n{2,}', '\n', text)
    # 分词
    words = text.split()
    # 去除停用词
    stop_words = set(stopwords.words('english'))
    words = [word for word in words if word not in stop_words]
    # 词形还原
    lemmatizer = WordNetLemmatizer()
    words = [lemmatizer.lemmatize(word) for word in words]
    return "\n".join(words)

def clean_text_newlines(text: str) -> str:
    if not isinstance(text, str):
        return text
    text=str(text)
    text = text.lstrip()
    text = text.rstrip()
    text = re.sub(r'<[^>]+>', '', text)
    # 强化JavaScript代码过滤
    # 1. 移除<script>标签及内容（包括类型声明）
    text = re.sub(r'<script\s*[^>]*?>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)

    # 2. 移除内联事件处理器 (onclick, onload等)
    text = re.sub(r'on\w+\s*=\s*["\'][^"\']*["\']', '', text, flags=re.IGNORECASE)

    # 3. 移除JavaScript代码块 (包括匿名函数、箭头函数)
    text = re.sub(r'(var|let|const)\s+\w+\s*=\s*function\s*[^\{]*\{.*?\}', '', text, flags=re.DOTALL)
    text = re.sub(r'function\s+\w*\s*\([^)]*\)\s*\{.*?\}', '', text, flags=re.DOTALL)
    text = re.sub(r'\([^)]*\)\s*=>\s*\{.*?\}', '', text, flags=re.DOTALL)
    text = re.sub(r'\w+\s*=>\s*[^;}]*', '', text, flags=re.DOTALL)

    # 4. 移除JS特定语句和表达式
    text = re.sub(r'console\.(log|error|warn|debug)\([^)]*\);?', '', text)
    text = re.sub(r'alert\([^)]*\);?', '', text)
    text = re.sub(r'window\.[^;=]+[;=]', '', text)
    text = re.sub(r'document\.[^;=]+[;=]', '', text)

    # 6. 移除CSS样式块（可能包含在JS中的样式）
    text = re.sub(r'<style\s*[^>]*?>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'\.[a-zA-Z0-9_]+\s*\{[^}]*\}', '', text)

    # 7. 移除HTML标签
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'\.TRS_Editor\s*[A-Za-z0-9_,\s]*\{[^}]*\}', '', text)
    text = re.sub(r'\.trs\s*[A-Za-z0-9_,\s]*\{[^}]*\}', '', text)
    text = re.sub(r'\.TRS_AUTO\s*[A-Za-z0-9_,\s]*\{[^}]*\}', '', text)
    text = re.sub(r'\$\s*\(\s*function\s*\(\s*\)\s*\{.*?\}\s*\)\s*;?', '', text, flags=re.DOTALL)
    text = re.sub(r'\$\s*\(\s*document\s*\)\s*\.\s*ready\s*\(\s*function\s*\(\s*\)\s*\{.*?\}\s*\)\s*;?', '', text,
                  flags=re.DOTALL)
    text = re.sub(r'on\w+=\s*[\'"].*?[\'"]', '', text)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'扫一扫在手机打开当前页', '', text)
    text = re.sub(r'打印本页', '', text)
    cleaned_text2 = re.sub(r'([\t]*[ \t]*[\r\n]+)+', '\n', text)
    # 移除首尾空白（此时行首的制表符是我们添加的，不应删除）
    cleaned_text2 = cleaned_text2.strip()

    # 在每行前（非空行）添加制表符
    cleaned_text2 = re.sub(r'(?m)^(?!$)', '\t', cleaned_text2)

    chinese_count = len(re.findall(r'[\u4e00-\u9fff]', cleaned_text2))
    if chinese_count >= 10:
        return cleaned_text2
    else:
        pass

def extract_source(text):
    # 1. 优先匹配任意位置的"来源："后的中文字符
    text=text.strip()
    source_match = re.search(r'来源：([\u4e00-\u9fa5]+)', text)
    if source_match:
        return source_match.group(1)

    # 2. 若无来源，匹配开头到第一个分隔符的内容
    # 分隔符包括：｜、普通空格、不间断空格(\u00a0)、制表符等空白字符
    prefix_match = re.match(r'^([^|\s\u00a0]+)', text)
    if prefix_match:
        return prefix_match.group(1)

    return None  # 无匹配内容

def parse_with_dateutil(date_str):
    """
    解析日期字符串为datetime对象
    支持多种日期格式
    """
    if not date_str or str(date_str).strip() == '':
        return None

    # 清理日期字符串
    date_str = re.sub(r'\s+', ' ', str(date_str)).strip()

    # 支持的日期格式
    date_formats = [
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%Y年%m月%d日',
        '%Y.%m.%d',
        '%m-%d-%Y',
        '%m/%d/%Y',
        '%Y-%m',
        '%Y/%m',
        '%Y年%m月'
    ]

    # 尝试匹配和解析日期
    for fmt in date_formats:
        try:
            # 先尝试直接解析
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    # 尝试提取日期部分
    date_patterns = [
        r'(\d{4}[-/年]\d{1,2}[-/月]\d{1,2}[日]?)',
        r'(\d{4}\.\d{1,2}\.\d{1,2})',
        r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
        r'(\d{4}[-/年]\d{1,2}[月]?)',
    ]

    for pattern in date_patterns:
        match = re.search(pattern, date_str)
        if match:
            found_date = match.group(1)
            # 标准化分隔符
            found_date = re.sub(r'[年月日]', '-', found_date)
            found_date = re.sub(r'[-/\.]+', '-', found_date).strip('-')

            # 尝试解析标准化后的日期
            for fmt in ['%Y-%m-%d', '%Y-%m', '%m-%d-%Y']:
                try:
                    return datetime.strptime(found_date, fmt)
                except ValueError:
                    continue

    logger.warning(f"无法解析日期: {date_str}")
    return None


def html_to_clean_text(html_content):
    """从HTML中提取干净的正文内容，保留段落结构"""
    if not isinstance(html_content, str):
        return ""

    # 替换HTML实体
    text = html.unescape(html_content)

    # 提取<p>标签内容
    ps = re.findall(r'<p[^>]*>(.*?)</p>', text, flags=re.S)
    if not ps:
        # 如果没有<p>标签，尝试<div>标签
        ps = re.findall(r'<div[^>]*>(.*?)</div>', text, flags=re.S)

    # 如果没有<p>或<div>，尝试其他块级标签
    if not ps:
        # 尝试提取所有块级标签内容
        block_tags = re.findall(r'<(p|div|section|article|main|header|footer)[^>]*>(.*?)</\1>', text, flags=re.S)
        if block_tags:
            ps = [content for _, content in block_tags]

    # 如果没有找到块级标签，回退到整个内容
    if not ps:
        ps = [text]

    # 过滤和清理每个段落
    filtered_ps = []
    useless_keywords = [
        "无障碍阅读", "视频", "广告", "相关链接", "推荐阅读",
        "图片", "声明", "责任编辑", "来源", "原标题", "分享",
        "扫一扫", "打印", "返回顶部", "返回首页", "点击查看",
        "查看更多", "版权声明", "免责声明", "投稿"
    ]

    for p in ps:
        if not p:
            continue

        # 移除内部HTML标签
        p = re.sub(r'<[^>]+>', '', p).strip()

        # 过滤无关内容
        if any(kw in p for kw in useless_keywords):
            continue

        # 过滤过短内容
        if len(p) < 15:
            continue

        # 过滤纯符号/数字内容
        if re.fullmatch(r'[\d\W_]+', p):
            continue

        filtered_ps.append(p)

    # 组合成带换行的正文
    return '\n\n'.join(filtered_ps)


def html_extract_p(html_content):
    # 使用lxml解析器解析HTML内容
    soup = BeautifulSoup(html_content, 'lxml')

    # 先尝试查找所有p标签
    tags = soup.find_all('p')

    # 如果没有找到p标签，则查找div标签
    if not tags:
        tags = soup.find_all('div')

    # 将找到的标签转换为字符串并使用换行符连接
    return '\n'.join(str(tag) for tag in tags)


def is_today(publish_datetime: datetime) -> bool:
    """
    判断发布时间是否为今天
    :param publish_datetime: 解析后的datetime对象
    :return: 是今天返回True，否则False
    """
    if not publish_datetime:
        return False
    today = date.today()
    return publish_datetime.date() == today


