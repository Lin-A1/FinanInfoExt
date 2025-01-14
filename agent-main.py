import asyncio
import time
import fire
import fitz  # PyMuPDF, 用于从PDF中提取文本
from metagpt.logs import logger
from agent.knowCleaner import knowCleaner, parse_json

import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)


def extract_text_from_pdf(pdf_path, max_paragraph_length=800):
    textList = []
    doc = fitz.open(pdf_path)

    prev_page_text = ""

    for page_num, page in enumerate(doc):
        text_dict = page.get_text("dict")
        page_text = ""

        # 遍历文本块并将它们合并成段落
        for block in text_dict["blocks"]:
            if block["type"] == 0:  # type == 0 表示文本块
                block_text = ""
                for line in block["lines"]:
                    for span in line["spans"]:
                        block_text += span["text"] + " "
                page_text += block_text.strip() + "\n\n"  # 每个块后添加空行

        # 合并当前页和上一页的文本（避免分页断开重要信息）
        if page_num > 0:  # 非第一页
            combined_text = prev_page_text.strip() + "\n\n" + page_text.strip()
            textList.append(combined_text)
        else:
            textList.append(page_text.strip())

        # 更新上一页的文本，为下一页做准备
        prev_page_text = page_text.strip()

    # 处理文本过长的问题，按照 max_paragraph_length 切割
    final_text = []
    for page_text in textList:
        paragraphs = page_text.split("\n\n")  # 按空行分段
        current_paragraph = ""

        for paragraph in paragraphs:
            if len(current_paragraph) + len(paragraph) > max_paragraph_length:
                final_text.append(current_paragraph.strip())
                current_paragraph = paragraph
            else:
                current_paragraph += "\n\n" + paragraph

        if current_paragraph.strip():
            final_text.append(current_paragraph.strip())

    return final_text


def calculate_time(func):
    """装饰器：计算异步函数执行时间"""

    async def wrapper(*args, **kwargs):
        start_time = time.time()  # 获取开始时间
        result = await func(*args, **kwargs)  # 等待异步函数执行
        end_time = time.time()  # 获取结束时间
        execution_time = end_time - start_time  # 计算花费的时间
        print(f"异步函数 {func.__name__} 执行时间: {execution_time:.6f} 秒")
        return result

    return wrapper


def main():
    pdf_path = r'/home/lin/work/code/DeepLearning/LLM/FinanInfoExt/data/工银瑞信养老目标日期2050五年持有期混合型发起式基金中基金（FOF）更新的招募说明书.pdf'
    textList = extract_text_from_pdf(pdf_path)
    pdf_name = pdf_path.split('/')[-1].replace('.pdf', '')
    for text in textList:
        try:
            role = knowCleaner('data')
            result = asyncio.run(role.run(text))
            logger.info(result)
        except Exception as e:
            pass


if __name__ == "__main__":
    fire.Fire(main)
