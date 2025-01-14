import os
import re
import json

from metagpt.actions import Action, UserRequirement
from metagpt.schema import Message
from metagpt.roles import Role


def parse_json(rsp):
    pattern = r"```json(.*)```"
    match = re.search(pattern, rsp, re.DOTALL)
    json = match.group(1) if match else rsp
    return json


def process_data(data, max_length=50):
    """
    处理字典数据，替换超过指定长度的值为空字符串。

    :param data: 输入的字典数据
    :param max_length: 设置的最大长度阈值
    :return: 处理后的字典
    """
    # 遍历字典
    for key, value in data.items():
        if isinstance(value, dict):
            # 如果值是字典，递归调用
            data[key] = process_data(value, max_length)
        elif isinstance(str(value), str) and len(str(value)) > max_length:
            # 如果值是字符串并且长度超过最大限制，替换为空字符串
            data[key] = ""
    return data


class knowClean(Action):
    PROMPT_TEMPLATE: str = """
        文本：{text}  
        你是一个基金信息提取系统，任务是从给定的基金相关文本数据中提取结构化的基金信息，并将其补充到已知信息中。请严格按照以下要求处理：
        
        1. **数据优化**：  
           - 确保提取的基金信息准确无误，避免冗余、不相关或错误的内容。  
           - 避免将相似但非真实的内容误判为有效信息。
           - 对比以有信息，若有更合适的数据，可以进行数据的替换
        
        2. **数据输入规则**：  
           - 你接收到的文本可能是不完整的片段，可能不存在有价值的信息。对于不存在的信息，应使用空字符串占位。  
           - **禁止删除或覆盖已知信息中的字段值**。已知信息中的非空字段（即已经包含有效值的字段）应保持原值。  
           - 如果文本中存在更准确或更完整的信息，可以**仅补充**已知信息中的空字段。如果已知信息中的字段为空，则可以更新该字段，请确保更新信息真实有效。  
           - **确保已知信息中的数据不被错误地覆盖或抹除**，只在空字段或缺失信息的情况下进行补充。
        
        3. **输出规则**：  
           - 如果文本中没有找到对应的信息，字段值应保持为空字符串，不得编造任何数据。  
           - 以下是已有信息以及模板，**请严格按照模板格式进行输出，尤其是字段命名**
           - 输出的结果必须是紧凑的 JSON 格式，符合以下模板结构：  

             ```json
             {knowledge}
             ```  
           - 确保输出仅包含有效的 JSON 数据，禁止包含任何额外的解释、注释、转义符或换行符。
        
        请按照以上规则返回结构化的基金信息。
    """

    name: str = "knowClean"

    async def run(self, text: str, pdf_name: str) -> dict:
        # 打开并读取 JSON 文件
        if os.path.exists(f'workspace/{pdf_name}.json'):
            with open(f'workspace/{pdf_name}.json', 'r', encoding='utf-8') as file:
                data = json.load(file)
        else:
            with open('data.json', 'r', encoding='utf-8') as file:
                data = json.load(file)

        data = process_data(data, max_length=150)
        # 创建 prompt
        prompt = self.PROMPT_TEMPLATE.format(text=text, knowledge=json.dumps(data, ensure_ascii=False, indent=4))
        rsp = await self._aask(prompt)

        # 解析响应内容
        text = parse_json(rsp)
        json_data = json.loads(text)
        # print('-----------------------')
        # print(json_data)
        # print('-----------------------')
        if type(json_data) == dict:
            # 更新 JSON 数据
            for key, value in json_data.items():
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        if sub_value not in ['', ' ', "", " ", None]:
                            # 只有新的值不是空字符串时，才更新
                            if sub_key in data.get(key, {}):
                                data[key][sub_key] = sub_value

            # 保存更新后的数据
            with open(f'workspace/{pdf_name}.json', 'w', encoding='utf-8') as file:
                json.dump(data, file, ensure_ascii=False, indent=4)

            return text
        else:
            return str(data)


class knowCleaner(Role):
    name: str = "Fund Information Extractor"
    profile: str = "从PDF文档中提取基金信息"

    def __init__(self, pdfname, **kwargs):
        super().__init__(**kwargs)
        self.set_actions([knowClean])
        self._watch([UserRequirement])
        self.pdfname = pdfname

    async def _act(self) -> Message:
        todo = self.rc.todo
        msg = self.get_memories(k=1)[0]
        context = self.get_memories()
        text = await todo.run(context, self.pdfname)
        msg = Message(text=text, role=self.profile, cause_by=type(todo))

        return msg
