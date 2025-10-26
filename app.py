import re
from typing import Dict, List, Any
import json

class AccountGroupFormatter:
    def __init__(self):
        self.patterns = {
            'account': r'账户：(.+?) - (.+)',
            'total_digits': r'总数字数: (\d+)',
            'total_skip': r'总跳过金额: ([\d.]+)元',
            'match_rate': r'全部匹配度: ([\d.]+)/%',
            'skip_content': r'跳过内容: (.+)',
            'absolute_content': r'绝对内容: (.+)'
        }
    
    def parse_content(self, content: str) -> List[Dict[str, Any]]:
        """解析原始内容"""
        groups = []
        current_group = {}
        
        lines = content.split('\n')
        i = 0
        
        while i < len(lines):
            line = lines[i].strip()
            
            # 检测新组合开始
            if line.startswith('## 组合'):
                if current_group:
                    groups.append(current_group)
                current_group = {'group_name': line}
                i += 1
                continue
            
            # 解析账户信息
            if '账户：' in line:
                match = re.search(self.patterns['account'], line)
                if match:
                    current_group['account1'] = match.group(1)
                    current_group['account2'] = match.group(2)
            
            # 解析其他字段
            for field, pattern in self.patterns.items():
                if field != 'account' and field in line.lower():
                    match = re.search(pattern, line)
                    if match:
                        current_group[field] = match.group(1)
            
            i += 1
        
        # 添加最后一个组合
        if current_group:
            groups.append(current_group)
        
        return groups
    
    def simplify_number_list(self, number_str: str) -> str:
        """简化数字列表显示"""
        if not number_str:
            return "无"
        
        # 提取所有数字
        numbers = re.findall(r'\((\d+)\)', number_str)
        if numbers:
            numbers = [int(n) for n in numbers]
            count = len(numbers)
            if count > 0:
                return f"共{count}个数字 ({numbers[0]}-{numbers[-1]})"
        
        return number_str
    
    def format_group(self, group: Dict[str, Any]) -> str:
        """格式化单个组合"""
        lines = []
        
        # 账户信息
        account1 = group.get('account1', '').strip()
        account2 = group.get('account2', '').strip()
        if account1 and account2:
            lines.append(f"账户: {account1} ↔ {account2}")
        
        # 基本统计信息
        total_digits = group.get('total_digits', '0')
        total_skip = group.get('total_skip', '0.00')
        match_rate = group.get('match_rate', '0.00')
        
        lines.append(f"📊 总数字数: {total_digits}")
        lines.append(f"💰 总跳过金额: {total_skip}元")
        lines.append(f"🎯 全部匹配度: {match_rate}%")
        
        # 跳过内容
        skip_content = group.get('skip_content', '')
        simplified_skip = self.simplify_number_list(skip_content)
        lines.append(f"📝 跳过内容: {simplified_skip}")
        
        # 绝对内容
        absolute_content = group.get('absolute_content', '')
        simplified_absolute = self.simplify_number_list(absolute_content)
        lines.append(f"📋 绝对内容: {simplified_absolute}")
        
        return '\n'.join(lines)
    
    def format_all_groups(self, content: str, lottery_type: str = "未知彩种") -> str:
        """格式化所有组合"""
        groups = self.parse_content(content)
        
        if not groups:
            return "未找到有效的组合数据"
        
        result = [f"🎲 彩种: {lottery_type} (发现{len(groups)}组)\n"]
        
        for i, group in enumerate(groups, 1):
            result.append(f"\n组合 {i}")
            result.append(self.format_group(group))
        
        return '\n'.join(result)

def auto_detect_lottery_type(content: str) -> str:
    """自动检测彩种类型"""
    content_lower = content.lower()
    
    if '快三' in content_lower:
        return '大发快三'
    elif 'pk10' in content_lower or '赛车' in content_lower:
        return 'PK10'
    elif '3d' in content_lower or '排三' in content_lower:
        return '3D/排三'
    elif '双色球' in content_lower:
        return '双色球'
    elif '大乐透' in content_lower:
        return '大乐透'
    else:
        return '未知彩种'

def process_uploaded_content(file_content: str) -> str:
    """处理上传的文件内容"""
    formatter = AccountGroupFormatter()
    
    # 自动检测彩种类型
    lottery_type = auto_detect_lottery_type(file_content)
    
    # 格式化内容
    formatted_content = formatter.format_all_groups(file_content, lottery_type)
    
    return formatted_content

# 使用示例
if __name__ == "__main__":
    # 示例内容（您上传的内容）
    sample_content = """# 2个账号组合 (共2组)

## 组合1
账户：h1857625635 - dhy20

## 总数字数: 49

## 总跳过金额: 0.00元

## 全部匹配度: 0.00/%  
h1857625635: 0个数字 | 总跳过: 0.00元 | 平均编号: 0.00元  

## 跳过内容: (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16), (17), (18), (19), (20), (21), (22), (23), (24), (25), (26), (27), (28), (29), (30), (31), (32), (33), (34), (35), (36), (37), (38), (39), (40), (41), (42), (43)  

## 绝对内容: (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16), (17), (18), (19), (20), (21), (22), (23), (24), (25), (26), (27), (28), (29), (30), (31), (32), (33), (34), (35), (36), (37), (38), (39), (40), (41), (42), (43)  

## 组合2
账户：h1857625635 - 13866605165

## 总数字数: 49

## 总跳过金额: 0.00元

## 全部匹配度: 0.00/%  
h1857625635: 0个数字 | 总跳过: 0.00元 | 平均编号: 0.00元  

## 跳过内容: (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16), (17), (18), (19), (20), (21), (22), (23), (24), (25), (26), (27), (28), (29), (30), (31), (32), (33), (34), (35), (36), (37), (38), (39), (40), (41), (42), (43)  

## 1:386663558: 0个数字 | 总跳过: 0.00元 | 平均编号: 0.00元  

## 跳过内容: (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16), (17), (18), (19), (20), (21), (22), (23), (24), (25), (26), (27), (28), (29), (30), (31), (32), (33), (34), (35), (36), (37), (38), (39), (40), (41), (42), (43)  """

    # 处理内容
    result = process_uploaded_content(sample_content)
    print(result)
