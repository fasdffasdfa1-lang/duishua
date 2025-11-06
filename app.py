import pandas as pd
import numpy as np
import streamlit as st
import io
import re
import logging
from collections import defaultdict
from datetime import datetime
from itertools import combinations
import warnings
import traceback
import hashlib
from functools import lru_cache

# 配置日志和警告
warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('MultiAccountWashTrade')

# Streamlit 页面配置
st.set_page_config(
    page_title="智能多账户对刷检测系统",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== 从第一套代码移植的配置 ====================
LOTTERY_CONFIGS = {
    'PK10': {
        'lotteries': [
            '分分PK拾', '三分PK拾', '五分PK拾', '新幸运飞艇', '澳洲幸运10',
            '一分PK10', '宾果PK10', '极速飞艇', '澳洲飞艇', '幸运赛车',
            '分分赛车', '北京PK10', '旧北京PK10', '极速赛车', '幸运赛車', 
            '北京赛车', '极速PK10', '幸运PK10', '赛车', '赛車'
        ],
        'min_number': 1,
        'max_number': 10,
        'gyh_min': 3,
        'gyh_max': 19,
        'position_names': ['冠军', '亚军', '第三名', '第四名', '第五名', 
                          '第六名', '第七名', '第八名', '第九名', '第十名']
    },
    'K3': {
        'lotteries': [
            '分分快三', '三分快3', '五分快3', '澳洲快三', '宾果快三',
            '1分快三', '3分快三', '5分快三', '10分快三', '加州快三',
            '幸运快三', '大发快三', '快三', '快3', 'k3', 'k三', 
            '澳门快三', '香港快三', '江苏快三'
        ],
        'min_number': 1,
        'max_number': 6,
        'hezhi_min': 3,
        'hezhi_max': 18
    },
    'LHC': {
        'lotteries': [
            '新澳门六合彩', '澳门六合彩', '香港六合彩', '一分六合彩',
            '五分六合彩', '三分六合彩', '香港⑥合彩', '分分六合彩',
            '快乐6合彩', '港⑥合彩', '台湾大乐透', '六合', 'lhc', '六合彩',
            '⑥合', '6合', '大发六合彩'
        ],
        'min_number': 1,
        'max_number': 49
    },
    'SSC': {
        'lotteries': [
            '分分时时彩', '三分时时彩', '五分时时彩', '宾果时时彩',
            '1分时时彩', '3分时时彩', '5分时时彩', '旧重庆时时彩',
            '幸运时时彩', '腾讯分分彩', '新疆时时彩', '天津时时彩',
            '重庆时时彩', '上海时时彩', '广东时时彩', '分分彩', '时时彩', '時時彩'
        ],
        'min_number': 0,
        'max_number': 9
    },
    'THREE_COLOR': {
        'lotteries': [
            '一分三色彩', '30秒三色彩', '五分三色彩', '三分三色彩',
            '三色', '三色彩', '三色球'
        ],
        'min_number': 0,
        'max_number': 9
    }
}

class Config:
    """配置参数类 - 增强版"""
    def __init__(self):
        self.min_amount = 10
        self.amount_similarity_threshold = 0.9
        self.min_continuous_periods = 3
        self.max_accounts_in_group = 5
        self.supported_file_types = ['.xlsx', '.xls', '.csv']
        
        # 增强的列名映射配置 - 从第一套代码移植
        self.column_mappings = {
            '会员账号': ['会员账号', '会员账户', '账号', '账户', '用户账号', '玩家账号', '用户ID', '玩家ID'],
            '彩种': ['彩种', '彩神', '彩票种类', '游戏类型', '彩票类型', '游戏彩种', '彩票名称'],
            '期号': ['期号', '期数', '期次', '期', '奖期', '期号信息', '期号编号'],
            '玩法': ['玩法', '玩法分类', '投注类型', '类型', '投注玩法', '玩法类型', '分类'],
            '内容': ['内容', '投注内容', '下注内容', '注单内容', '投注号码', '号码内容', '投注信息'],
            '金额': ['金额', '下注总额', '投注金额', '总额', '下注金额', '投注额', '金额数值']
        }
        
        # 修正：根据账户总投注期数设置不同的对刷期数阈值
        self.period_thresholds = {
            'low_activity': 10,        # 低活跃度账户阈值（总投注期数≤10）
            'medium_activity_low': 11,  # 中活跃度下限（总投注期数11-200）
            'medium_activity_high': 200, # 中活跃度上限
            'min_periods_low': 3,       # 低活跃度账户最小对刷期数
            'min_periods_medium': 5,    # 中活跃度账户最小对刷期数
            'min_periods_high': 8       # 高活跃度账户最小对刷期数
        }
        
        # 扩展：增加龙虎方向模式
        self.direction_patterns = {
            '小': ['两面-小', '和值-小', '小', 'small', 'xia'],
            '大': ['两面-大', '和值-大', '大', 'big', 'da'], 
            '单': ['两面-单', '和值-单', '单', 'odd', 'dan'],
            '双': ['两面-双', '和值-双', '双', 'even', 'shuang'],
            '龙': ['龙', 'long', '龍', 'dragon'],
            '虎': ['虎', 'hu', 'tiger']
        }
        
        # 扩展：增加龙虎对立组
        self.opposite_groups = [{'大', '小'}, {'单', '双'}, {'龙', '虎'}]

# ==================== 从第一套代码移植的数据处理器 ====================
class DataProcessor:
    def __init__(self):
        self.required_columns = ['会员账号', '彩种', '期号', '玩法', '内容', '金额']
        self.column_mapping = {
            '会员账号': ['会员账号', '会员账户', '账号', '账户', '用户账号', '玩家账号', '用户ID', '玩家ID'],
            '彩种': ['彩种', '彩神', '彩票种类', '游戏类型', '彩票类型', '游戏彩种', '彩票名称'],
            '期号': ['期号', '期数', '期次', '期', '奖期', '期号信息', '期号编号'],
            '玩法': ['玩法', '玩法分类', '投注类型', '类型', '投注玩法', '玩法类型', '分类'],
            '内容': ['内容', '投注内容', '下注内容', '注单内容', '投注号码', '号码内容', '投注信息'],
            '金额': ['金额', '下注总额', '投注金额', '总额', '下注金额', '投注额', '金额数值']
        }
    
    def smart_column_identification(self, df_columns):
        """智能列识别 - 从第一套代码移植"""
        identified_columns = {}
        actual_columns = [str(col).strip() for col in df_columns]
        
        with st.expander("🔍 列名识别详情", expanded=False):
            st.info(f"检测到的列名: {actual_columns}")
            
            for standard_col, possible_names in self.column_mapping.items():
                found = False
                for actual_col in actual_columns:
                    actual_col_lower = actual_col.lower().replace(' ', '').replace('_', '').replace('-', '')
                    
                    for possible_name in possible_names:
                        possible_name_lower = possible_name.lower().replace(' ', '').replace('_', '').replace('-', '')
                        
                        # 增强会员账号识别
                        if standard_col == '会员账号':
                            # 更宽松的匹配规则
                            account_keywords = ['会员', '账号', '账户', '用户', '玩家', 'id']
                            if any(keyword in actual_col_lower for keyword in account_keywords):
                                identified_columns[actual_col] = standard_col
                                st.success(f"✅ 识别列名: {actual_col} -> {standard_col}")
                                found = True
                                break
                        else:
                            # 其他列的原有匹配逻辑
                            if (possible_name_lower in actual_col_lower or 
                                actual_col_lower in possible_name_lower or
                                len(set(possible_name_lower) & set(actual_col_lower)) / len(possible_name_lower) > 0.7):
                                identified_columns[actual_col] = standard_col
                                st.success(f"✅ 识别列名: {actual_col} -> {standard_col}")
                                found = True
                                break
                    
                    if found:
                        break
                
                if not found:
                    st.warning(f"⚠️ 未识别到 {standard_col} 对应的列名")
        
        return identified_columns
    
    def find_data_start(self, df):
        """智能找到数据起始位置"""
        for row_idx in range(min(20, len(df))):
            for col_idx in range(min(10, len(df.columns))):
                cell_value = str(df.iloc[row_idx, col_idx])
                if pd.notna(cell_value) and any(keyword in cell_value for keyword in ['会员', '账号', '期号', '彩种', '玩法', '内容', '订单', '用户']):
                    return row_idx, col_idx
        return 0, 0
    
    def validate_data_quality(self, df):
        """数据质量验证 - 从第一套代码移植"""
        logger.info("正在进行数据质量验证...")
        issues = []
        
        # 检查必要列
        missing_cols = [col for col in self.required_columns if col not in df.columns]
        if missing_cols:
            issues.append(f"缺少必要列: {missing_cols}")
        
        # 检查空值
        for col in self.required_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    issues.append(f"列 '{col}' 有 {null_count} 个空值")
        
        # 特别检查会员账号的完整性
        if '会员账号' in df.columns:
            # 检查是否有被截断的账号
            truncated_accounts = df[df['会员账号'].str.contains(r'\.\.\.|…', na=False)]
            if len(truncated_accounts) > 0:
                issues.append(f"发现 {len(truncated_accounts)} 个可能被截断的会员账号")
            
            # 检查账号长度异常的情况
            account_lengths = df['会员账号'].str.len()
            if account_lengths.max() > 50:  # 假设正常账号长度不超过50个字符
                issues.append("发现异常长度的会员账号")
            
            # 显示账号格式样本
            unique_accounts = df['会员账号'].unique()[:5]
            sample_info = " | ".join([f"'{acc}'" for acc in unique_accounts])
            st.info(f"会员账号格式样本: {sample_info}")
        
        # 检查数据类型
        if '期号' in df.columns:
            # 修复期号格式问题：去掉.0
            df['期号'] = df['期号'].astype(str).str.replace(r'\.0$', '', regex=True)
            # 允许期号包含字母和数字
            invalid_periods = df[~df['期号'].str.match(r'^[\dA-Za-z]+$')]
            if len(invalid_periods) > 0:
                issues.append(f"发现 {len(invalid_periods)} 条无效期号记录")
        
        # 检查重复数据
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            issues.append(f"发现 {duplicate_count} 条重复记录")
        
        if issues:
            with st.expander("⚠️ 数据质量问题", expanded=True):
                for issue in issues:
                    st.warning(f"  - {issue}")
        else:
            st.success("✅ 数据质量检查通过")
        
        return issues
    
    def clean_data(self, uploaded_file):
        """数据清洗主函数 - 从第一套代码移植并改进"""
        try:
            # 第一次读取用于定位
            df_temp = pd.read_excel(uploaded_file, header=None, nrows=50)
            st.info(f"原始数据维度: {df_temp.shape}")
            
            # 找到数据起始位置
            start_row, start_col = self.find_data_start(df_temp)
            st.info(f"数据起始位置: 第{start_row+1}行, 第{start_col+1}列")
            
            # 重新读取数据 - 特别处理常规格式单元格
            df_clean = pd.read_excel(
                uploaded_file, 
                header=start_row,
                skiprows=range(start_row + 1) if start_row > 0 else None,
                dtype=str,  # 将所有列读取为字符串
                na_filter=False,  # 不过滤空值
                keep_default_na=False,  # 不使用默认的NA值处理
                converters={}  # 为空，让pandas不要进行任何转换
            )
            
            # 删除起始列之前的所有列
            if start_col > 0:
                df_clean = df_clean.iloc[:, start_col:]
            
            st.info(f"清理后数据维度: {df_clean.shape}")
            
            # 智能列识别
            column_mapping = self.smart_column_identification(df_clean.columns)
            if column_mapping:
                df_clean = df_clean.rename(columns=column_mapping)
                st.success("✅ 列名识别完成!")
                for old_col, new_col in column_mapping.items():
                    logger.info(f"  {old_col} -> {new_col}")
            
            # 确保必要列存在
            missing_columns = [col for col in self.required_columns if col not in df_clean.columns]
            if missing_columns and len(df_clean.columns) >= 4:
                st.warning("自动映射列名...")
                manual_mapping = {}
                col_names = ['会员账号', '彩种', '期号', '内容', '玩法', '金额']
                for i, col_name in enumerate(col_names):
                    if i < len(df_clean.columns):
                        manual_mapping[df_clean.columns[i]] = col_name
                
                df_clean = df_clean.rename(columns=manual_mapping)
                st.info(f"手动重命名后的列: {list(df_clean.columns)}")
            
            # 数据清理
            initial_count = len(df_clean)
            df_clean = df_clean.dropna(subset=[col for col in self.required_columns if col in df_clean.columns])
            df_clean = df_clean.dropna(axis=1, how='all')
            
            # 数据类型转换 - 特别小心处理会员账号
            for col in self.required_columns:
                if col in df_clean.columns:
                    if col == '会员账号':
                        # 特别处理会员账号：确保不丢失任何字符
                        df_clean[col] = df_clean[col].apply(
                            lambda x: str(x) if pd.notna(x) else ''
                        )
                    else:
                        df_clean[col] = df_clean[col].astype(str).str.strip()
            
            # 修复期号格式：去掉.0
            if '期号' in df_clean.columns:
                df_clean['期号'] = df_clean['期号'].str.replace(r'\.0$', '', regex=True)
            
            # 数据质量验证
            self.validate_data_quality(df_clean)
            
            st.success(f"✅ 数据清洗完成: {initial_count} -> {len(df_clean)} 条记录")
            
            # 显示统计信息
            st.info(f"📊 唯一会员账号数: {df_clean['会员账号'].nunique()}")
            
            # 彩种分布显示
            if '彩种' in df_clean.columns:
                lottery_dist = df_clean['彩种'].value_counts()
                with st.expander("🎯 彩种分布", expanded=False):
                    st.dataframe(lottery_dist.reset_index().rename(columns={'index': '彩种', '彩种': '数量'}))
            
            return df_clean
            
        except Exception as e:
            st.error(f"❌ 数据清洗失败: {str(e)}")
            logger.error(f"数据清洗失败: {str(e)}")
            return None

    def debug_account_issues(self, df):
        """调试会员账号问题 - 从第一套代码移植"""
        st.subheader("🔍 会员账号调试信息")
        
        if '会员账号' not in df.columns:
            st.error("未找到会员账号列")
            return
        
        # 显示账号统计信息
        st.write("### 账号统计")
        st.write(f"总记录数: {len(df)}")
        st.write(f"唯一账号数: {df['会员账号'].nunique()}")
        
        # 显示账号长度分布
        df['账号长度'] = df['会员账号'].str.len()
        length_stats = df['账号长度'].describe()
        st.write("### 账号长度统计")
        st.write(length_stats)
        
        # 显示可能的问题账号
        st.write("### 可能的问题账号")
        
        # 查找非常短的账号（可能被截断）
        short_accounts = df[df['账号长度'] < 3]['会员账号'].unique()
        if len(short_accounts) > 0:
            st.warning(f"发现 {len(short_accounts)} 个过短的账号: {list(short_accounts)}")
        
        # 查找包含特殊截断符号的账号
        truncated_patterns = [r'\.\.\.', r'…', r'\.$', r'_\d+$']
        for pattern in truncated_patterns:
            truncated = df[df['会员账号'].str.contains(pattern, na=False)]['会员账号'].unique()
            if len(truncated) > 0:
                st.warning(f"发现 {len(truncated)} 个可能被截断的账号（模式: {pattern}）: {list(truncated)}")
        
        # 查找包含下划线的账号（如 _551531wxh_）
        underscore_accounts = df[df['会员账号'].str.contains('_', na=False)]['会员账号'].unique()
        if len(underscore_accounts) > 0:
            st.info(f"发现 {len(underscore_accounts)} 个包含下划线的账号:")
            for account in underscore_accounts:
                # 使用Markdown转义来确保下划线正确显示
                account_display = account.replace('_', '\\_')  # 转义下划线
                st.markdown(f"- `{account_display}` (长度: {len(account)}, 显示: '{account}')")
        
        # 显示前30个账号样本 - 使用Markdown格式确保正确显示
        st.write("### 账号样本（前30个）")
        sample_accounts = df['会员账号'].head(30).tolist()
        for i, account in enumerate(sample_accounts, 1):
            # 使用Markdown格式显示账号，确保特殊字符正确显示
            account_display = account.replace('_', '\\_')  # 转义下划线
            st.markdown(f"{i:2d}. `{account_display}` (长度: {len(account)})")
        
        # 显示数据类型的详细信息
        st.write("### 数据类型信息")
        st.write(f"会员账号列的数据类型: {df['会员账号'].dtype}")
        
        # 显示包含特殊字符的账号
        st.write("### 包含特殊字符的账号")
        special_chars = ['_', '-', '.', '@', '#', '$', '%', '&', '*']
        for char in special_chars:
            special_accounts = df[df['会员账号'].str.contains(char, na=False, regex=False)]['会员账号'].unique()
            if len(special_accounts) > 0:
                st.write(f"包含 '{char}' 的账号 ({len(special_accounts)}个):")
                for account in special_accounts[:10]:
                    st.code(f"  {account}")
                if len(special_accounts) > 10:
                    st.write(f"  ... 还有 {len(special_accounts) - 10} 个")

# ==================== 增强的彩种识别器 ====================
class EnhancedLotteryIdentifier:
    def __init__(self):
        self.lottery_configs = LOTTERY_CONFIGS
        self.unknown_lottery_patterns = {}
        self.identified_unknown_lotteries = {}
        
    def identify_lottery_type(self, lottery_name):
        """增强的彩种识别 - 包含未知彩种的智能识别和记录"""
        lottery_str = str(lottery_name).strip()
        
        # 1. 首先尝试标准识别
        for lottery_type, config in self.lottery_configs.items():
            for lottery in config['lotteries']:
                if lottery in lottery_str:
                    return lottery_type
        
        lottery_lower = lottery_str.lower()
        
        # 2. 关键词识别
        if any(word in lottery_lower for word in ['pk', '飞艇', '赛车', '幸运10', 'pk10', 'pk拾', '赛車']):
            return 'PK10'
        elif any(word in lottery_lower for word in ['快三', '快3', 'k3', 'k三']):
            return 'K3'
        elif any(word in lottery_lower for word in ['六合', 'lhc', '六合彩', '⑥合', '6合']):
            return 'LHC'
        elif any(word in lottery_lower for word in ['时时彩', 'ssc', '分分彩', '时时彩', '時時彩']):
            return 'SSC'
        elif any(word in lottery_lower for word in ['三色', '三色彩', '三色球']):
            return 'THREE_COLOR'
        
        # 3. 智能识别未知彩种
        return self.smart_identify_unknown_lottery(lottery_str)
    
    def smart_identify_unknown_lottery(self, lottery_name):
        """智能识别未知彩种并记录模式"""
        lottery_str = str(lottery_name).strip()
        
        # 记录未知彩种
        if lottery_str not in self.identified_unknown_lotteries:
            self.identified_unknown_lotteries[lottery_str] = {
                'count': 0,
                'first_seen': datetime.now(),
                'patterns': set(),
                'inferred_type': None
            }
        
        self.identified_unknown_lotteries[lottery_str]['count'] += 1
        
        # 基于玩法模式推断彩种类型
        inferred_type = self.infer_lottery_type_by_patterns(lottery_str)
        if inferred_type:
            self.identified_unknown_lotteries[lottery_str]['inferred_type'] = inferred_type
            return inferred_type
        
        # 如果无法推断，标记为未知但记录特征
        return '未知彩种'
    
    def infer_lottery_type_by_patterns(self, lottery_name):
        """基于玩法模式推断彩种类型"""
        lottery_lower = lottery_name.lower()
        
        # 基于开奖号码特征推断
        number_patterns = {
            'PK10': [r'1[0-9]选1', r'冠亚', r'前[一二三]', r'第[1-9]名', r'定位胆.*[1-9]'],
            'K3': [r'和值', r'三军', r'独胆', r'二不同', r'三不同'],
            'SSC': [r'第[1-5]球', r'定位胆', r'万位', r'千位', r'百位', r'十位', r'个位'],
            'LHC': [r'特码', r'正码', r'平特', r'特肖', r'连肖', r'尾数', r'色波'],
            'THREE_COLOR': [r'三色', r'红蓝绿', r'三色彩']
        }
        
        for lottery_type, patterns in number_patterns.items():
            for pattern in patterns:
                if re.search(pattern, lottery_lower):
                    return lottery_type
        
        # 基于开奖时间模式推断
        time_patterns = {
            'PK10': [r'[135]分', r'极速', r'高频'],
            'SSC': [r'[135]分', r'分分彩', r'时时彩'],
            'K3': [r'[135]分', r'快三'],
            'LHC': [r'[15]分', r'六合彩']
        }
        
        for lottery_type, patterns in time_patterns.items():
            for pattern in patterns:
                if re.search(pattern, lottery_lower):
                    return lottery_type
        
        return None
    
    def record_play_pattern(self, lottery_name, play_category, content):
        """记录未知彩种的玩法模式"""
        if lottery_name not in self.unknown_lottery_patterns:
            self.unknown_lottery_patterns[lottery_name] = {
                'play_categories': set(),
                'content_patterns': set(),
                'sample_contents': []
            }
        
        patterns = self.unknown_lottery_patterns[lottery_name]
        patterns['play_categories'].add(play_category)
        
        # 分析内容模式
        content_str = str(content)
        if len(patterns['sample_contents']) < 10:  # 只保留10个样本
            patterns['sample_contents'].append(content_str)
        
        # 提取数字模式
        number_patterns = re.findall(r'\d+', content_str)
        if number_patterns:
            patterns['content_patterns'].update(number_patterns)
    
    def get_unknown_lottery_stats(self):
        """获取未知彩种统计信息"""
        stats = {
            'total_unknown': len(self.identified_unknown_lotteries),
            'unknown_details': {},
            'recommendations': []
        }
        
        for lottery_name, data in self.identified_unknown_lotteries.items():
            stats['unknown_details'][lottery_name] = {
                'count': data['count'],
                'inferred_type': data['inferred_type'],
                'first_seen': data['first_seen'].strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # 如果这个未知彩种出现频率高，生成配置建议
            if data['count'] >= 10 and lottery_name in self.unknown_lottery_patterns:
                patterns = self.unknown_lottery_patterns[lottery_name]
                inferred_type = data['inferred_type'] or '未知类型'
                
                recommendation = {
                    'lottery_name': lottery_name,
                    'count': data['count'],
                    'inferred_type': inferred_type,
                    'play_categories': list(patterns['play_categories']),
                    'sample_patterns': list(patterns['content_patterns'])[:5]
                }
                stats['recommendations'].append(recommendation)
        
        return stats

# ==================== 更新数据处理器 ====================
class EnhancedDataProcessor(DataProcessor):
    def __init__(self):
        super().__init__()
        self.lottery_identifier = EnhancedLotteryIdentifier()
    
    def enhance_data_processing(self, df_clean):
        """增强的数据处理流程 - 包含未知彩种识别"""
        try:
            # 1. 彩种识别（包含未知彩种处理）
            if '彩种' in df_clean.columns:
                df_clean['彩种类型'] = df_clean['彩种'].apply(
                    self.lottery_identifier.identify_lottery_type
                )
                
                # 记录未知彩种的玩法模式
                unknown_mask = df_clean['彩种类型'] == '未知彩种'
                if unknown_mask.any():
                    unknown_df = df_clean[unknown_mask]
                    for _, row in unknown_df.iterrows():
                        play_category = row.get('玩法分类', '')
                        content = row.get('内容', '')
                        self.lottery_identifier.record_play_pattern(
                            row['彩种'], play_category, content
                        )
            
            # 2. 玩法分类统一
            if '玩法' in df_clean.columns:
                df_clean['玩法分类'] = df_clean['玩法'].apply(self.play_normalizer.normalize_category)
            
            # 3. 计算账户统计信息
            self.calculate_account_total_periods_by_lottery(df_clean)
            
            # 4. 提取投注金额和方向
            df_clean['投注金额'] = df_clean['金额'].apply(lambda x: self.extract_bet_amount_safe(x))
            df_clean['投注方向'] = df_clean['内容'].apply(lambda x: self.enhanced_extract_direction(x))
            
            # 过滤有效记录
            df_valid = df_clean[
                (df_clean['投注方向'] != '') & 
                (df_clean['投注金额'] >= self.config.min_amount)
            ].copy()
            
            if len(df_valid) == 0:
                st.error("❌ 过滤后没有有效记录")
                return pd.DataFrame()
            
            self.data_processed = True
            self.df_valid = df_valid
            
            # 显示未知彩种统计
            self.display_unknown_lottery_stats()
            
            return df_valid
            
        except Exception as e:
            logger.error(f"数据处理增强失败: {str(e)}")
            st.error(f"数据处理增强失败: {str(e)}")
            return pd.DataFrame()
    
    def display_unknown_lottery_stats(self):
        """显示未知彩种统计信息"""
        stats = self.lottery_identifier.get_unknown_lottery_stats()
        
        if stats['total_unknown'] > 0:
            with st.expander("🔍 未知彩种识别报告", expanded=True):
                st.warning(f"发现 {stats['total_unknown']} 个未知彩种")
                
                # 显示未知彩种详情
                for lottery_name, data in stats['unknown_details'].items():
                    st.write(f"**彩种名称:** {lottery_name}")
                    st.write(f"  - 出现次数: {data['count']}")
                    st.write(f"  - 推断类型: {data['inferred_type'] or '未识别'}")
                    st.write(f"  - 首次出现: {data['first_seen']}")
                
                # 显示配置建议
                if stats['recommendations']:
                    st.subheader("🎯 配置建议")
                    st.info("以下彩种出现频率较高，建议添加到配置中:")
                    
                    for rec in stats['recommendations']:
                        with st.expander(f"建议添加: {rec['lottery_name']} (出现{rec['count']}次)"):
                            st.write(f"**推断类型:** {rec['inferred_type']}")
                            st.write(f"**玩法分类:** {', '.join(rec['play_categories'])}")
                            st.write(f"**内容模式:** {rec['sample_patterns']}")
                            
                            # 生成配置代码建议
                            config_suggestion = self.generate_config_suggestion(rec)
                            st.code(config_suggestion, language='python')
    
    def generate_config_suggestion(self, recommendation):
        """生成配置代码建议"""
        lottery_name = recommendation['lottery_name']
        inferred_type = recommendation['inferred_type']
        
        if inferred_type != '未知类型':
            config_key = inferred_type.upper()
            suggestion = f"""
# 建议添加到 {config_key} 配置中:
LOTTERY_CONFIGS['{config_key}']['lotteries'].append('{lottery_name}')
"""
        else:
            suggestion = f"""
# 建议添加新的彩种配置:
LOTTERY_CONFIGS['NEW_LOTTERY'] = {{
    'lotteries': ['{lottery_name}'],
    # 需要补充其他配置参数...
}}
"""
        return suggestion

# ==================== 更新对刷检测器 ====================
class EnhancedWashTradeDetector(WashTradeDetector):
    def __init__(self, config=None):
        super().__init__(config)
        self.data_processor = EnhancedDataProcessor()  # 使用增强的数据处理器
    
    def upload_and_process(self, uploaded_file):
        """上传并处理文件 - 使用增强的数据处理器"""
        try:
            if uploaded_file is None:
                st.error("❌ 没有上传文件")
                return None, None
            
            filename = uploaded_file.name
            logger.info(f"✅ 已上传文件: {filename}")
            
            if not any(filename.endswith(ext) for ext in self.config.supported_file_types):
                st.error(f"❌ 不支持的文件类型: {filename}")
                return None, None
            
            # 使用增强的数据处理器
            with st.spinner("🔄 正在清洗数据..."):
                df_clean = self.data_processor.clean_data(uploaded_file)
            
            if df_clean is not None and len(df_clean) > 0:
                # 增强的数据处理（包含未知彩种识别）
                df_enhanced = self.data_processor.enhance_data_processing(df_clean)
                return df_enhanced, filename
            else:
                return None, None
            
        except Exception as e:
            logger.error(f"文件处理失败: {str(e)}")
            st.error(f"文件处理失败: {str(e)}")
            return None, None

# ==================== 更新主函数 ====================
def main():
    """主函数"""
    st.title("🎯 智能多账户对刷检测系统 - 增强版")
    st.markdown("---")
    
    # ==================== 左侧边栏 - 文件上传 ====================
    with st.sidebar:
        st.header("📁 数据上传")
        
        uploaded_file = st.file_uploader(
            "请上传数据文件", 
            type=['xlsx', 'xls', 'csv'],
            help="请确保文件包含必要的列：会员账号、期号、内容、金额"
        )
    
    # ==================== 主区域 - 配置和结果显示 ====================
    if uploaded_file is not None:
        try:
            # 配置参数
            st.sidebar.header("⚙️ 检测参数配置")
            
            min_amount = st.sidebar.number_input("最小投注金额", value=10, min_value=1, help="低于此金额的记录将被过滤")
            similarity_threshold = st.sidebar.slider("金额匹配度阈值", 0.8, 1.0, 0.9, 0.01, help="对立方向金额匹配度阈值")
            max_accounts = st.sidebar.slider("最大检测账户数", 2, 8, 5, help="检测的最大账户组合数量")
            
            # 活跃度阈值配置
            st.sidebar.subheader("📊 活跃度阈值配置")
            st.sidebar.markdown("**低活跃度:** 总投注期数≤10期")
            st.sidebar.markdown("**中活跃度:** 总投注期数11-200期")  
            st.sidebar.markdown("**高活跃度:** 总投注期数≥201期")
            
            min_periods_low = st.sidebar.number_input("低活跃度最小对刷期数", value=3, min_value=1, 
                                                    help="总投注期数≤10的账户，要求≥3期连续对刷")
            min_periods_medium = st.sidebar.number_input("中活跃度最小对刷期数", value=5, min_value=1,
                                                       help="总投注期数11-200的账户，要求≥5期连续对刷")
            min_periods_high = st.sidebar.number_input("高活跃度最小对刷期数", value=8, min_value=1,
                                                     help="总投注期数≥201的账户，要求≥8期连续对刷")
            
            # 调试选项
            st.sidebar.subheader("🔧 调试选项")
            debug_mode = st.sidebar.checkbox("启用调试模式", value=False)
            account_debug = st.sidebar.checkbox("启用账号调试", value=False)
            lottery_debug = st.sidebar.checkbox("启用彩种识别调试", value=False)
            
            # 更新配置参数
            config = Config()
            config.min_amount = min_amount
            config.amount_similarity_threshold = similarity_threshold
            config.max_accounts_in_group = max_accounts
            config.period_thresholds = {
                'low_activity': 10,
                'medium_activity_low': 11,  
                'medium_activity_high': 200, 
                'min_periods_low': min_periods_low,
                'min_periods_medium': min_periods_medium,
                'min_periods_high': min_periods_high
            }
            
            # 使用增强的检测器
            detector = EnhancedWashTradeDetector(config)
            
            st.success(f"✅ 已上传文件: {uploaded_file.name}")
            
            # 自动开始处理和分析
            with st.spinner("🔄 正在解析数据..."):
                df_enhanced, filename = detector.upload_and_process(uploaded_file)
                
                if df_enhanced is not None and len(df_enhanced) > 0:
                    st.success("✅ 数据解析完成")
                    
                    # 数据概览
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("有效记录数", f"{len(df_enhanced):,}")
                    with col2:
                        st.metric("唯一期号数", f"{df_enhanced['期号'].nunique():,}")
                    with col3:
                        st.metric("唯一账户数", f"{df_enhanced['会员账号'].nunique():,}")
                    with col4:
                        if '彩种类型' in df_enhanced.columns:
                            known_count = (df_enhanced['彩种类型'] != '未知彩种').sum()
                            unknown_count = (df_enhanced['彩种类型'] == '未知彩种').sum()
                            st.metric("彩种识别", f"{known_count}已知/{unknown_count}未知")
                    
                    # 彩种识别详情
                    if lottery_debug and '彩种类型' in df_enhanced.columns:
                        with st.expander("🎯 彩种识别详情", expanded=False):
                            lottery_stats = df_enhanced['彩种类型'].value_counts()
                            st.write("**彩种类型分布:**")
                            st.dataframe(lottery_stats)
                            
                            # 显示原始彩种名称与识别结果的对应关系
                            if '彩种' in df_enhanced.columns:
                                cross_tab = pd.crosstab(df_enhanced['彩种'], df_enhanced['彩种类型'])
                                st.write("**原始彩种名称与识别结果对应关系:**")
                                st.dataframe(cross_tab)
                    
                    # 数据详情 - 默认折叠
                    with st.expander("📊 数据详情", expanded=False):
                        tab1, tab2, tab3 = st.tabs(["数据概览", "彩种分布", "玩法分布"])
                        
                        with tab1:
                            st.dataframe(df_enhanced.head(100), use_container_width=True)
                            
                        with tab2:
                            if '彩种类型' in df_enhanced.columns:
                                lottery_type_stats = df_enhanced['彩种类型'].value_counts()
                                st.bar_chart(lottery_type_stats)
                                st.dataframe(lottery_type_stats.reset_index().rename(
                                    columns={'index': '彩种类型', '彩种类型': '数量'}
                                ))
                        
                        with tab3:
                            if '玩法分类' in df_enhanced.columns:
                                play_stats = df_enhanced['玩法分类'].value_counts().head(15)
                                st.bar_chart(play_stats)
                                st.dataframe(play_stats.reset_index().rename(
                                    columns={'index': '玩法分类', '玩法分类': '数量'}
                                ))
                    
                    # 如果启用了账号调试，显示调试信息
                    if account_debug:
                        with st.expander("🔍 账号调试信息", expanded=False):
                            detector.data_processor.debug_account_issues(df_enhanced)
                    
                    # 自动开始检测
                    st.info("🚀 自动开始检测对刷交易...")
                    with st.spinner("🔍 正在检测对刷交易..."):
                        patterns = detector.detect_all_wash_trades()
                    
                    if patterns:
                        st.success(f"✅ 检测完成！发现 {len(patterns)} 个对刷组")
                        
                        detector.display_detailed_results(patterns)
                        
                        excel_output, export_filename = detector.export_to_excel(patterns, filename)
                        
                        if excel_output is not None:
                            st.download_button(
                                label="📥 下载检测报告",
                                data=excel_output,
                                file_name=export_filename,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True
                            )
                    else:
                        st.warning("⚠️ 未发现符合阈值条件的对刷行为")
                else:
                    st.error("❌ 数据解析失败，请检查文件格式和内容")
            
        except Exception as e:
            st.error(f"❌ 程序执行失败: {str(e)}")
            st.error(f"详细错误信息:\n{traceback.format_exc()}")
    else:
        # 未上传文件时的欢迎界面
        st.info("👈 请在左侧边栏上传数据文件开始分析")
        
        # 功能特色介绍
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.subheader("🔍 智能检测")
            st.markdown("""
            - 多账户对刷模式识别
            - **智能彩种识别**
            - 智能金额匹配分析
            - 活跃度自适应阈值
            """)
        
        with col2:
            st.subheader("📊 专业分析")
            st.markdown("""
            - **未知彩种自动学习**
            - 玩法分类标准化
            - 数据质量验证
            - 详细统计报告
            """)
        
        with col3:
            st.subheader("🚀 高效处理")
            st.markdown("""
            - 大数据量优化
            - 并行检测算法
            - 一键导出结果
            - **配置建议生成**
            """)
    
    # 使用说明
    with st.expander("📖 系统使用说明 - 增强版", expanded=False):
        st.markdown("""
        ### 系统功能说明 - 增强版

        **🎯 智能彩种识别:**
        - **自动识别**: 支持PK10、K3、六合彩、时时彩等主流彩种
        - **未知彩种学习**: 自动识别未知彩种并记录特征模式
        - **智能推断**: 基于玩法模式和时间特征推断彩种类型
        - **配置建议**: 为高频未知彩种生成配置代码建议

        **📊 检测逻辑：**
        - **总投注期数**：账户在特定彩种中的所有期号投注次数
        - **对刷期数**：账户组实际发生对刷行为的期数
        - 根据**总投注期数**判定账户活跃度，设置不同的**对刷期数**阈值

        **📈 活跃度判定：**
        - **低活跃度账户**：总投注期数 ≤ 10期 → 要求 ≥ 3期连续对刷
        - **中活跃度账户**：总投注期数 11-200期 → 要求 ≥ 5期连续对刷  
        - **高活跃度账户**：总投注期数 ≥ 201期 → 要求 ≥ 8期连续对刷

        **🎯 对刷检测规则：**
        - 检测2-5个账户之间的对刷行为
        - **支持的对立投注类型：**
          - 大 vs 小
          - 单 vs 双  
          - 龙 vs 虎
        - 金额匹配度 ≥ 90%
        - 排除同一账户多方向下注

        **🔧 调试功能:**
        - 账号调试：分析账号格式和可能的问题
        - 彩种识别调试：查看彩种识别详情和对应关系
        - 未知彩种报告：自动生成配置建议
        """)

if __name__ == "__main__":
    main()
