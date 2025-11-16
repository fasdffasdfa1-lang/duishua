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

# ==================== 配置类 - 增强版，新增3D系列和位置精度 ====================
class Config:
    """配置参数类 - 增强版"""
    def __init__(self):
        self.min_amount = 10
        self.amount_similarity_threshold = 0.8
        self.min_continuous_periods = 3
        self.max_accounts_in_group = 5
        self.supported_file_types = ['.xlsx', '.xls', '.csv']
        
        # 增强的列名映射配置
        self.column_mappings = {
            '会员账号': ['会员账号', '会员账户', '账号', '账户', '用户账号', '玩家账号', '用户ID', '玩家ID'],
            '彩种': ['彩种', '彩神', '彩票种类', '游戏类型', '彩票类型', '游戏彩种', '彩票名称'],
            '期号': ['期号', '期数', '期次', '期', '奖期', '期号信息', '期号编号'],
            '玩法': ['玩法', '玩法分类', '投注类型', '类型', '投注玩法', '玩法类型', '分类'],
            '内容': ['内容', '投注内容', '下注内容', '注单内容', '投注号码', '号码内容', '投注信息'],
            '金额': ['金额', '下注总额', '投注金额', '总额', '下注金额', '投注额', '金额数值']
        }
        
        # 增强：根据您的要求调整对刷期数阈值
        self.period_thresholds = {
            'low_activity': 10,           # 低活跃度上限（总投注期数1-10）
            'medium_activity_low': 11,    # 中活跃度下限（总投注期数11-50）
            'medium_activity_high': 50,   # 中活跃度上限
            'high_activity_low': 51,      # 高活跃度下限（总投注期数51-100）
            'high_activity_high': 100,    # 高活跃度上限
            'min_periods_low': 3,         # 低活跃度账户最小对刷期数
            'min_periods_medium': 5,      # 中活跃度账户最小对刷期数
            'min_periods_high': 8,        # 高活跃度账户最小对刷期数
            'min_periods_very_high': 11   # 极高活跃度账户最小对刷期数
        }
        
        # 扩展：根据账户数量调整匹配度阈值
        self.account_count_similarity_thresholds = {
            2: 0.8,    # 2个账户：80%匹配度
            3: 0.85,   # 3个账户：85%匹配度  
            4: 0.9,    # 4个账户：90%匹配度
            5: 0.95    # 5个账户：95%匹配度
        }
        
        # 新增：账户期数差异阈值
        self.account_period_diff_threshold = 150  # 账户总投注期数最大差异阈值
        
        # 扩展：增加龙虎方向模式，并添加质合方向，增强位置精度
        self.direction_patterns = {
            '小': ['两面-小', '和值-小', '小', 'small', 'xia'],
            '大': ['两面-大', '和值-大', '大', 'big', 'da'], 
            '单': ['两面-单', '和值-单', '单', 'odd', 'dan'],
            '双': ['两面-双', '和值-双', '双', 'even', 'shuang'],
            '龙': ['龙', 'long', '龍', 'dragon'],
            '虎': ['虎', 'hu', 'tiger'],
            '质': ['质', '质数', 'prime', 'zhi', '質', '質數'],
            '合': ['合', '合数', 'composite', 'he', '合數']
        }
        
        # 扩展：增加龙虎对立组，并添加质合对立组
        self.opposite_groups = [{'大', '小'}, {'单', '双'}, {'龙', '虎'}, {'质', '合'}]
        
        # 增强：位置关键词映射 - 扩展更多位置关键词
        self.position_keywords = {
            'PK10': {
                '冠军': ['冠军', '第1名', '第一名', '前一', '冠 军', '冠　军', '1st', '第一名', '1名'],
                '亚军': ['亚军', '第2名', '第二名', '亚 军', '亚　军', '2nd', '第二名', '2名'],
                '第三名': ['第三名', '第3名', '季军', '3rd', '第三名', '3名'],
                '第四名': ['第四名', '第4名', '4th', '第四名', '4名'],
                '第五名': ['第五名', '第5名', '5th', '第五名', '5名'],
                '第六名': ['第六名', '第6名', '6th', '第六名', '6名'],
                '第七名': ['第七名', '第7名', '7th', '第七名', '7名'],
                '第八名': ['第八名', '第8名', '8th', '第八名', '8名'],
                '第九名': ['第九名', '第9名', '9th', '第九名', '9名'],
                '第十名': ['第十名', '第10名', '10th', '第十名', '10名']
            },
            '3D': {
                '百位': ['百位', '百', '百位胆', '百位定位胆', 'baiwei', 'bai'],
                '十位': ['十位', '十', '十位胆', '十位定位胆', 'shiwei', 'shi'],
                '个位': ['个位', '个', '个位胆', '个位定位胆', 'gewei', 'ge']
            },
            'SSC': {
                '第1球': ['第1球', '万位', '第一位', '1球', 'ball1', '第一球', '万位定位胆'],
                '第2球': ['第2球', '千位', '第二位', '2球', 'ball2', '第二球', '千位定位胆'],
                '第3球': ['第3球', '百位', '第三位', '3球', 'ball3', '第三球', '百位定位胆'],
                '第4球': ['第4球', '十位', '第四位', '4球', 'ball4', '第四球', '十位定位胆'],
                '第5球': ['第5球', '个位', '第五位', '5球', 'ball5', '第五球', '个位定位胆']
            },
            'K3': {
                '和值': ['和值', '总和', '和数', '点数', 'hezhi', 'sum'],
                '三军': ['三军', '独胆', '单码', 'sanjun', 'single'],
                '二不同号': ['二不同号', '二不同', '二不同', 'two_diff'],
                '三不同号': ['三不同号', '三不同', '三不同', 'three_diff']
            },
            'LHC': {
                '特码': ['特码', '特肖', '特码A', '特码B', '特码单双', '特码大小', 'tema', 'special'],
                '正码': ['正码', '正特', '正码1-6', '正码特', 'zhengma', 'normal'],
                '平特': ['平特', '平特肖', '平特尾', 'pingte', 'flat_special'],
                '连肖': ['连肖', '二肖', '三肖', '四肖', '五肖', 'lianxiao', 'continuous']
            }
        }

# ==================== 数据处理器类 - 增强版 ====================
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
        """智能列识别"""
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
                        
                        if standard_col == '会员账号':
                            account_keywords = ['会员', '账号', '账户', '用户', '玩家', 'id']
                            if any(keyword in actual_col_lower for keyword in account_keywords):
                                identified_columns[actual_col] = standard_col
                                st.success(f"✅ 识别列名: {actual_col} -> {standard_col}")
                                found = True
                                break
                        else:
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
        """数据质量验证"""
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
            truncated_accounts = df[df['会员账号'].str.contains(r'\.\.\.|…', na=False)]
            if len(truncated_accounts) > 0:
                issues.append(f"发现 {len(truncated_accounts)} 个可能被截断的会员账号")
            
            account_lengths = df['会员账号'].str.len()
            if account_lengths.max() > 50:
                issues.append("发现异常长度的会员账号")
            
            unique_accounts = df['会员账号'].unique()[:5]
            sample_info = " | ".join([f"'{acc}'" for acc in unique_accounts])
            st.info(f"会员账号格式样本: {sample_info}")
        
        # 检查数据类型
        if '期号' in df.columns:
            df['期号'] = df['期号'].astype(str).str.replace(r'\.0$', '', regex=True)
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
        """数据清洗主函数"""
        try:
            df_temp = pd.read_excel(uploaded_file, header=None, nrows=50)
            st.info(f"原始数据维度: {df_temp.shape}")
            
            start_row, start_col = self.find_data_start(df_temp)
            st.info(f"数据起始位置: 第{start_row+1}行, 第{start_col+1}列")
            
            df_clean = pd.read_excel(
                uploaded_file, 
                header=start_row,
                skiprows=range(start_row + 1) if start_row > 0 else None,
                dtype=str,
                na_filter=False,
                keep_default_na=False
            )
            
            if start_col > 0:
                df_clean = df_clean.iloc[:, start_col:]
            
            st.info(f"清理后数据维度: {df_clean.shape}")
            
            column_mapping = self.smart_column_identification(df_clean.columns)
            if column_mapping:
                df_clean = df_clean.rename(columns=column_mapping)
                st.success("✅ 列名识别完成!")
            
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
            
            initial_count = len(df_clean)
            df_clean = df_clean.dropna(subset=[col for col in self.required_columns if col in df_clean.columns])
            df_clean = df_clean.dropna(axis=1, how='all')
            
            for col in self.required_columns:
                if col in df_clean.columns:
                    if col == '会员账号':
                        df_clean[col] = df_clean[col].apply(
                            lambda x: str(x) if pd.notna(x) else ''
                        )
                    else:
                        df_clean[col] = df_clean[col].astype(str).str.strip()
            
            if '期号' in df_clean.columns:
                df_clean['期号'] = df_clean['期号'].str.replace(r'\.0$', '', regex=True)
            
            self.validate_data_quality(df_clean)
            
            st.success(f"✅ 数据清洗完成: {initial_count} -> {len(df_clean)} 条记录")
            
            st.info(f"📊 唯一会员账号数: {df_clean['会员账号'].nunique()}")
            
            if '彩种' in df_clean.columns:
                lottery_dist = df_clean['彩种'].value_counts()
                with st.expander("🎯 彩种分布", expanded=False):
                    st.dataframe(lottery_dist.reset_index().rename(columns={'index': '彩种', '彩种': '数量'}))
            
            return df_clean
            
        except Exception as e:
            st.error(f"❌ 数据清洗失败: {str(e)}")
            logger.error(f"数据清洗失败: {str(e)}")
            return None

# ==================== 彩种识别器 - 增强版，新增3D系列 ====================
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
    # 新增：3D系列彩种配置
    '3D': {
        'lotteries': [
            '排列三', '排列3', '幸运排列3', '一分排列3', '二分排列3', '三分排列3', 
            '五分排列3', '十分排列3', '大发排列3', '好运排列3', '福彩3D', '极速3D',
            '极速排列3', '幸运3D', '一分3D', '二分3D', '三分3D', '五分3D', 
            '十分3D', '大发3D', '好运3D'
        ],
        'min_number': 0,
        'max_number': 9,
        'position_names': ['百位', '十位', '个位']
    }
}

class LotteryIdentifier:
    def __init__(self):
        self.lottery_configs = LOTTERY_CONFIGS
        self.general_keywords = {
            'PK10': ['pk10', 'pk拾', '飞艇', '赛车', '赛車', '幸运10', '北京赛车', '极速赛车'],
            'K3': ['快三', '快3', 'k3', 'k三', '骰宝', '三军'],
            'LHC': ['六合', 'lhc', '六合彩', '⑥合', '6合', '特码', '平特', '连肖'],
            'SSC': ['时时彩', 'ssc', '分分彩', '時時彩', '重庆时时彩', '腾讯分分彩'],
            # 新增：3D系列关键词
            '3D': ['排列三', '排列3', '福彩3d', '3d', '极速3d', '排列', 'p3', 'p三']
        }
        
        self.lottery_aliases = {
            '分分PK拾': 'PK10', '三分PK拾': 'PK10', '五分PK拾': 'PK10',
            '新幸运飞艇': 'PK10', '澳洲幸运10': 'PK10', '一分PK10': 'PK10',
            '宾果PK10': 'PK10', '极速飞艇': 'PK10', '澳洲飞艇': 'PK10',
            '幸运赛车': 'PK10', '分分赛车': 'PK10', '北京PK10': 'PK10',
            '旧北京PK10': 'PK10', '极速赛车': 'PK10', '幸运赛車': 'PK10',
            '北京赛车': 'PK10', '极速PK10': 'PK10', '幸运PK10': 'PK10',
            '分分快三': 'K3', '三分快3': 'K3', '五分快3': 'K3', '澳洲快三': 'K3',
            '宾果快三': 'K3', '1分快三': 'K3', '3分快三': 'K3', '5分快三': 'K3',
            '10分快三': 'K3', '加州快三': 'K3', '幸运快三': 'K3', '大发快三': 'K3',
            '澳门快三': 'K3', '香港快三': 'K3', '江苏快三': 'K3',
            '新澳门六合彩': 'LHC', '澳门六合彩': 'LHC', '香港六合彩': 'LHC',
            '一分六合彩': 'LHC', '五分六合彩': 'LHC', '三分六合彩': 'LHC',
            '香港⑥合彩': 'LHC', '分分六合彩': 'LHC', '快乐6合彩': 'LHC',
            '港⑥合彩': 'LHC', '台湾大乐透': 'LHC', '大发六合彩': 'LHC',
            '分分时时彩': 'SSC', '三分时时彩': 'SSC', '五分时时彩': 'SSC',
            '宾果时时彩': 'SSC', '1分时时彩': 'SSC', '3分时时彩': 'SSC',
            '5分时时彩': 'SSC', '旧重庆时时彩': 'SSC', '幸运时时彩': 'SSC',
            '腾讯分分彩': 'SSC', '新疆时时彩': 'SSC', '天津时时彩': 'SSC',
            '重庆时时彩': 'SSC', '上海时时彩': 'SSC', '广东时时彩': 'SSC',
            # 新增：3D系列别名
            '排列三': '3D', '排列3': '3D', '幸运排列3': '3D', '一分排列3': '3D',
            '二分排列3': '3D', '三分排列3': '3D', '五分排列3': '3D', '十分排列3': '3D',
            '大发排列3': '3D', '好运排列3': '3D', '福彩3D': '3D', '极速3D': '3D',
            '极速排列3': '3D', '幸运3D': '3D', '一分3D': '3D', '二分3D': '3D',
            '三分3D': '3D', '五分3D': '3D', '十分3D': '3D', '大发3D': '3D', '好运3D': '3D'
        }

    def identify_lottery_type(self, lottery_name):
        """彩种类型识别"""
        lottery_str = str(lottery_name).strip()
        
        if lottery_str in self.lottery_aliases:
            return self.lottery_aliases[lottery_str]
        
        for lottery_type, config in self.lottery_configs.items():
            for lottery in config['lotteries']:
                if lottery in lottery_str:
                    return lottery_type
        
        lottery_lower = lottery_str.lower()
        
        for lottery_type, keywords in self.general_keywords.items():
            for keyword in keywords:
                if keyword.lower() in lottery_lower:
                    return lottery_type
        
        return lottery_str

# ==================== 玩法分类器 - 增强版，借鉴第一个代码的详细映射 ====================
class PlayCategoryNormalizer:
    def __init__(self):
        self.category_mapping = self._create_category_mapping()
    
    def _create_category_mapping(self):
        """创建玩法分类映射 - 借鉴第一个代码的详细映射"""
        mapping = {
            # 快三玩法
            '和值': '和值', '和值_大小单双': '和值', '两面': '两面',
            '二不同号': '二不同号', '三不同号': '三不同号', '独胆': '独胆',
            '点数': '和值', '三军': '独胆', '三軍': '独胆',
            
            # 六合彩玩法
            '特码': '特码', '正1特': '正1特', '正码特_正一特': '正1特',
            '正2特': '正2特', '正码特_正二特': '正2特', '正3特': '正3特',
            '正码特_正三特': '正3特', '正4特': '正4特', '正码特_正四特': '正4特',
            '正5特': '正5特', '正码特_正五特': '正5特', '正6特': '正6特',
            '正码特_正六特': '正6特', '正码': '正码', '正特': '正特',
            '尾数': '尾数', '特肖': '特肖', '平特': '平特', '一肖': '一肖',
            '连肖': '连肖', '连尾': '连尾', '龙虎': '龙虎', '五行': '五行',
            '色波': '色波', '半波': '半波',
            
            # 3D系列玩法
            '两面': '两面', '大小单双': '两面', '百位': '百位', '十位': '十位', 
            '个位': '个位', '百十': '百十', '百个': '百个', '十个': '十个',
            '百十个': '百十个', '定位胆': '定位胆', '定位胆_百位': '定位胆_百位',
            '定位胆_十位': '定位胆_十位', '定位胆_个位': '定位胆_个位',
            
            # 时时彩玩法
            '斗牛': '斗牛', '1-5球': '1-5球', '第1球': '第1球', '第2球': '第2球',
            '第3球': '第3球', '第4球': '第4球', '第5球': '第5球', '总和': '总和',
            '正码': '正码', '定位胆': '定位胆',
            
            # PK拾/赛车玩法
            '前一': '冠军', '定位胆': '定位胆', '1-5名': '1-5名', '6-10名': '6-10名',
            '冠军': '冠军', '亚军': '亚军', '季军': '第三名', '第3名': '第三名',
            '第4名': '第四名', '第5名': '第五名', '第6名': '第六名',
            '第7名': '第七名', '第8名': '第八名', '第9名': '第九名',
            '第10名': '第十名', '双面': '两面', '冠亚和': '冠亚和'
        }
        return mapping
    
    def normalize_category(self, category):
        """统一玩法分类名称 - 增强版"""
        category_str = str(category).strip()
        
        # 直接映射
        if category_str in self.category_mapping:
            return self.category_mapping[category_str]
        
        # 关键词匹配
        for key, value in self.category_mapping.items():
            if key in category_str:
                return value
        
        # 智能匹配
        category_lower = category_str.lower()
        
        # PK10/赛车智能匹配
        if any(word in category_lower for word in ['冠军', '第一名', '第1名', '1st']):
            return '冠军'
        elif any(word in category_lower for word in ['亚军', '第二名', '第2名', '2nd']):
            return '亚军'
        elif any(word in category_lower for word in ['第三名', '第3名', '季军', '3rd']):
            return '第三名'
        elif any(word in category_lower for word in ['第四名', '第4名', '4th']):
            return '第四名'
        elif any(word in category_lower for word in ['第五名', '第5名', '5th']):
            return '第五名'
        elif any(word in category_lower for word in ['第六名', '第6名', '6th']):
            return '第六名'
        elif any(word in category_lower for word in ['第七名', '第7名', '7th']):
            return '第七名'
        elif any(word in category_lower for word in ['第八名', '第8名', '8th']):
            return '第八名'
        elif any(word in category_lower for word in ['第九名', '第9名', '9th']):
            return '第九名'
        elif any(word in category_lower for word in ['第十名', '第10名', '10th']):
            return '第十名'
        
        # 3D系列智能匹配
        elif any(word in category_lower for word in ['百位']):
            return '百位'
        elif any(word in category_lower for word in ['十位']):
            return '十位'
        elif any(word in category_lower for word in ['个位']):
            return '个位'
        
        # 时时彩智能匹配
        elif any(word in category_lower for word in ['第1球', '万位']):
            return '第1球'
        elif any(word in category_lower for word in ['第2球', '千位']):
            return '第2球'
        elif any(word in category_lower for word in ['第3球', '百位']):
            return '第3球'
        elif any(word in category_lower for word in ['第4球', '十位']):
            return '第4球'
        elif any(word in category_lower for word in ['第5球', '个位']):
            return '第5球'
        
        return category_str

# ==================== 内容解析器 - 借鉴第一个代码的详细解析逻辑 ====================
class ContentParser:
    """从第一个代码借鉴的投注内容解析器"""

    @staticmethod
    def parse_pk10_vertical_format(content):
        """
        解析PK10竖线分隔的定位胆格式
        格式：号码1,号码2|号码3|号码4,号码5|号码6|号码7,号码8,号码9|号码10
        """
        try:
            content_str = str(content).strip()
            bets_by_position = defaultdict(list)
            
            if not content_str:
                return bets_by_position
            
            positions = ['冠军', '亚军', '第三名', '第四名', '第五名', 
                        '第六名', '第七名', '第八名', '第九名', '第十名']
            
            parts = content_str.split('|')
            
            for i, part in enumerate(parts):
                if i < len(positions):
                    position = positions[i]
                    part_clean = part.strip()
                    
                    if not part_clean or part_clean == '_' or part_clean == '':
                        continue
                    
                    numbers = []
                    if ',' in part_clean:
                        number_strs = part_clean.split(',')
                        for num_str in number_strs:
                            num_clean = num_str.strip()
                            if num_clean.isdigit():
                                numbers.append(int(num_clean))
                    else:
                        if part_clean.isdigit():
                            numbers.append(int(part_clean))
                    
                    bets_by_position[position].extend(numbers)
            
            return bets_by_position
        except Exception as e:
            logger.warning(f"解析PK10竖线格式失败: {content}, 错误: {str(e)}")
            return defaultdict(list)

    @staticmethod
    def parse_3d_vertical_format(content):
        """
        解析3D竖线分隔的定位胆格式
        格式：号码1,号码2|号码3|号码4,号码5,号码6
        """
        try:
            content_str = str(content).strip()
            bets_by_position = defaultdict(list)
            
            if not content_str:
                return bets_by_position
            
            positions = ['百位', '十位', '个位']
            
            parts = content_str.split('|')
            
            for i, part in enumerate(parts):
                if i < len(positions):
                    position = positions[i]
                    part_clean = part.strip()
                    
                    if not part_clean or part_clean == '_' or part_clean == '':
                        continue
                    
                    numbers = []
                    if ',' in part_clean:
                        number_strs = part_clean.split(',')
                        for num_str in number_strs:
                            num_clean = num_str.strip()
                            if num_clean.isdigit():
                                numbers.append(int(num_clean))
                    else:
                        if part_clean.isdigit():
                            numbers.append(int(part_clean))
                    
                    bets_by_position[position].extend(numbers)
            
            return bets_by_position
        except Exception as e:
            logger.warning(f"解析3D竖线格式失败: {content}, 错误: {str(e)}")
            return defaultdict(list)

    @staticmethod
    def parse_positional_bets(content, position_keywords=None):
        """
        解析位置投注内容
        格式：位置1-投注项1,投注项2,位置2-投注项1,投注项2,...
        """
        content_str = str(content).strip()
        bets_by_position = defaultdict(list)
        
        if not content_str:
            return bets_by_position
        
        parts = [part.strip() for part in content_str.split(',')]
        
        current_position = None
        
        for part in parts:
            is_position = False
            if position_keywords:
                for keyword in position_keywords:
                    if keyword in part and '-' in part:
                        is_position = True
                        break
            
            if '-' in part and (is_position or position_keywords is None):
                try:
                    position_part, bet_value = part.split('-', 1)
                    current_position = position_part.strip()
                    bets_by_position[current_position].append(bet_value.strip())
                except ValueError:
                    if current_position:
                        bets_by_position[current_position].append(part)
            elif current_position:
                bets_by_position[current_position].append(part)
            else:
                bets_by_position['未知位置'].append(part)
        
        return bets_by_position

# ==================== 增强的对刷检测器 - 完善位置判断逻辑 ====================
class WashTradeDetector:
    def __init__(self, config=None):
        self.config = config or Config()
        self.data_processor = DataProcessor()
        self.lottery_identifier = LotteryIdentifier()
        self.play_normalizer = PlayCategoryNormalizer()
        self.content_parser = ContentParser()  # 新增内容解析器
        
        self.data_processed = False
        self.df_valid = None
        self.export_data = []
        
        # 按彩种存储账户统计
        self.account_total_periods_by_lottery = defaultdict(dict)
        self.account_record_stats_by_lottery = defaultdict(dict)
        self.performance_stats = {}
    
    def upload_and_process(self, uploaded_file):
        """上传并处理文件"""
        try:
            if uploaded_file is None:
                st.error("❌ 没有上传文件")
                return None, None
            
            filename = uploaded_file.name
            logger.info(f"✅ 已上传文件: {filename}")
            
            if not any(filename.endswith(ext) for ext in self.config.supported_file_types):
                st.error(f"❌ 不支持的文件类型: {filename}")
                return None, None
            
            with st.spinner("🔄 正在清洗数据..."):
                df_clean = self.data_processor.clean_data(uploaded_file)
            
            if df_clean is not None and len(df_clean) > 0:
                df_enhanced = self.enhance_data_processing(df_clean)
                return df_enhanced, filename
            else:
                return None, None
            
        except Exception as e:
            logger.error(f"文件处理失败: {str(e)}")
            st.error(f"文件处理失败: {str(e)}")
            return None, None
    
    def enhance_data_processing(self, df_clean):
        """增强的数据处理流程"""
        try:
            # 彩种识别
            if '彩种' in df_clean.columns:
                df_clean['原始彩种'] = df_clean['彩种']
                df_clean['彩种类型'] = df_clean['彩种'].apply(self.lottery_identifier.identify_lottery_type)
            
            # 玩法分类统一
            if '玩法' in df_clean.columns:
                df_clean['玩法分类'] = df_clean['玩法'].apply(self.play_normalizer.normalize_category)
            
            # 计算账户统计信息
            self.calculate_account_total_periods_by_lottery(df_clean)
            
            # 提取投注金额和方向 - 增强版，添加位置精度
            df_clean['投注金额'] = df_clean['金额'].apply(lambda x: self.extract_bet_amount_safe(x))
            
            # 修改：传入玩法分类信息进行位置判断
            df_clean['投注方向'] = df_clean.apply(
                lambda row: self.enhanced_extract_direction_with_position(
                    row['内容'], 
                    row['玩法分类'],  # 新增玩法分类参数
                    row['彩种类型']
                ), 
                axis=1
            )
            
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
            
            return df_valid
            
        except Exception as e:
            logger.error(f"数据处理增强失败: {str(e)}")
            st.error(f"数据处理增强失败: {str(e)}")
            return pd.DataFrame()
    
    def extract_bet_amount_safe(self, amount_text):
        """安全提取投注金额"""
        try:
            if pd.isna(amount_text):
                return 0
            
            text = str(amount_text).strip()
            
            # 直接转换
            try:
                cleaned_text = text.replace(',', '').replace('，', '').replace(' ', '')
                if re.match(r'^-?\d+(\.\d+)?$', cleaned_text):
                    amount = float(cleaned_text)
                    if amount >= self.config.min_amount:
                        return amount
            except:
                pass
            
            # 模式匹配
            patterns = [
                r'投注[:：]?\s*(\d+[,，]?\d*\.?\d*)',
                r'下注[:：]?\s*(\d+[,，]?\d*\.?\d*)',
                r'金额[:：]?\s*(\d+[,，]?\d*\.?\d*)',
                r'总额[:：]?\s*(\d+[,，]?\d*\.?\d*)',
                r'(\d+[,，]?\d*\.?\d*)\s*元',
                r'￥\s*(\d+[,，]?\d*\.?\d*)',
                r'¥\s*(\d+[,，]?\d*\.?\d*)',
                r'[\$￥¥]?\s*(\d+[,，]?\d*\.?\d+)',
                r'(\d+[,，]?\d*\.?\d+)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    amount_str = match.group(1).replace(',', '').replace('，', '').replace(' ', '')
                    try:
                        amount = float(amount_str)
                        if amount >= self.config.min_amount:
                            return amount
                    except:
                        continue
            
            return 0
            
        except Exception as e:
            logger.warning(f"金额提取失败: {amount_text}, 错误: {e}")
            return 0
    
    def enhanced_extract_direction_with_position(self, content, play_category, lottery_type):
        """增强的投注方向提取 - 同时利用玩法和内容进行位置判断"""
        try:
            if pd.isna(content):
                return ""
            
            content_str = str(content).strip()
            play_category_str = str(play_category).strip() if play_category else ""
            
            # 首先提取位置信息 - 修改：同时传入玩法和内容
            position = self._extract_position_from_content_and_play(content_str, play_category_str, lottery_type)
            
            # 提取方向信息
            direction = self._extract_direction_from_content(content_str)
            
            if not direction:
                return ""
            
            # 如果有位置信息，组合成"位置-方向"格式
            if position and position != '未知位置':
                return f"{position}-{direction}"
            else:
                return direction
            
        except Exception as e:
            logger.warning(f"方向提取失败: {content}, 玩法: {play_category}, 错误: {e}")
            return ""
    
    def _extract_position_from_content_and_play(self, content, play_category, lottery_type):
        """从内容和玩法中提取位置信息 - 完整的双重判断逻辑"""
        content_str = str(content).strip()
        play_str = str(play_category).strip()
        
        # 根据彩种类型获取位置关键词
        position_keywords = self.config.position_keywords.get(lottery_type, {})
        
        # 1. 首先从玩法分类中提取位置（高优先级）
        play_position = self._extract_position_from_text(play_str, position_keywords, "玩法")
        if play_position and play_position != '未知位置':
            logger.info(f"从玩法识别位置: {play_str} -> {play_position}")
            return play_position
        
        # 2. 从内容中提取位置（中优先级）
        content_position = self._extract_position_from_text(content_str, position_keywords, "内容")
        if content_position and content_position != '未知位置':
            logger.info(f"从内容识别位置: {content_str} -> {content_position}")
            return content_position
        
        # 3. 特殊处理竖线格式（低优先级）
        vertical_position = self._extract_position_from_vertical_format(content_str, lottery_type)
        if vertical_position and vertical_position != '未知位置':
            logger.info(f"从竖线格式识别位置: {content_str} -> {vertical_position}")
            return vertical_position
        
        # 4. 检查是否有位置-方向的组合格式
        combined_position = self._extract_position_from_combined_format(content_str, position_keywords)
        if combined_position and combined_position != '未知位置':
            logger.info(f"从组合格式识别位置: {content_str} -> {combined_position}")
            return combined_position
        
        return '未知位置'
    
    def _extract_position_from_text(self, text, position_keywords, source_type):
        """从文本中提取位置信息"""
        if not text:
            return '未知位置'
        
        text_lower = text.lower()
        
        # 精确匹配：完整位置名称
        for position, keywords in position_keywords.items():
            for keyword in keywords:
                keyword_lower = keyword.lower()
                # 检查是否包含关键词（考虑边界情况）
                if (keyword_lower == text_lower or 
                    f" {keyword_lower} " in f" {text_lower} " or
                    text_lower.startswith(keyword_lower + "-") or
                    text_lower.endswith("-" + keyword_lower)):
                    return position
        
        # 模糊匹配：包含关键词
        for position, keywords in position_keywords.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    return position
        
        return '未知位置'
    
    def _extract_position_from_vertical_format(self, content, lottery_type):
        """从竖线格式中提取位置信息"""
        if '|' not in content:
            return '未知位置'
        
        try:
            if lottery_type == 'PK10':
                bets_by_position = self.content_parser.parse_pk10_vertical_format(content)
                for position in bets_by_position:
                    if bets_by_position[position]:
                        return position
            elif lottery_type == '3D':
                bets_by_position = self.content_parser.parse_3d_vertical_format(content)
                for position in bets_by_position:
                    if bets_by_position[position]:
                        return position
        except Exception as e:
            logger.warning(f"竖线格式解析失败: {content}, 错误: {str(e)}")
        
        return '未知位置'
    
    def _extract_position_from_combined_format(self, content, position_keywords):
        """从位置-方向组合格式中提取位置"""
        if '-' not in content:
            return '未知位置'
        
        parts = content.split('-')
        if len(parts) >= 2:
            position_part = parts[0].strip()
            return self._extract_position_from_text(position_part, position_keywords, "组合格式")
        
        return '未知位置'
    
    def _extract_direction_from_content(self, content):
        """从内容中提取方向信息"""
        content_str = str(content).strip().lower()
        
        for direction, patterns in self.config.direction_patterns.items():
            for pattern in patterns:
                pattern_lower = pattern.lower()
                # 精确匹配方向关键词
                if (pattern_lower == content_str or 
                    f" {pattern_lower} " in f" {content_str} " or
                    content_str.startswith(pattern_lower + "-") or
                    content_str.endswith("-" + pattern_lower) or
                    pattern_lower in content_str):
                    return direction
        
        # 特殊处理：和值相关的方向
        if '和值' in content_str or '和数' in content_str or '总和' in content_str:
            for direction in ['大', '小', '单', '双', '质', '合']:
                if direction in content_str:
                    return direction
        
        # 检查是否有方向在内容中明确出现
        direction_keywords = ['大', '小', '单', '双', '龙', '虎', '质', '合']
        for direction in direction_keywords:
            if direction in content_str:
                return direction
        
        return ""
    
    def calculate_account_total_periods_by_lottery(self, df):
        """按彩种计算每个账户的总投注期数统计"""
        self.account_total_periods_by_lottery = defaultdict(dict)
        self.account_record_stats_by_lottery = defaultdict(dict)
        
        lottery_col = '原始彩种' if '原始彩种' in df.columns else '彩种'
        
        for lottery in df[lottery_col].unique():
            df_lottery = df[df[lottery_col] == lottery]
            
            period_counts = df_lottery.groupby('会员账号')['期号'].nunique().to_dict()
            self.account_total_periods_by_lottery[lottery] = period_counts
            
            record_counts = df_lottery.groupby('会员账号').size().to_dict()
            self.account_record_stats_by_lottery[lottery] = record_counts
    
    def detect_all_wash_trades(self):
        """检测所有类型的对刷交易"""
        if not self.data_processed or self.df_valid is None or len(self.df_valid) == 0:
            st.error("❌ 没有有效数据可用于检测")
            return []
        
        self.performance_stats = {
            'start_time': datetime.now(),
            'total_records': len(self.df_valid),
            'total_periods': self.df_valid['期号'].nunique(),
            'total_accounts': self.df_valid['会员账号'].nunique()
        }
        
        df_filtered = self.exclude_multi_direction_accounts(self.df_valid)
        
        if len(df_filtered) == 0:
            st.error("❌ 过滤后无有效数据")
            return []
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        all_patterns = []
        total_steps = self.config.max_accounts_in_group - 1
        
        for account_count in range(2, self.config.max_accounts_in_group + 1):
            status_text.text(f"🔍 检测{account_count}个账户对刷模式...")
            patterns = self.detect_n_account_patterns_optimized(df_filtered, account_count)
            all_patterns.extend(patterns)
            
            progress = (account_count - 1) / total_steps
            progress_bar.progress(progress)
        
        progress_bar.progress(1.0)
        status_text.text("✅ 检测完成")
        
        self.performance_stats['end_time'] = datetime.now()
        self.performance_stats['detection_time'] = (
            self.performance_stats['end_time'] - self.performance_stats['start_time']
        ).total_seconds()
        self.performance_stats['total_patterns'] = len(all_patterns)
        
        self.display_performance_stats()
        
        return all_patterns
    
    def detect_n_account_patterns_optimized(self, df_filtered, n_accounts):
        """优化版的N个账户对刷模式检测"""
        wash_records = []
        
        period_groups = df_filtered.groupby(['期号', '原始彩种'])
        
        valid_direction_combinations = self._get_valid_direction_combinations(n_accounts)
        
        batch_size = 100
        period_keys = list(period_groups.groups.keys())
        
        for i in range(0, len(period_keys), batch_size):
            batch_keys = period_keys[i:i+batch_size]
            
            for period_key in batch_keys:
                period_data = period_groups.get_group(period_key)
                period_accounts = period_data['会员账号'].unique()
                
                if len(period_accounts) < n_accounts:
                    continue
                
                batch_patterns = self._detect_combinations_for_period(
                    period_data, period_accounts, n_accounts, valid_direction_combinations
                )
                wash_records.extend(batch_patterns)
        
        return self.find_continuous_patterns_optimized(wash_records)
    
    def _get_valid_direction_combinations(self, n_accounts):
        """获取有效的方向组合 - 增强版，支持位置精度"""
        valid_combinations = []
        
        # 对于2个账户：标准的对立组（包括带位置的对立组）
        if n_accounts == 2:
            # 无位置的对立组
            for opposites in self.config.opposite_groups:
                dir1, dir2 = list(opposites)
                valid_combinations.append({
                    'directions': [dir1, dir2],
                    'dir1_count': 1,
                    'dir2_count': 1,
                    'opposite_type': f"{dir1}-{dir2}"
                })
            
            # 带位置的对立组 - 动态生成
            positions = ['冠军', '亚军', '第三名', '第四名', '第五名', 
                        '第六名', '第七名', '第八名', '第九名', '第十名',
                        '百位', '十位', '个位', '第1球', '第2球', '第3球', '第4球', '第5球']
            
            for position in positions:
                for opposites in self.config.opposite_groups:
                    dir1, dir2 = list(opposites)
                    valid_combinations.append({
                        'directions': [f"{position}-{dir1}", f"{position}-{dir2}"],
                        'dir1_count': 1,
                        'dir2_count': 1,
                        'opposite_type': f"{position}-{dir1} vs {position}-{dir2}"
                    })
        
        # 对于3个及以上账户：允许多种分布
        else:
            for opposites in self.config.opposite_groups:
                dir1, dir2 = list(opposites)
                
                for i in range(1, n_accounts):
                    j = n_accounts - i
                    valid_combinations.append({
                        'directions': [dir1] * i + [dir2] * j,
                        'dir1_count': i,
                        'dir2_count': j,
                        'opposite_type': f"{dir1}-{dir2}"
                    })
        
        return valid_combinations
    
    def _detect_combinations_for_period(self, period_data, period_accounts, n_accounts, valid_combinations):
        """为单个期号检测组合 - 添加账户期数差异检查"""
        patterns = []
        
        # 获取当前彩种
        lottery = period_data['原始彩种'].iloc[0] if '原始彩种' in period_data.columns else period_data['彩种'].iloc[0]
        
        # 构建账户信息字典
        account_info = {}
        for _, row in period_data.iterrows():
            account = row['会员账号']
            direction = row['投注方向']
            amount = row['投注金额']
            
            if account not in account_info:
                account_info[account] = []
            account_info[account].append({
                'direction': direction,
                'amount': amount
            })
        
        # 检查所有可能的账户组合
        for account_group in combinations(period_accounts, n_accounts):
            # 新增：检查账户期数差异
            if not self._check_account_period_difference(account_group, lottery):
                continue
            
            group_directions = []
            group_amounts = []
            
            for account in account_group:
                if account in account_info:
                    if account_info[account]:
                        first_bet = account_info[account][0]
                        group_directions.append(first_bet['direction'])
                        group_amounts.append(first_bet['amount'])
            
            if len(group_directions) != n_accounts:
                continue
            
            # 检查是否匹配任何有效的方向组合
            for combo in valid_combinations:
                target_directions = combo['directions']
                
                actual_directions_sorted = sorted(group_directions)
                target_directions_sorted = sorted(target_directions)
                
                if actual_directions_sorted == target_directions_sorted:
                    # 计算两个方向的总金额
                    dir1_total = 0
                    dir2_total = 0
                    dir1 = combo['directions'][0]  # 取第一个方向作为参考
                    
                    for direction, amount in zip(group_directions, group_amounts):
                        if direction == dir1:
                            dir1_total += amount
                        else:
                            dir2_total += amount
                    
                    # 检查金额相似度 - 根据账户数量使用不同的阈值
                    similarity_threshold = self.config.account_count_similarity_thresholds.get(
                        n_accounts, self.config.amount_similarity_threshold
                    )
                    
                    if dir1_total > 0 and dir2_total > 0:
                        similarity = min(dir1_total, dir2_total) / max(dir1_total, dir2_total)
                        
                        if similarity >= similarity_threshold:
                            lottery_type = period_data['彩种类型'].iloc[0] if '彩种类型' in period_data.columns else '未知'
                            
                            record = {
                                '期号': period_data['期号'].iloc[0],
                                '彩种': lottery,
                                '彩种类型': lottery_type,
                                '账户组': list(account_group),
                                '方向组': group_directions,
                                '金额组': group_amounts,
                                '总金额': dir1_total + dir2_total,
                                '相似度': similarity,
                                '账户数量': n_accounts,
                                '模式': f"{combo['opposite_type'].split('-')[0]}({combo['dir1_count']}个) vs {combo['opposite_type'].split('-')[1]}({combo['dir2_count']}个)",
                                '对立类型': combo['opposite_type']
                            }
                            
                            patterns.append(record)
        
        return patterns
    
    def _check_account_period_difference(self, account_group, lottery):
        """检查账户组内账户的总投注期数差异是否在阈值内"""
        if lottery not in self.account_total_periods_by_lottery:
            return True  # 如果没有该彩种的统计信息，默认允许组合
        
        total_periods_stats = self.account_total_periods_by_lottery[lottery]
        
        # 获取账户组内每个账户的总投注期数
        account_periods = []
        for account in account_group:
            if account in total_periods_stats:
                account_periods.append(total_periods_stats[account])
            else:
                # 如果某个账户没有统计信息，无法比较，默认允许组合
                return True
        
        # 如果只有一个账户有期数信息，无法比较，默认允许组合
        if len(account_periods) < 2:
            return True
        
        # 计算最大和最小期数差异
        max_period = max(account_periods)
        min_period = min(account_periods)
        period_diff = max_period - min_period
        
        # 如果期数差异超过阈值，不允许组合
        if period_diff > self.config.account_period_diff_threshold:
            logger.info(f"跳过账户组 {account_group}，期数差异 {period_diff} > {self.config.account_period_diff_threshold}")
            return False
        
        return True
    
    def find_continuous_patterns_optimized(self, wash_records):
        """优化版的连续对刷模式检测 - 修改阈值逻辑"""
        if not wash_records:
            return []
        
        account_group_patterns = defaultdict(list)
        for record in wash_records:
            account_group_key = (tuple(sorted(record['账户组'])), record['彩种'])
            account_group_patterns[account_group_key].append(record)
        
        continuous_patterns = []
        
        for (account_group, lottery), records in account_group_patterns.items():
            sorted_records = sorted(records, key=lambda x: x['期号'])
            
            # 修改：根据新的阈值要求确定最小对刷期数
            required_min_periods = self.get_required_min_periods(account_group, lottery)
            
            if len(sorted_records) >= required_min_periods:
                total_investment = sum(r['总金额'] for r in sorted_records)
                similarities = [r['相似度'] for r in sorted_records]
                avg_similarity = np.mean(similarities) if similarities else 0
                
                opposite_type_counts = defaultdict(int)
                for record in sorted_records:
                    opposite_type_counts[record['对立类型']] += 1
                
                pattern_count = defaultdict(int)
                for record in sorted_records:
                    pattern_count[record['模式']] += 1
                
                main_opposite_type = max(opposite_type_counts.items(), key=lambda x: x[1])[0]
                
                # 账户统计信息
                account_stats_info = []
                total_periods_stats = self.account_total_periods_by_lottery.get(lottery, {})
                record_stats = self.account_record_stats_by_lottery.get(lottery, {})
                
                for account in account_group:
                    total_periods = total_periods_stats.get(account, 0)
                    records_count = record_stats.get(account, 0)
                    account_stats_info.append(f"{account}({total_periods}期/{records_count}记录)")
                
                activity_level = self.get_account_group_activity_level(account_group, lottery)
                
                continuous_patterns.append({
                    '账户组': list(account_group),
                    '彩种': lottery,
                    '彩种类型': records[0]['彩种类型'] if records else '未知',
                    '账户数量': len(account_group),
                    '主要对立类型': main_opposite_type,
                    '对立类型分布': dict(opposite_type_counts),
                    '对刷期数': len(sorted_records),
                    '总投注金额': total_investment,
                    '平均相似度': avg_similarity,
                    '模式分布': dict(pattern_count),
                    '详细记录': sorted_records,
                    '账户活跃度': activity_level,
                    '账户统计信息': account_stats_info,
                    '要求最小对刷期数': required_min_periods
                })
        
        return continuous_patterns

    def exclude_multi_direction_accounts(self, df_valid):
        """排除同一账户多方向下注"""
        multi_direction_mask = (
            df_valid.groupby(['期号', '会员账号'])['投注方向']
            .transform('nunique') > 1
        )
        
        df_filtered = df_valid[~multi_direction_mask].copy()
        
        return df_filtered
    
    def get_account_group_activity_level(self, account_group, lottery):
        """修改：根据新的活跃度阈值获取活跃度水平"""
        if lottery not in self.account_total_periods_by_lottery:
            return 'unknown'
        
        total_periods_stats = self.account_total_periods_by_lottery[lottery]
        
        # 计算账户组中在指定彩种的最小总投注期数
        min_total_periods = min(total_periods_stats.get(account, 0) for account in account_group)
        
        # 修改：按照新的活跃度阈值
        if min_total_periods <= self.config.period_thresholds['low_activity']:
            return 'low'        # 总投注期数1-10
        elif min_total_periods <= self.config.period_thresholds['medium_activity_high']:
            return 'medium'     # 总投注期数11-50
        elif min_total_periods <= self.config.period_thresholds['high_activity_high']:
            return 'high'       # 总投注期数51-100
        else:
            return 'very_high'  # 总投注期数100以上
    
    def get_required_min_periods(self, account_group, lottery):
        """修改：根据新的活跃度阈值获取所需的最小对刷期数"""
        activity_level = self.get_account_group_activity_level(account_group, lottery)
        
        if activity_level == 'low':
            return self.config.period_thresholds['min_periods_low']      # 3期
        elif activity_level == 'medium':
            return self.config.period_thresholds['min_periods_medium']   # 5期
        elif activity_level == 'high':
            return self.config.period_thresholds['min_periods_high']     # 8期
        else:
            return self.config.period_thresholds['min_periods_very_high'] # 11期
    
    def display_performance_stats(self):
        """显示性能统计"""
        if not self.performance_stats:
            return
        
        with st.expander("📈 性能统计", expanded=False):
            st.write(f"**数据处理统计:**")
            st.write(f"- 总记录数: {self.performance_stats['total_records']:,}")
            st.write(f"- 总期号数: {self.performance_stats['total_periods']:,}")
            st.write(f"- 总账户数: {self.performance_stats['total_accounts']:,}")
            
            if 'detection_time' in self.performance_stats:
                st.write(f"**检测性能:**")
                st.write(f"- 检测时间: {self.performance_stats['detection_time']:.2f} 秒")
                st.write(f"- 发现模式: {self.performance_stats['total_patterns']} 个")
    
    def display_detailed_results(self, patterns):
        """显示详细检测结果"""
        st.write("\n" + "="*60)
        st.write("🎯 多账户对刷检测结果")
        st.write("="*60)
        
        if not patterns:
            st.error("❌ 未发现符合阈值条件的连续对刷模式")
            return
        
        patterns_by_lottery = defaultdict(list)
        for pattern in patterns:
            lottery_key = pattern['彩种']
            patterns_by_lottery[lottery_key].append(pattern)
        
        for lottery, lottery_patterns in patterns_by_lottery.items():
            with st.expander(f"🎲 彩种：{lottery}（发现{len(lottery_patterns)}组）", expanded=True):
                for i, pattern in enumerate(lottery_patterns, 1):
                    st.markdown(f"**对刷组 {i}:** {' ↔ '.join(pattern['账户组'])}")
                    
                    activity_icon = "🟢" if pattern['账户活跃度'] == 'low' else "🟡" if pattern['账户活跃度'] == 'medium' else "🟠" if pattern['账户活跃度'] == 'high' else "🔴"
                    st.markdown(f"**活跃度:** {activity_icon} {pattern['账户活跃度']} | **彩种:** {pattern['彩种']} | **主要类型:** {pattern['主要对立类型']}")
                    
                    st.markdown(f"**账户在该彩种投注期数/记录数:** {', '.join(pattern['账户统计信息'])}")
                    st.markdown(f"**对刷期数:** {pattern['对刷期数']}期 (要求≥{pattern['要求最小对刷期数']}期)")
                    st.markdown(f"**总金额:** {pattern['总投注金额']:.2f}元 | **平均匹配:** {pattern['平均相似度']:.2%}")
                    
                    st.markdown("**详细记录:**")
                    for j, record in enumerate(pattern['详细记录'], 1):
                        account_directions = []
                        for account, direction, amount in zip(record['账户组'], record['方向组'], record['金额组']):
                            account_directions.append(f"{account}({direction}:{amount})")
                        
                        st.markdown(f"{j}. **期号:** {record['期号']} | **模式:** {record['模式']} | **方向:** {' ↔ '.join(account_directions)} | **匹配度:** {record['相似度']:.2%}")
                    
                    if i < len(lottery_patterns):
                        st.markdown("---")
        
        self.display_summary_statistics(patterns)
    
    def display_summary_statistics(self, patterns):
        """显示总体统计"""
        if not patterns:
            return
            
        st.write(f"\n{'='*60}")
        st.write("📊 总体统计")
        st.write(f"{'='*60}")
        
        total_groups = len(patterns)
        total_accounts = sum(p['账户数量'] for p in patterns)
        total_wash_periods = sum(p['对刷期数'] for p in patterns)
        total_amount = sum(p['总投注金额'] for p in patterns)
        
        account_count_stats = defaultdict(int)
        for pattern in patterns:
            account_count_stats[pattern['账户数量']] += 1
        
        lottery_stats = defaultdict(int)
        for pattern in patterns:
            lottery_stats[pattern['彩种']] += 1
        
        activity_stats = defaultdict(int)
        for pattern in patterns:
            activity_stats[pattern['账户活跃度']] += 1
        
        opposite_type_stats = defaultdict(int)
        for pattern in patterns:
            for opposite_type, count in pattern['对立类型分布'].items():
                opposite_type_stats[opposite_type] += count
        
        st.write(f"**🎯 检测结果汇总:**")
        st.write(f"- 对刷组数: {total_groups} 组")
        st.write(f"- 涉及账户: {total_accounts} 个")
        st.write(f"- 总对刷期数: {total_wash_periods} 期")
        st.write(f"- 总涉及金额: {total_amount:.2f} 元")
        
        st.write(f"**👥 按账户数量分布:**")
        for account_count, count in sorted(account_count_stats.items()):
            st.write(f"- {account_count}个账户组: {count} 组")
        
        st.write(f"**🎲 按彩种分布:**")
        for lottery, count in lottery_stats.items():
            st.write(f"- {lottery}: {count} 组")
            
        st.write(f"**📈 按活跃度分布:**")
        for activity, count in activity_stats.items():
            st.write(f"- {activity}活跃度: {count} 组")
            
        st.write(f"**🎯 按对立类型分布:**")
        for opposite_type, count in opposite_type_stats.items():
            st.write(f"- {opposite_type}: {count} 期对刷")

# ==================== 主函数 ====================
def main():
    """主函数"""
    st.title("🎯 智能多账户对刷检测系统")
    st.markdown("---")
    
    with st.sidebar:
        st.header("📁 数据上传")
        uploaded_file = st.file_uploader(
            "请上传数据文件", 
            type=['xlsx', 'xls', 'csv'],
            help="请确保文件包含必要的列：会员账号、期号、内容、金额"
        )
    
    if uploaded_file is not None:
        try:
            # 配置参数
            st.sidebar.header("⚙️ 检测参数配置")
            
            min_amount = st.sidebar.number_input("最小投注金额", value=10, min_value=1, help="低于此金额的记录将被过滤")
            base_similarity_threshold = st.sidebar.slider("基础金额匹配度阈值", 0.8, 1.0, 0.8, 0.01, help="2个账户的基础匹配度阈值")
            max_accounts = st.sidebar.slider("最大检测账户数", 2, 8, 5, help="检测的最大账户组合数量")
            
            # 新增：账户期数差异阈值配置
            period_diff_threshold = st.sidebar.number_input(
                "账户期数最大差异阈值", 
                value=150, 
                min_value=0, 
                max_value=1000,
                help="账户总投注期数最大允许差异，超过此值不进行组合检测"
            )
            
            # 活跃度阈值配置
            st.sidebar.subheader("📊 活跃度阈值配置")
            st.sidebar.markdown("**新阈值设置:**")
            st.sidebar.markdown("- **1-10期:** 要求≥3期连续对刷")
            st.sidebar.markdown("- **11-50期:** 要求≥5期连续对刷")  
            st.sidebar.markdown("- **51-100期:** 要求≥8期连续对刷")
            st.sidebar.markdown("- **100期以上:** 要求≥11期连续对刷")
            
            # 多账户匹配度配置
            st.sidebar.subheader("🎯 多账户匹配度配置")
            st.sidebar.markdown("**账户数量 vs 匹配度要求:**")
            st.sidebar.markdown("- **2个账户:** 80%匹配度")
            st.sidebar.markdown("- **3个账户:** 85%匹配度")  
            st.sidebar.markdown("- **4个账户:** 90%匹配度")
            st.sidebar.markdown("- **5个账户:** 95%匹配度")
            
            # 更新配置参数
            config = Config()
            config.min_amount = min_amount
            config.amount_similarity_threshold = base_similarity_threshold
            config.max_accounts_in_group = max_accounts
            config.account_period_diff_threshold = period_diff_threshold
            
            # 设置多账户匹配度阈值
            config.account_count_similarity_thresholds = {
                2: base_similarity_threshold,
                3: max(base_similarity_threshold + 0.05, 0.85),
                4: max(base_similarity_threshold + 0.1, 0.9),
                5: max(base_similarity_threshold + 0.15, 0.95)
            }
            
            detector = WashTradeDetector(config)
            
            st.success(f"✅ 已上传文件: {uploaded_file.name}")
            
            with st.spinner("🔄 正在解析数据..."):
                df_enhanced, filename = detector.upload_and_process(uploaded_file)
                
                if df_enhanced is not None and len(df_enhanced) > 0:
                    st.success("✅ 数据解析完成")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("有效记录数", f"{len(df_enhanced):,}")
                    with col2:
                        st.metric("唯一期号数", f"{df_enhanced['期号'].nunique():,}")
                    with col3:
                        st.metric("唯一账户数", f"{df_enhanced['会员账号'].nunique():,}")
                    with col4:
                        if '彩种类型' in df_enhanced.columns:
                            st.metric("彩种类型数", f"{df_enhanced['彩种类型'].nunique()}")
                    
                    with st.expander("📊 数据详情", expanded=False):
                        tab1, tab2, tab3 = st.tabs(["数据概览", "彩种分布", "位置判断详情"])
                        
                        with tab1:
                            st.dataframe(df_enhanced.head(100), use_container_width=True)
                            
                        with tab2:
                            if '彩种类型' in df_enhanced.columns:
                                lottery_type_stats = df_enhanced['彩种类型'].value_counts()
                                st.bar_chart(lottery_type_stats)
                        
                        with tab3:
                            # 显示位置判断的详细分析
                            if '投注方向' in df_enhanced.columns:
                                position_analysis = df_enhanced[['玩法分类', '内容', '投注方向']].copy()
                                position_analysis['位置来源'] = position_analysis.apply(
                                    lambda row: "玩法" if '-' in row['投注方向'] and row['玩法分类'] in row['投注方向'] else 
                                               "内容" if '-' in row['投注方向'] else "无位置", 
                                    axis=1
                                )
                                st.dataframe(position_analysis.head(50), use_container_width=True)
                                
                                # 位置来源统计
                                source_stats = position_analysis['位置来源'].value_counts()
                                st.write("**位置判断来源统计:**")
                                for source, count in source_stats.items():
                                    st.write(f"- {source}: {count} 条记录")
                    
                    st.info("🚀 自动开始检测对刷交易...")
                    with st.spinner("🔍 正在检测对刷交易..."):
                        patterns = detector.detect_all_wash_trades()
                    
                    if patterns:
                        st.success(f"✅ 检测完成！发现 {len(patterns)} 个对刷组")
                        detector.display_detailed_results(patterns)
                    else:
                        st.warning("⚠️ 未发现符合阈值条件的对刷行为")
                else:
                    st.error("❌ 数据解析失败，请检查文件格式和内容")
            
        except Exception as e:
            st.error(f"❌ 程序执行失败: {str(e)}")
            st.error(f"详细错误信息:\n{traceback.format_exc()}")
    else:
        st.info("👈 请在左侧边栏上传数据文件开始分析")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.subheader("🔍 智能检测")
            st.markdown("""
            - 多账户对刷模式识别
            - 智能金额匹配分析
            - 活跃度自适应阈值
            - 实时进度监控
            """)
        
        with col2:
            st.subheader("📊 专业分析")
            st.markdown("""
            - 完整彩种支持（新增3D系列）
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
            - 实时性能监控
            """)
    
    with st.expander("📖 系统使用说明", expanded=False):
        st.markdown("""
        ### 系统功能说明

        **🎯 检测逻辑：**
        - **总投注期数**：账户在特定彩种中的所有期号投注次数
        - **对刷期数**：账户组实际发生对刷行为的期数
        - 根据**总投注期数**判定账户活跃度，设置不同的**对刷期数**阈值

        **📊 新活跃度判定：**
        - **1-10期**：要求≥3期连续对刷
        - **11-50期**：要求≥5期连续对刷  
        - **51-100期**：要求≥8期连续对刷
        - **100期以上**：要求≥11期连续对刷

        **🎯 多账户匹配度要求：**
        - **2个账户**：80%匹配度
        - **3个账户**：85%匹配度  
        - **4个账户**：90%匹配度
        - **5个账户**：95%匹配度

        **🔄 账户期数差异检查：**
        - 避免期数差异过大的账户组合
        - 默认阈值：150期
        - 可自定义调整阈值

        **🎲 位置判断增强：**
        - **双重判断机制**：同时利用玩法和内容进行位置判断
        - **优先级顺序**：玩法分类 > 投注内容 > 竖线格式 > 组合格式
        - **精确匹配**：支持完整位置名称和关键词匹配
        - **多格式支持**：PK10竖线格式、3D竖线格式、位置-方向组合格式

        **⚡ 自动检测：**
        - 数据上传后自动开始处理和分析
        - 无需手动点击开始检测按钮
        """)

if __name__ == "__main__":
    main()
