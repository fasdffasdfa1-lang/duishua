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

# ==================== 配置类 - 修复版 ====================
class Config:
    """配置参数类 - 修复版，支持变异形式但映射到基础方向"""
    def __init__(self):
        self.min_amount = 10
        self.amount_similarity_threshold = 0.8
        self.min_continuous_periods = 3
        self.max_accounts_in_group = 5
        self.supported_file_types = ['.xlsx', '.xls', '.csv']
        
        # 列名映射配置
        self.column_mappings = {
            '会员账号': ['会员账号', '会员账户', '账号', '账户', '用户账号', '玩家账号', '用户ID', '玩家ID'],
            '彩种': ['彩种', '彩神', '彩票种类', '游戏类型', '彩票类型', '游戏彩种', '彩票名称'],
            '期号': ['期号', '期数', '期次', '期', '奖期', '期号信息', '期号编号'],
            '玩法': ['玩法', '玩法分类', '投注类型', '类型', '投注玩法', '玩法类型', '分类'],
            '内容': ['内容', '投注内容', '下注内容', '注单内容', '投注号码', '号码内容', '投注信息'],
            '金额': ['金额', '下注总额', '投注金额', '总额', '下注金额', '投注额', '金额数值']
        }
        
        # 活跃度阈值配置
        self.period_thresholds = {
            'low_activity': 10,
            'medium_activity_low': 11,
            'medium_activity_high': 50,
            'high_activity_low': 51,
            'high_activity_high': 100,
            'min_periods_low': 3,
            'min_periods_medium': 5,
            'min_periods_high': 8,
            'min_periods_very_high': 11
        }
        
        # 多账户匹配度阈值
        self.account_count_similarity_thresholds = {
            2: 0.8,
            3: 0.85,
            4: 0.9,
            5: 0.95
        }
        
        # 账户期数差异阈值
        self.account_period_diff_threshold = 150
        
        # 🎯 关键修复：扩展方向模式，但保持变异形式的独立性
        self.direction_patterns = {
            # 基础方向
            '小': ['两面-小', '和值-小', '小', 'small', 'xia', 'xiao'],
            '大': ['两面-大', '和值-大', '大', 'big', 'da', 'large'], 
            '单': ['两面-单', '和值-单', '单', 'odd', 'dan', '奇数'],
            '双': ['两面-双', '和值-双', '双', 'even', 'shuang', '偶数'],
            '龙': ['龙', 'long', 'dragon', '龍', '龍虎-龙'],
            '虎': ['虎', 'hu', 'tiger', '龍虎-虎'],
            '质': ['质', '质数', 'prime', 'zhi', '質', '質數'],
            '合': ['合', '合数', 'composite', 'he', '合數'],
            
            # 🎯 新增：保持变异形式的独立性
            '特小': ['特小', '极小', '最小'],
            '特大': ['特大', '极大', '最大'],
            '特单': ['特单'],
            '特双': ['特双'],
            '总和小': ['总和小', '和小'],
            '总和大': ['总和大', '和大'],
            '总和单': ['总和单', '和单'],
            '总和双': ['总和双', '和双']
        }
        
        # 🎯 修复：扩展对立组，包含变异形式
        self.opposite_groups = [
            # 基础对立组
            {'大', '小'}, {'单', '双'}, {'龙', '虎'}, {'质', '合'},
            # 变异形式对立组
            {'特大', '特小'}, {'特单', '特双'}, 
            {'总和大', '总和小'}, {'总和单', '总和双'}
        ]
        
        # 位置关键词映射 - 增强版
        self.position_keywords = {
            'PK10': {
                '冠军': ['冠军', '第1名', '第一名', '前一', '冠 军', '冠　军'],
                '亚军': ['亚军', '第2名', '第二名', '亚 军', '亚　军'],
                '季军': ['季军', '第3名', '第三名', '季 军', '季　军'],
                '第四名': ['第四名', '第4名'],
                '第五名': ['第五名', '第5名'],
                '第六名': ['第六名', '第6名'],
                '第七名': ['第七名', '第7名'],
                '第八名': ['第八名', '第8名'],
                '第九名': ['第九名', '第9名'],
                '第十名': ['第十名', '第10名']
            },
            '3D': {
                '百位': ['百位', '定位_百位', '百位定位'],
                '十位': ['十位', '定位_十位', '十位定位'],
                '个位': ['个位', '定位_个位', '个位定位']
            },
            'SSC': {
                '第1球': ['第1球', '万位', '第一位', '定位_万位', '万位定位'],
                '第2球': ['第2球', '千位', '第二位', '定位_千位', '千位定位'],
                '第3球': ['第3球', '百位', '第三位', '定位_百位', '百位定位'],
                '第4球': ['第4球', '十位', '第四位', '定位_十位', '十位定位'],
                '第5球': ['第5球', '个位', '第五位', '定位_个位', '个位定位']
            }
        }

# ==================== 数据处理器类 ====================
class DataProcessor:
    def __init__(self):
        self.required_columns = ['会员账号', '彩种', '期号', '玩法', '内容', '金额']
        self.column_mapping = {
            '会员账号': ['会员账号', '会员账户', '账号', '账户', '用户账号', '玩家账号', '用户ID', '玩家ID', '用户名称', '玩家名称'],
            '彩种': ['彩种', '彩神', '彩票种类', '游戏类型', '彩票类型', '游戏彩种', '彩票名称', '彩系', '游戏名称'],
            '期号': ['期号', '期数', '期次', '期', '奖期', '期号信息', '期号编号', '开奖期号', '奖期号'],
            '玩法': ['玩法', '玩法分类', '投注类型', '类型', '投注玩法', '玩法类型', '分类', '玩法名称', '投注方式'],
            '内容': ['内容', '投注内容', '下注内容', '注单内容', '投注号码', '号码内容', '投注信息', '号码', '选号'],
            '金额': ['金额', '下注总额', '投注金额', '总额', '下注金额', '投注额', '金额数值', '单注金额', '投注额', '钱', '元']
        }
        
        self.similarity_threshold = 0.7
    
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
                        
                        similarity_score = self._calculate_string_similarity(possible_name_lower, actual_col_lower)
                        
                        if (possible_name_lower in actual_col_lower or 
                            actual_col_lower in possible_name_lower or
                            similarity_score >= self.similarity_threshold):
                            
                            identified_columns[actual_col] = standard_col
                            st.success(f"✅ 识别列名: {actual_col} -> {standard_col} (相似度: {similarity_score:.2f})")
                            found = True
                            break
                    
                    if found:
                        break
                
                if not found:
                    st.warning(f"⚠️ 未识别到 {standard_col} 对应的列名")
        
        return identified_columns
    
    # ========== 🆕 新增这个方法 ==========
    def _calculate_string_similarity(self, str1, str2):
        """计算字符串相似度 - 整合第一套代码算法"""
        if not str1 or not str2:
            return 0
        
        # 使用集合交集计算相似度
        set1 = set(str1)
        set2 = set(str2)
        intersection = set1 & set2
        
        if not set1:
            return 0
        
        return len(intersection) / len(set1)
    
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

        if '会员账号' in df.columns:
            # 检查截断账号
            truncated_accounts = df[df['会员账号'].str.contains(r'\.\.\.|…', na=False)]
            if len(truncated_accounts) > 0:
                issues.append(f"发现 {len(truncated_accounts)} 个可能被截断的会员账号")
            
            # 检查账号长度异常
            account_lengths = df['会员账号'].str.len()
            if account_lengths.max() > 50:
                issues.append("发现异常长度的会员账号")
            
            # 显示账号格式样本
            unique_accounts = df['会员账号'].unique()[:5]
            sample_info = " | ".join([f"'{acc}'" for acc in unique_accounts])
            st.info(f"会员账号格式样本: {sample_info}")
        
        if '期号' in df.columns:
            df['期号'] = df['期号'].astype(str).str.replace(r'\.0$', '', regex=True)
            invalid_periods = df[~df['期号'].str.match(r'^[\dA-Za-z]+$')]
            if len(invalid_periods) > 0:
                issues.append(f"发现 {len(invalid_periods)} 条无效期号记录")
        
        if '彩种' in df.columns:
            lottery_stats = df['彩种'].value_counts()
            st.info(f"🎲 彩种分布: 共{len(lottery_stats)}种，前5: {', '.join([f'{k}({v}条)' for k,v in lottery_stats.head().items()])}")
        
        if hasattr(df, '投注方向') and '投注方向' in df.columns:
            direction_stats = df['投注方向'].value_counts().head(10)
            with st.expander("🎯 投注方向分布TOP10", expanded=False):
                for direction, count in direction_stats.items():
                    st.write(f"  - {direction}: {count}次")
        
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
            
            # ========== 🔄 修复这里：调用增强的数据验证 ==========
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

# ==================== 彩种识别器 ====================
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

# ==================== 玩法分类器 ====================
class PlayCategoryNormalizer:
    def __init__(self):
        self.category_mapping = self._create_category_mapping()
    
    def _create_category_mapping(self):
        """创建玩法分类映射"""
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
        """统一玩法分类名称"""
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

# ==================== 内容解析器 - 修复版 ====================
class ContentParser:
    """修复内容解析器 - 支持变异形式但映射到基础方向"""
    
    @staticmethod
    def extract_basic_directions(content, config):
        """提取方向 - 保持变异形式独立性"""
        content_str = str(content).strip()
        directions = []
        
        if not content_str:
            return directions
        
        content_lower = content_str.lower()
        
        # 🎯 提取所有可能的方向（保持变异形式独立性）
        for direction, patterns in config.direction_patterns.items():
            for pattern in patterns:
                pattern_lower = pattern.lower()
                # 精确匹配检查
                if (pattern_lower == content_lower or 
                    pattern_lower in content_lower or 
                    content_lower in pattern_lower):
                    directions.append(direction)
                    break
        
        return directions

    @staticmethod
    def extract_position_from_play_category(play_category, lottery_type, config):
        """从玩法分类中提取位置信息"""
        play_str = str(play_category).strip()
        
        if not play_str:
            return '未知位置'
        
        # 根据彩种类型获取位置关键词
        position_keywords = config.position_keywords.get(lottery_type, {})
        
        for position, keywords in position_keywords.items():
            for keyword in keywords:
                if keyword in play_str:
                    return position
        
        return '未知位置'

    @staticmethod
    def parse_pk10_vertical_format(content):
        """解析PK10竖线分隔格式"""
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
                    
                    # 注意：这里解析数字，但我们只关心方向，所以这个函数主要用于位置提取
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
        """解析3D竖线分隔格式"""
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
                    
                    # 注意：这里解析数字，但我们只关心方向
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

# ==================== 修复的对刷检测器 ====================
class WashTradeDetector:
    def __init__(self, config=None):
        self.config = config or Config()
        self.data_processor = DataProcessor()
        self.lottery_identifier = LotteryIdentifier()
        self.play_normalizer = PlayCategoryNormalizer()
        self.content_parser = ContentParser()
        
        self.data_processed = False
        self.df_valid = None
        self.export_data = []
        
        # 按彩种存储账户统计
        self.account_total_periods_by_lottery = defaultdict(dict)
        self.account_record_stats_by_lottery = defaultdict(dict)
        self.performance_stats = {}

        self._cache_clear()
    
    def _cache_clear(self):
        """清空缓存"""
        self.cached_extract_bet_amount.cache_clear()
        self.cached_extract_direction.cache_clear()
    
    @lru_cache(maxsize=2000)  # 🔄 增大缓存容量
    def cached_extract_bet_amount(self, amount_text):
        """增强缓存金额提取"""
        return self.extract_bet_amount_safe(amount_text)
    
    @lru_cache(maxsize=1000)  # 🔄 增大缓存容量
    def cached_extract_direction(self, content, play_category, lottery_type):
        """增强缓存方向提取"""
        return self.enhanced_extract_direction_with_position(content, play_category, lottery_type)
    
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
        """增强的数据处理流程 - 修复版"""
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
            
            # 提取投注金额和方向 - 使用缓存版本
            st.info("💰 正在提取投注金额和方向...")
            progress_bar = st.progress(0)
            total_rows = len(df_clean)
            
            # 分批处理显示进度
            batch_size = 1000
            for i in range(0, total_rows, batch_size):
                end_idx = min(i + batch_size, total_rows)
                batch_df = df_clean.iloc[i:end_idx]
                
                # 处理当前批次
                df_clean.loc[i:end_idx-1, '投注金额'] = batch_df['金额'].apply(
                    lambda x: self.cached_extract_bet_amount(str(x))
                )
                df_clean.loc[i:end_idx-1, '投注方向'] = batch_df.apply(
                    lambda row: self.cached_extract_direction(
                        row['内容'], 
                        row.get('玩法', ''), 
                        row['彩种类型']
                    ), 
                    axis=1
                )
                
                # 更新进度
                progress = (end_idx) / total_rows
                progress_bar.progress(progress)
            
            progress_bar.empty()
            
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
        """安全提取投注金额 - 增强版"""
        try:
            if pd.isna(amount_text):
                return 0
            
            text = str(amount_text).strip()
            
            # 处理科学计数法
            if 'E' in text or 'e' in text:
                try:
                    amount = float(text)
                    if amount >= self.config.min_amount:
                        return amount
                except:
                    pass
            
            # 直接转换
            try:
                # 移除所有非数字字符（除了小数点和负号）
                cleaned_text = re.sub(r'[^\d.-]', '', text)
                if cleaned_text and cleaned_text != '-':
                    amount = float(cleaned_text)
                    if amount >= self.config.min_amount:
                        return amount
            except:
                pass
            
            # 模式匹配 - 增强模式
            patterns = [
                r'投注[:：]?\s*([-]?\d+[,，]?\d*\.?\d*)',
                r'下注[:：]?\s*([-]?\d+[,，]?\d*\.?\d*)',
                r'金额[:：]?\s*([-]?\d+[,，]?\d*\.?\d*)',
                r'总额[:：]?\s*([-]?\d+[,，]?\d*\.?\d*)',
                r'([-]?\d+[,，]?\d*\.?\d*)\s*元',
                r'￥\s*([-]?\d+[,，]?\d*\.?\d*)',
                r'¥\s*([-]?\d+[,，]?\d*\.?\d*)',
                r'[\$￥¥]?\s*([-]?\d+[,，]?\d*\.?\d+)',
                r'([-]?\d+[,，]?\d*\.?\d+)',
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
        """🎯 修复版方向提取 - 保持变异形式独立性，正确提取位置"""
        try:
            if pd.isna(content):
                return ""
            
            content_str = str(content).strip()
            
            # 🎯 使用修复的内容解析器提取方向（保持变异形式独立性）
            directions = self.content_parser.extract_basic_directions(content_str, self.config)
            
            if not directions:
                return ""
            
            # 🎯 从玩法分类中提取位置信息
            position = self.content_parser.extract_position_from_play_category(play_category, lottery_type, self.config)
            
            # 🎯 选择主要方向
            main_direction = self._select_primary_direction(directions, content_str)
            
            if not main_direction:
                return ""
            
            # 🎯 组合位置和方向
            if position and position != '未知位置':
                return f"{position}-{main_direction}"
            else:
                return main_direction
            
        except Exception as e:
            logger.warning(f"方向提取失败: {content}, 错误: {e}")
            return ""
    
    def _select_primary_direction(self, directions, content):
        """选择主要方向 - 修复版"""
        if not directions:
            return ""
        
        if len(directions) == 1:
            return directions[0]
        
        content_str = str(content)
        
        # 🎯 优先级规则 - 修复版
        priority_rules = [
            # 最高优先级：总和相关
            lambda d: any(keyword in content_str for keyword in ['总和', '总']) and d in directions,
            # 高优先级：特字相关
            lambda d: '特' in content_str and d in directions,
            # 中优先级：和值相关
            lambda d: any(keyword in content_str for keyword in ['和值', '和']) and d in directions,
            # 基础优先级：两面相关
            lambda d: '两面' in content_str and d in directions,
            # 默认优先级
            lambda d: d in directions
        ]
        
        for rule in priority_rules:
            matching_directions = [d for d in directions if rule(d)]
            if matching_directions:
                return matching_directions[0]
        
        return directions[0]
    
    def _extract_position_from_content(self, content, lottery_type):
        """从内容中提取位置信息"""
        content_str = str(content).strip()
        
        # 根据彩种类型获取位置关键词
        position_keywords = self.config.position_keywords.get(lottery_type, {})
        
        for position, keywords in position_keywords.items():
            for keyword in keywords:
                if keyword in content_str:
                    return position
        
        # 特殊处理竖线格式
        if '|' in content_str:
            if lottery_type == 'PK10':
                bets_by_position = self.content_parser.parse_pk10_vertical_format(content_str)
                for position in bets_by_position:
                    if bets_by_position[position]:
                        return position
            elif lottery_type == '3D':
                bets_by_position = self.content_parser.parse_3d_vertical_format(content_str)
                for position in bets_by_position:
                    if bets_by_position[position]:
                        return position
        
        return '未知位置'
    
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
        """🎯 修复版有效方向组合生成 - 保持基础对立组但支持变异形式"""
        valid_combinations = []
        
        # 🎯 基础对立组处理 - 保持4组基础对立关系
        for opposites in self.config.opposite_groups:
            opposite_list = list(opposites)
            
            if n_accounts == 2:
                # 2个账户：标准的1v1对立
                if len(opposite_list) == 2:
                    dir1, dir2 = opposite_list
                    valid_combinations.append({
                        'directions': [dir1, dir2],
                        'dir1_count': 1,
                        'dir2_count': 1,
                        'opposite_type': f"{dir1}-{dir2}"
                    })
            else:
                # 3个及以上账户：多种分布
                for i in range(1, n_accounts):
                    j = n_accounts - i
                    if len(opposite_list) == 2:
                        dir1, dir2 = opposite_list
                        valid_combinations.append({
                            'directions': [dir1] * i + [dir2] * j,
                            'dir1_count': i,
                            'dir2_count': j,
                            'opposite_type': f"{dir1}-{dir2}"
                        })
        
        # 🎯 带位置的对立组 - 动态生成（支持变异形式）
        positions = ['冠军', '亚军', '第三名', '第四名', '第五名', 
                    '第六名', '第七名', '第八名', '第九名', '第十名',
                    '百位', '十位', '个位', '第1球', '第2球', '第3球', '第4球', '第5球']
        
        for position in positions:
            for opposites in self.config.opposite_groups:
                if len(opposites) == 2:
                    dir1, dir2 = list(opposites)
                    if n_accounts == 2:
                        valid_combinations.append({
                            'directions': [f"{position}-{dir1}", f"{position}-{dir2}"],
                            'dir1_count': 1,
                            'dir2_count': 1,
                            'opposite_type': f"{position}-{dir1} vs {position}-{dir2}"
                        })
                    else:
                        for i in range(1, n_accounts):
                            j = n_accounts - i
                            valid_combinations.append({
                                'directions': [f"{position}-{dir1}"] * i + [f"{position}-{dir2}"] * j,
                                'dir1_count': i,
                                'dir2_count': j,
                                'opposite_type': f"{position}-{dir1} vs {position}-{dir2}"
                            })
        
        return valid_combinations
    
    def _detect_combinations_for_period(self, period_data, period_accounts, n_accounts, valid_combinations):
        """为单个期号检测组合 - 修复版"""
        patterns = []
        
        # 获取当前彩种
        lottery = period_data['原始彩种'].iloc[0] if '原始彩种' in period_data.columns else period_data['彩种'].iloc[0]
        
        # 🎯 构建账户信息字典
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
            # 检查账户期数差异
            if not self._check_account_period_difference(account_group, lottery):
                continue
            
            group_directions = []
            group_amounts = []
            
            for account in account_group:
                if account in account_info and account_info[account]:
                    first_bet = account_info[account][0]
                    group_directions.append(first_bet['direction'])
                    group_amounts.append(first_bet['amount'])
            
            if len(group_directions) != n_accounts:
                continue
            
            # 🎯 检查是否匹配任何有效的方向组合
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
                    
                    # 检查金额相似度
                    similarity_threshold = self.config.account_count_similarity_thresholds.get(
                        n_accounts, self.config.amount_similarity_threshold
                    )
                    
                    if dir1_total > 0 and dir2_total > 0:
                        similarity = min(dir1_total, dir2_total) / max(dir1_total, dir2_total)
                        
                        if similarity >= similarity_threshold:
                            lottery_type = period_data['彩种类型'].iloc[0] if '彩种类型' in period_data.columns else '未知'
                            
                            # 🎯 修复模式字符串生成
                            if ' vs ' in combo['opposite_type']:
                                # 带位置的对立类型，如 "第3球-小 vs 第3球-大"
                                pattern_parts = combo['opposite_type'].split(' vs ')
                                if len(pattern_parts) == 2:
                                    dir1_part = pattern_parts[0].split('-')
                                    dir2_part = pattern_parts[1].split('-')
                                    if len(dir1_part) == 2 and len(dir2_part) == 2:
                                        # 格式：位置-方向(数量个) vs 位置-方向(数量个)
                                        pattern_str = f"{dir1_part[0]}-{dir1_part[1]}({combo['dir1_count']}个) vs {dir2_part[0]}-{dir2_part[1]}({combo['dir2_count']}个)"
                                    else:
                                        pattern_str = f"{pattern_parts[0]}({combo['dir1_count']}个) vs {pattern_parts[1]}({combo['dir2_count']}个)"
                                else:
                                    pattern_str = combo['opposite_type']
                            else:
                                # 基础对立类型，如 "大-小"
                                opposite_parts = combo['opposite_type'].split('-')
                                if len(opposite_parts) == 2:
                                    pattern_str = f"{opposite_parts[0]}({combo['dir1_count']}个) vs {opposite_parts[1]}({combo['dir2_count']}个)"
                                else:
                                    pattern_str = combo['opposite_type']
                            
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
                                '模式': pattern_str,  # 🎯 使用修复后的模式字符串
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
        """优化版的连续对刷模式检测"""
        if not wash_records:
            return []
        
        account_group_patterns = defaultdict(list)
        for record in wash_records:
            account_group_key = (tuple(sorted(record['账户组'])), record['彩种'])
            account_group_patterns[account_group_key].append(record)
        
        continuous_patterns = []
        
        for (account_group, lottery), records in account_group_patterns.items():
            sorted_records = sorted(records, key=lambda x: x['期号'])
            
            # 根据新的阈值要求确定最小对刷期数
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
                
                # 🎯 优化主要对立类型显示
                main_opposite_type = max(opposite_type_counts.items(), key=lambda x: x[1])[0]
                # 如果主要对立类型包含 " vs "，则进行格式化
                if ' vs ' in main_opposite_type:
                    parts = main_opposite_type.split(' vs ')
                    if len(parts) == 2:
                        # 提取位置和方向，格式化为 "位置-方向1-方向2"
                        pos_dir1 = parts[0].split('-')
                        pos_dir2 = parts[1].split('-')
                        if len(pos_dir1) >= 2 and len(pos_dir2) >= 2:
                            # 假设位置相同，只显示一次位置
                            position = pos_dir1[0]  # 取第一个位置
                            dir1 = pos_dir1[-1]     # 取最后一个部分作为方向
                            dir2 = pos_dir2[-1]     # 取最后一个部分作为方向
                            main_opposite_type = f"{position}-{dir1}-{dir2}"
                        else:
                            main_opposite_type = f"{parts[0]}-{parts[1].split('-')[-1]}" if '-' in parts[1] else f"{parts[0]}-{parts[1]}"
                
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

    def _calculate_detailed_account_stats(self, patterns):
        """计算详细账户统计 - 调整列名以匹配图片格式"""
        account_participation = defaultdict(lambda: {
            'periods': set(),
            'lotteries': set(),
            'positions': set(),
            'total_combinations': 0,
            'total_bet_amount': 0,
            'continuous_periods': 0,
            'actual_bet_records': []
        })
        
        # 从原始数据中收集账户的实际投注金额
        if self.df_valid is not None:
            for _, row in self.df_valid.iterrows():
                account = row['会员账号']
                amount = row['投注金额']
                period = row['期号']
                lottery = row['彩种'] if '彩种' in row else '未知'
                
                if account in account_participation:
                    account_participation[account]['actual_bet_records'].append({
                        'amount': amount,
                        'period': period,
                        'lottery': lottery
                    })
        
        # 收集账户参与信息
        for pattern in patterns:
            for account in pattern['账户组']:
                account_info = account_participation[account]
                
                # 添加期号
                for record in pattern['详细记录']:
                    account_info['periods'].add(record['期号'])
                
                # 添加彩种
                account_info['lotteries'].add(pattern['彩种'])
                
                # 添加位置信息
                for record in pattern['详细记录']:
                    for direction in record['方向组']:
                        if '-' in direction:
                            position = direction.split('-')[0]
                            account_info['positions'].add(position)
                
                account_info['total_combinations'] += 1
                account_info['continuous_periods'] = max(account_info['continuous_periods'], pattern['对刷期数'])
                
                # 计算该账户在对刷模式中的实际投注金额
                pattern_bet_amount = 0
                for record in pattern['详细记录']:
                    for acc, amt in zip(record['账户组'], record['金额组']):
                        if acc == account:
                            pattern_bet_amount += amt
                
                account_info['total_bet_amount'] += pattern_bet_amount
        
        # 转换为显示格式 - 调整列名以匹配图片
        account_stats = []
        for account, info in account_participation.items():
            stat_record = {
                '账户': account,
                '参与组合数': info['total_combinations'],
                '涉及期数': len(info['periods']),  # 对应图片中的"涉及指数"
                '涉及彩种': len(info['lotteries']),  # 对应图片中的"涉及时间"
                '总投注金额': info['total_bet_amount'],  # 对应图片中的"总投放金额"
                '平均每组金额': info['total_bet_amount'] / info['total_combinations'] if info['total_combinations'] > 0 else 0  # 对应图片中的"平均投放金额"
            }
            
            account_stats.append(stat_record)
        
        return sorted(account_stats, key=lambda x: x['总投注金额'], reverse=True)

    def exclude_multi_direction_accounts(self, df_valid):
        """排除同一账户多方向下注"""
        multi_direction_mask = (
            df_valid.groupby(['期号', '会员账号'])['投注方向']
            .transform('nunique') > 1
        )
        
        df_filtered = df_valid[~multi_direction_mask].copy()
        
        return df_filtered
    
    def get_account_group_activity_level(self, account_group, lottery):
        """获取活跃度水平"""
        if lottery not in self.account_total_periods_by_lottery:
            return 'unknown'
        
        total_periods_stats = self.account_total_periods_by_lottery[lottery]
        
        # 计算账户组中在指定彩种的最小总投注期数
        min_total_periods = min(total_periods_stats.get(account, 0) for account in account_group)
        
        # 按照新的活跃度阈值
        if min_total_periods <= self.config.period_thresholds['low_activity']:
            return 'low'        # 总投注期数1-10
        elif min_total_periods <= self.config.period_thresholds['medium_activity_high']:
            return 'medium'     # 总投注期数11-50
        elif min_total_periods <= self.config.period_thresholds['high_activity_high']:
            return 'high'       # 总投注期数51-100
        else:
            return 'very_high'  # 总投注期数100以上
    
    def get_required_min_periods(self, account_group, lottery):
        """根据新的活跃度阈值获取所需的最小对刷期数"""
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
        """显示详细检测结果 - 确保只有一个总体统计"""
        st.write("\n" + "="*60)
        st.write("🎯 多账户对刷检测结果")
        st.write("="*60)
        
        if not patterns:
            st.error("❌ 未发现符合阈值条件的连续对刷模式")
            return
    
        # ========== 只显示一个总体统计 ==========
        # 直接在这里显示总体统计，不再调用单独的display_summary_statistics方法
        self._display_compact_summary(patterns)
        
        st.write("\n" + "="*60)
        
        # ========== 显示参与账户详细统计 ==========
        st.subheader("👥 参与账户详细统计")
        
        # 计算账户参与统计
        account_stats = self._calculate_detailed_account_stats(patterns)
        
        if account_stats:
            df_stats = pd.DataFrame(account_stats)
            
            # 使用表格形式展示
            st.dataframe(
                df_stats,
                use_container_width=True,
                hide_index=True,
                height=min(400, len(df_stats) * 35 + 38)
            )
        
        # ========== 按彩种分组显示详细对刷组 ==========
        st.write("\n" + "="*60)
        st.subheader("🔍 详细对刷组分析")
        
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
                        
                        st.write(f"{j}. 期号: {record['期号']} | 方向: {' ↔ '.join(account_directions)} | 匹配度: {record['相似度']:.2%}")
                    
                    if i < len(lottery_patterns):
                        st.markdown("---")
        
        # 调用修改后的总体统计显示
        self.display_summary_statistics(patterns)
    
    def _display_compact_summary(self, patterns):
        """显示紧凑版总体统计 - 根据您提供的图片格式"""
        if not patterns:
            return
            
        st.subheader("📊 总体统计")
        
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
        
        # ========== 第一行：关键指标 ==========
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("总对刷组数", total_groups)
        
        with col2:
            st.metric("涉及账户数", total_accounts)
        
        with col3:
            st.metric("总对刷期数", total_wash_periods)
        
        with col4:
            st.metric("总涉及金额", f"¥{total_amount:,.2f}")
        
        # ========== 第二行：彩种类型统计 ==========
        st.subheader("🎲 彩种类型统计")
        
        # 创建彩种统计列
        lottery_cols = st.columns(min(5, len(lottery_stats)))
        
        for i, (lottery, count) in enumerate(lottery_stats.items()):
            if i < len(lottery_cols):
                with lottery_cols[i]:
                    st.metric(
                        label=lottery,
                        value=f"{count}组"
                    )
        
        # ========== 第三行：两列布局 ==========
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.subheader("👥 账户组合分布")
            
            for account_count, group_count in sorted(account_count_stats.items()):
                # 计算该类型组合的总对刷期数
                account_type_periods = sum(p['对刷期数'] for p in patterns if p['账户数量'] == account_count)
                st.write(f"- **{account_count}组**: {group_count}组 ({account_type_periods}期)")
        
        with col_right:
            st.subheader("📈 活跃度分布")
            
            activity_display_names = {
                'low': '低活跃度',
                'medium': '中活跃度',
                'high': '高活跃度',
                'very_high': '极高活跃度'
            }
            
            for activity, count in activity_stats.items():
                display_name = activity_display_names.get(activity, activity)
                # 计算该活跃度的总对刷期数
                activity_periods = sum(p['对刷期数'] for p in patterns if p['账户活跃度'] == activity)
                st.write(f"- **{display_name}**: {count}组 ({activity_periods}期)")
        
        # ========== 第四行：关键指标 ==========
        st.subheader("📈 关键指标")
        
        # 计算平均每组金额
        avg_group_amount = total_amount / total_groups if total_groups > 0 else 0
        
        metric_col1, metric_col2, metric_col3 = st.columns(3)
        
        with metric_col1:
            st.metric("平均每组金额", f"¥{avg_group_amount:,.2f}")
        
        with metric_col2:
            # 业务类型总额
            business_total = total_amount
            st.metric("业务类型总额", f"¥{business_total:,.2f}")
        
        with metric_col3:
            # 参与总账户数
            st.metric("参与总账户数", total_accounts)
        
        # ========== 第五行：主要对立类型 ==========
        st.subheader("🎯 主要对立类型")
        
        # 显示前3个主要对立类型
        top_opposites = sorted(opposite_type_stats.items(), key=lambda x: x[1], reverse=True)[:3]
        
        for opposite_type, count in top_opposites:
            # 简化对立类型显示
            if ' vs ' in opposite_type:
                display_type = opposite_type.replace(' vs ', '-')
            else:
                display_type = opposite_type
            st.write(f"- **{display_type}**: {count}期")

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
            
            # 账户期数差异阈值配置
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
                # ========== 🆕 修复这里：正确的数据处理流程 ==========
                # 直接调用 upload_and_process，它会内部处理列名识别和数据验证
                df_enhanced, filename = detector.upload_and_process(uploaded_file)
                
                if df_enhanced is not None and len(df_enhanced) > 0:
                    st.success("✅ 数据解析完成")
                    
                    # ========== 🆕 新增这里：显示数据质量验证结果 ==========
                    # 在数据处理器中已经有数据验证，这里只是显示结果
                    with st.expander("📊 数据质量验证结果", expanded=False):
                        # 这里可以显示detector中已经进行的验证结果
                        st.info("数据质量验证已在处理过程中完成")
                    
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
                        tab1, tab2 = st.tabs(["数据概览", "彩种分布"])
                        
                        with tab1:
                            st.dataframe(df_enhanced.head(100), use_container_width=True)
                            
                        with tab2:
                            if '彩种类型' in df_enhanced.columns:
                                lottery_type_stats = df_enhanced['彩种类型'].value_counts()
                                st.bar_chart(lottery_type_stats)
                    
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

        **🎲 支持的方向检测：**
        - **基础方向**：大、小、单、双、龙、虎、质、合
        - **变异形式**：特大、特小、总和单、总和大等（自动映射到基础方向）
        - **位置精度**：冠军到第十名、百位十位个位等精确位置判断

        **⚡ 自动检测：**
        - 数据上传后自动开始处理和分析
        - 无需手动点击开始检测按钮
        """)

if __name__ == "__main__":
    main()
