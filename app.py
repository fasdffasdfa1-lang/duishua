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
        self.amount_similarity_threshold = 0.8
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
class LotteryIdentifier:
    def __init__(self):
        self.lottery_configs = LOTTERY_CONFIGS
        # 添加通用彩种关键词识别
        self.general_keywords = {
            'PK10': ['pk10', 'pk拾', '飞艇', '赛车', '赛車', '幸运10', '北京赛车', '极速赛车'],
            'K3': ['快三', '快3', 'k3', 'k三', '骰宝', '三军'],
            'LHC': ['六合', 'lhc', '六合彩', '⑥合', '6合', '特码', '平特', '连肖'],
            'SSC': ['时时彩', 'ssc', '分分彩', '時時彩', '重庆时时彩', '腾讯分分彩'],
            'THREE_COLOR': ['三色', '三色彩', '三色球'],
            # 可以继续添加更多彩种类型
            '11选5': ['11选5', '十一选五', '广东11选5', '山东11选5'],
            '3D': ['3d', '福彩3d', '体彩3d', '排列三'],
            'KL8': ['快乐8', '快乐8', 'kl8', 'keno'],
            'MARK_SIX': ['mark six', '万字票', '数字彩']
        }
        
        # 彩种别名映射
        self.lottery_aliases = {
            '分分PK拾': 'PK10', '三分PK拾': 'PK10', '五分PK拾': 'PK10',
            '新幸运飞艇': 'PK10', '澳洲幸运10': 'PK10', '一分PK10': 'PK10',
            '宾果PK10': 'PK10', '极速飞艇': 'PK10', '澳洲飞艇': 'PK10',
            '幸运赛车': 'PK10', '分分赛车': 'PK10', '北京PK10': 'PK10',
            '旧北京PK10': 'PK10', '极速赛车': 'PK10', '幸运赛車': 'PK10',
            '北京赛车': 'PK10', '极速PK10': 'PK10', '幸运PK10': 'PK10',
            # K3 别名
            '分分快三': 'K3', '三分快3': 'K3', '五分快3': 'K3', '澳洲快三': 'K3',
            '宾果快三': 'K3', '1分快三': 'K3', '3分快三': 'K3', '5分快三': 'K3',
            '10分快三': 'K3', '加州快三': 'K3', '幸运快三': 'K3', '大发快三': 'K3',
            '澳门快三': 'K3', '香港快三': 'K3', '江苏快三': 'K3',
            # LHC 别名
            '新澳门六合彩': 'LHC', '澳门六合彩': 'LHC', '香港六合彩': 'LHC',
            '一分六合彩': 'LHC', '五分六合彩': 'LHC', '三分六合彩': 'LHC',
            '香港⑥合彩': 'LHC', '分分六合彩': 'LHC', '快乐6合彩': 'LHC',
            '港⑥合彩': 'LHC', '台湾大乐透': 'LHC', '大发六合彩': 'LHC',
            # SSC 别名
            '分分时时彩': 'SSC', '三分时时彩': 'SSC', '五分时时彩': 'SSC',
            '宾果时时彩': 'SSC', '1分时时彩': 'SSC', '3分时时彩': 'SSC',
            '5分时时彩': 'SSC', '旧重庆时时彩': 'SSC', '幸运时时彩': 'SSC',
            '腾讯分分彩': 'SSC', '新疆时时彩': 'SSC', '天津时时彩': 'SSC',
            '重庆时时彩': 'SSC', '上海时时彩': 'SSC', '广东时时彩': 'SSC',
            # 三色彩别名
            '一分三色彩': 'THREE_COLOR', '30秒三色彩': 'THREE_COLOR',
            '五分三色彩': 'THREE_COLOR', '三分三色彩': 'THREE_COLOR'
        }

    def identify_lottery_type(self, lottery_name):
        """增强的彩种类型识别 - 自动学习新彩种"""
        lottery_str = str(lottery_name).strip()
        
        # 1. 首先检查别名映射
        if lottery_str in self.lottery_aliases:
            return self.lottery_aliases[lottery_str]
        
        # 2. 检查预设彩种列表
        for lottery_type, config in self.lottery_configs.items():
            for lottery in config['lotteries']:
                if lottery in lottery_str:
                    return lottery_type
        
        lottery_lower = lottery_str.lower()
        
        # 3. 使用关键词识别
        for lottery_type, keywords in self.general_keywords.items():
            for keyword in keywords:
                if keyword.lower() in lottery_lower:
                    return lottery_type
        
        # 4. 智能模式匹配
        if self._is_pk10_like(lottery_lower):
            return 'PK10'
        elif self._is_k3_like(lottery_lower):
            return 'K3'
        elif self._is_lhc_like(lottery_lower):
            return 'LHC'
        elif self._is_ssc_like(lottery_lower):
            return 'SSC'
        elif self._is_three_color_like(lottery_lower):
            return 'THREE_COLOR'
        elif self._is_11x5_like(lottery_lower):
            return '11选5'
        elif self._is_3d_like(lottery_lower):
            return '3D'
        elif self._is_kl8_like(lottery_lower):
            return 'KL8'
        
        # 5. 如果还是无法识别，记录并返回原名称，而不是"未知彩种"
        return lottery_str  # 返回原名称而不是"未知彩种"

    def _is_pk10_like(self, lottery_lower):
        """判断是否为PK10类彩种"""
        pk10_patterns = [
            r'.*pk.*10.*', r'.*pk.*拾.*', r'.*飞艇.*', r'.*赛车.*', 
            r'.*幸运.*10.*', r'.*北京.*车.*', r'.*极速.*车.*'
        ]
        return any(re.search(pattern, lottery_lower) for pattern in pk10_patterns)

    def _is_k3_like(self, lottery_lower):
        """判断是否为快三类彩种"""
        k3_patterns = [r'.*快三.*', r'.*快3.*', r'.*k3.*', r'.*骰宝.*', r'.*三军.*']
        return any(re.search(pattern, lottery_lower) for pattern in k3_patterns)

    def _is_lhc_like(self, lottery_lower):
        """判断是否为六合彩类彩种"""
        lhc_patterns = [r'.*六合.*', r'.*lhc.*', r'.*特码.*', r'.*平特.*', r'.*连肖.*']
        return any(re.search(pattern, lottery_lower) for pattern in lhc_patterns)

    def _is_ssc_like(self, lottery_lower):
        """判断是否为时时彩类彩种"""
        ssc_patterns = [r'.*时时彩.*', r'.*ssc.*', r'.*分分彩.*', r'.*\d星.*', r'.*定位.*']
        return any(re.search(pattern, lottery_lower) for pattern in ssc_patterns)

    def _is_three_color_like(self, lottery_lower):
        """判断是否为三色彩类彩种"""
        return '三色' in lottery_lower

    def _is_11x5_like(self, lottery_lower):
        """判断是否为11选5类彩种"""
        patterns = [r'.*11选5.*', r'.*十一选五.*', r'.*\d选\d.*']
        return any(re.search(pattern, lottery_lower) for pattern in patterns)

    def _is_3d_like(self, lottery_lower):
        """判断是否为3D类彩种"""
        patterns = [r'.*3d.*', r'.*福彩.*', r'.*体彩.*', r'.*排列三.*']
        return any(re.search(pattern, lottery_lower) for pattern in patterns)

    def _is_kl8_like(self, lottery_lower):
        """判断是否为快乐8类彩种"""
        patterns = [r'.*快乐8.*', r'.*keno.*', r'.*kl8.*']
        return any(re.search(pattern, lottery_lower) for pattern in patterns)

    def learn_new_lottery(self, lottery_name, lottery_type):
        """学习新的彩种映射"""
        self.lottery_aliases[lottery_name] = lottery_type
        # 这里可以添加将新学习的彩种保存到文件或数据库的逻辑

    def analyze_lottery_distribution(self, df):
        """分析彩种分布并识别未知彩种"""
        if '彩种' not in df.columns:
            return {}
        
        lottery_counts = df['彩种'].value_counts()
        identified_lotteries = {}
        unknown_lotteries = {}
        
        for lottery, count in lottery_counts.items():
            lottery_type = self.identify_lottery_type(lottery)
            if lottery_type == lottery:  # 如果返回原名称，说明是未知彩种
                unknown_lotteries[lottery] = count
            else:
                identified_lotteries[lottery] = lottery_type
        
        return {
            'identified': identified_lotteries,
            'unknown': unknown_lotteries,
            'total_identified': len(identified_lotteries),
            'total_unknown': len(unknown_lotteries)
        }

# ==================== 从第一套代码移植的玩法分类器 ====================
class PlayCategoryNormalizer:
    def __init__(self):
        self.category_mapping = self._create_category_mapping()
    
    def _create_category_mapping(self):
        """创建玩法分类映射的完整映射"""
        mapping = {
            # 快三玩法
            '和值': '和值',
            '和值_大小单双': '和值',
            '两面': '两面',
            '二不同号': '二不同号',
            '三不同号': '三不同号',
            '独胆': '独胆',
            '点数': '和值',
            '三军': '独胆',
            '三軍': '独胆',
            '三军_大小': '独胆',
            '三军_单双': '独胆',
            
            # 六合彩玩法完整映射
            '特码': '特码',
            '正1特': '正1特',
            '正码特_正一特': '正1特',
            '正2特': '正2特',
            '正码特_正二特': '正2特',
            '正3特': '正3特',
            '正码特_正三特': '正3特',
            '正4特': '正4特',
            '正码特_正四特': '正4特',
            '正5特': '正5特',
            '正码特_正五特': '正5特',
            '正6特': '正6特',
            '正码特_正六特': '正6特',
            '正码': '正码',
            '正特': '正特',
            '正玛特': '正特',
            '正码1-6': '正码',
            
            # 尾数相关玩法独立映射
            '尾数': '尾数',
            '尾数_头尾数': '尾数_头尾数',
            '特尾': '特尾',
            '全尾': '全尾',
            '尾数_正特尾数': '尾数',
            
            # 其他六合彩玩法
            '特肖': '特肖',
            '生肖_特肖': '特肖',
            '平特': '平特',
            '生肖_正肖': '平特',
            '生肖_一肖': '一肖',
            '连肖': '连肖',
            '连尾': '连尾',
            '龙虎': '龙虎',
            '五行': '五行',

            # 波色相关玩法
            '色波': '色波',
            '七色波': '色波',
            '波色': '色波',

            #半波相关玩法映射
            '半波': '半波',
            '蓝波': '半波',
            '绿波': '半波',
            '红波': '半波',
            '半波_红波': '半波',
            '半波_蓝波': '半波',
            '半波_绿波': '半波',

            # 正码1-6相关映射
            '正码1-6': '正码1-6',
            '正码1~6': '正码1-6',
            '正码1-6特': '正码1-6',
            '正码1~6特': '正码1-6',
            
            # 时时彩玩法
            '斗牛': '斗牛',
            '1-5球': '1-5球',
            '第1球': '第1球',
            '第2球': '第2球',
            '第3球': '第3球',
            '第4球': '第4球',
            '第5球': '第5球',
            '总和': '总和',
            '正码': '正码',
            '正码特': '正码',
            '正码_特': '正码',
            '定位胆': '定位胆',
            '定位_万位': '定位_万位',
            '定位_千位': '定位_千位',
            '定位_百位': '定位_百位',
            '定位_十位': '定位_十位',
            '定位_个位': '定位_个位',
            '两面': '两面',
            
            # PK拾/赛车玩法
            '前一': '冠军',
            '定位胆': '定位胆',
            '1-5名': '1-5名',
            '6-10名': '6-10名',
            '冠军': '冠军',
            '亚军': '亚军',
            '季军': '第三名',
            '第3名': '第三名',
            '第4名': '第四名',
            '第5名': '第五名',
            '第6名': '第六名',
            '第7名': '第七名',
            '第8名': '第八名',
            '第9名': '第九名',
            '第10名': '第十名',
            '双面': '两面',
            '冠亚和': '冠亚和',
            '冠亚和_大小单双': '冠亚和_大小单双',
            '冠亚和_和值': '冠亚和_和值',
            
            # 大小单双独立玩法
            '大小_冠军': '大小_冠军',
            '大小_亚军': '大小_亚军',
            '大小_季军': '大小_季军',
            '单双_冠军': '单双_冠军',
            '单双_亚军': '单双_亚军',
            '单双_季军': '单双_季军',
            
            # 龙虎独立玩法
            '龙虎_冠军': '龙虎_冠军',
            '龙虎_冠 军': '龙虎_冠军',
            '龙虎_亚军': '龙虎_亚军',
            '龙虎_亚 军': '龙虎_亚军',
            '龙虎_季军': '龙虎_季军',
            '龙虎_季 军': '龙虎_季军',
            
            # 定位胆细分
            '定位胆_第1~5名': '定位胆_第1~5名',
            '定位胆_第6~10名': '定位胆_第6~10名',
            '定位胆_1~5': '定位胆_第1~5名',
            '定位胆_6~10': '定位胆_第6~10名',
            '定位胆_1-5': '定位胆_第1~5名', 
            '定位胆_6-10': '定位胆_第6~10名',
            '定位胆_1~5名': '定位胆_第1~5名',
            '定位胆_6~10名': '定位胆_第6~10名',
            
            # 大小单双玩法变体
            '大小单双': '两面',
            '大小': '大小',
            '单双': '单双',
            
            # 龙虎玩法变体
            '龙虎斗': '龙虎',
            '冠亚龙虎': '龙虎_冠军',
            '冠军龙虎': '龙虎_冠军',
            
            # 时时彩定位胆变体
            '定位_万位': '定位_万位',
            '定位_千位': '定位_千位', 
            '定位_百位': '定位_百位',
            '定位_十位': '定位_十位',
            '定位_个位': '定位_个位',
            '万位': '定位_万位',
            '千位': '定位_千位',
            '百位': '定位_百位',
            '十位': '定位_十位',
            '个位': '定位_个位',
            
            # 六合彩玩法变体
            '特码A': '特码',
            '特码B': '特码', 
            '正码A': '正码',
            '正码B': '正码',
            '正码1': '正1特',
            '正码2': '正2特',
            '正码3': '正3特',
            '正码4': '正4特',
            '正码5': '正5特',
            '正码6': '正6特',
            
            # 三色彩
            '正码': '正码',
            '两面': '两面',
            '色波': '色波',
            '特码': '特码'
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
        
        category_lower = category_str.lower()
        
        # PK10/赛车智能匹配
        if any(word in category_lower for word in ['定位胆_第1~5名', '定位胆1~5', '定位胆1-5']):
            return '定位胆_第1~5名'
        elif any(word in category_lower for word in ['定位胆_第6~10名', '定位胆6~10', '定位胆6-10']):
            return '定位胆_第6~10名'
        elif any(word in category_lower for word in ['1-5名', '1~5名', '1-5', '1~5']):
            return '1-5名'
        elif any(word in category_lower for word in ['6-10名', '6~10名', '6-10', '6~10']):
            return '6-10名'
        elif any(word in category_lower for word in ['冠军', '第一名', '第1名', '1st']):
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
        elif any(word in category_lower for word in ['前一']):
            return '冠军'
        
        # 时时彩定位胆智能匹配
        elif any(word in category_lower for word in ['万位', '第一位', '第一球']):
            return '定位_万位'
        elif any(word in category_lower for word in ['千位', '第二位', '第二球']):
            return '定位_千位'
        elif any(word in category_lower for word in ['百位', '第三位', '第三球']):
            return '定位_百位'
        elif any(word in category_lower for word in ['十位', '第四位', '第四球']):
            return '定位_十位'
        elif any(word in category_lower for word in ['个位', '第五位', '第五球']):
            return '定位_个位'
        elif any(word in category_lower for word in ['定位胆']):
            return '定位胆'
        
        # 六合彩智能匹配
        elif any(word in category_lower for word in ['特码']):
            return '特码'
        elif any(word in category_lower for word in ['正码']):
            return '正码'
        elif any(word in category_lower for word in ['正特', '正玛特']):
            return '正特'
        elif any(word in category_lower for word in ['尾数']):
            return '尾数'
        elif any(word in category_lower for word in ['平特']):
            return '平特'
        elif any(word in category_lower for word in ['特肖']):
            return '特肖'
        elif any(word in category_lower for word in ['一肖']):
            return '一肖'
        elif any(word in category_lower for word in ['连肖']):
            return '连肖'
        elif any(word in category_lower for word in ['连尾']):
            return '连尾'
        elif any(word in category_lower for word in ['龙虎']):
            return '龙虎'
        elif any(word in category_lower for word in ['五行']):
            return '五行'
        elif any(word in category_lower for word in ['色波', '七色波', '波色']):
            return '色波'
        elif any(word in category_lower for word in ['半波']):
            return '半波'
        
        # 快三智能匹配
        elif any(word in category_lower for word in ['和值', '点数']):
            return '和值'
        elif any(word in category_lower for word in ['独胆', '三军', '三軍']):
            return '独胆'
        elif any(word in category_lower for word in ['二不同号']):
            return '二不同号'
        elif any(word in category_lower for word in ['三不同号']):
            return '三不同号'
        
        return category_str

# ==================== 增强的对刷检测器 ====================
class WashTradeDetector:
    def __init__(self, config=None):
        self.config = config or Config()
        self.data_processor = DataProcessor()
        self.lottery_identifier = LotteryIdentifier()
        self.play_normalizer = PlayCategoryNormalizer()
        
        self.data_processed = False
        self.df_valid = None
        self.export_data = []
        
        # 修正：按彩种存储账户总投注期数统计
        self.account_total_periods_by_lottery = defaultdict(dict)
        self.account_record_stats_by_lottery = defaultdict(dict)
        self.column_mapping_used = {}
        self.performance_stats = {}
    
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
                # 增强的数据处理
                df_enhanced = self.enhance_data_processing(df_clean)
                return df_enhanced, filename
            else:
                return None, None
            
        except Exception as e:
            logger.error(f"文件处理失败: {str(e)}")
            st.error(f"文件处理失败: {str(e)}")
            return None, None
    
    def enhance_data_processing(self, df_clean):
        """增强的数据处理流程 - 修复彩种名称显示问题"""
        try:
            # 0. 先分析彩种分布
            lottery_analysis = self.lottery_identifier.analyze_lottery_distribution(df_clean)
            
            # 显示彩种分析结果
            if lottery_analysis['total_unknown'] > 0:
                st.warning(f"发现 {lottery_analysis['total_unknown']} 个新彩种，系统正在自动学习...")
                with st.expander("🔍 新彩种详情", expanded=True):
                    st.write("**新发现的彩种:**")
                    for lottery, count in lottery_analysis['unknown'].items():
                        st.write(f"- {lottery}: {count} 条记录")
            
            # 1. 彩种识别 - 保留原始彩种名称，同时添加彩种类型
            if '彩种' in df_clean.columns:
                # 保存原始彩种名称
                df_clean['原始彩种'] = df_clean['彩种']
                
                # 添加彩种类型分类
                df_clean['彩种类型'] = df_clean['彩种'].apply(self.lottery_identifier.identify_lottery_type)
                
                # 显示彩种识别统计
                identified_stats = df_clean['彩种类型'].value_counts()
                with st.expander("🎯 彩种识别统计", expanded=False):
                    st.dataframe(identified_stats.reset_index().rename(
                        columns={'index': '彩种类型', '彩种类型': '数量'}
                    ))
            
            # 2. 玩法分类统一
            if '玩法' in df_clean.columns:
                df_clean['玩法分类'] = df_clean['玩法'].apply(self.play_normalizer.normalize_category)
            
            # 3. 计算账户统计信息 - 使用原始彩种名称
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
            
            # 显示账户统计信息
            self.display_account_statistics(df_valid)
            
            return df_valid
            
        except Exception as e:
            logger.error(f"数据处理增强失败: {str(e)}")
            st.error(f"数据处理增强失败: {str(e)}")
            return pd.DataFrame()
    
    def display_account_statistics(self, df_valid):
        """显示账户统计信息"""
        with st.expander("📊 账户统计信息", expanded=False):
            # 显示每个彩种的账户统计
            for lottery in df_valid['原始彩种'].unique():
                df_lottery = df_valid[df_valid['原始彩种'] == lottery]
                account_stats = df_lottery.groupby('会员账号').agg({
                    '期号': 'nunique',
                    '投注金额': 'count'
                }).rename(columns={'期号': '投注期数', '投注金额': '记录数'})
                
                st.write(f"**{lottery}** 账户统计:")
                st.dataframe(account_stats.head(20))  # 只显示前20个账户
    
    def extract_bet_amount_safe(self, amount_text):
        """安全提取投注金额 - 改进版本"""
        try:
            if pd.isna(amount_text):
                return 0
            
            text = str(amount_text).strip()
            
            # 首先尝试直接转换
            try:
                cleaned_text = text.replace(',', '').replace('，', '').replace(' ', '')
                if re.match(r'^-?\d+(\.\d+)?$', cleaned_text):
                    amount = float(cleaned_text)
                    if amount >= self.config.min_amount:
                        return amount
            except:
                pass
            
            # 使用多种模式匹配
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
            
            # 最后尝试提取所有数字
            numbers = re.findall(r'\d+\.?\d*', text)
            if numbers:
                try:
                    amount = float(numbers[0])
                    if amount >= self.config.min_amount:
                        return amount
                except:
                    pass
            
            return 0
            
        except Exception as e:
            logger.warning(f"金额提取失败: {amount_text}, 错误: {e}")
            return 0
    
    def enhanced_extract_direction(self, content):
        """增强的投注方向提取 - 结合玩法分类"""
        try:
            if pd.isna(content):
                return ""
            
            content_str = str(content).strip().lower()
            
            # 基础方向提取
            for direction, patterns in self.config.direction_patterns.items():
                for pattern in patterns:
                    if pattern.lower() in content_str:
                        return direction
            
            return ""
        except Exception as e:
            logger.warning(f"方向提取失败: {content}, 错误: {e}")
            return ""
    
    def calculate_account_total_periods_by_lottery(self, df):
        """修正：按彩种计算每个账户的总投注期数统计（使用原始彩种名称）"""
        self.account_total_periods_by_lottery = defaultdict(dict)
        self.account_record_stats_by_lottery = defaultdict(dict)
        
        # 使用原始彩种名称进行分组，而不是彩种类型
        lottery_col = '原始彩种' if '原始彩种' in df.columns else '彩种'
        
        for lottery in df[lottery_col].unique():
            df_lottery = df[df[lottery_col] == lottery]
            
            # 计算每个账户的总投注期数（唯一期号数）
            period_counts = df_lottery.groupby('会员账号')['期号'].nunique().to_dict()
            self.account_total_periods_by_lottery[lottery] = period_counts
            
            # 计算每个账户的记录数
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
        """优化版的N个账户对刷模式检测 - 使用原始彩种名称"""
        wash_records = []
        
        # 使用原始彩种名称进行分组，而不是彩种类型
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
        """获取有效的方向组合 - 修复版本"""
        valid_combinations = []
        
        # 对于2个账户：标准的对立组
        if n_accounts == 2:
            for opposites in self.config.opposite_groups:
                dir1, dir2 = list(opposites)
                valid_combinations.append({
                    'directions': [dir1, dir2],
                    'dir1_count': 1,
                    'dir2_count': 1,
                    'opposite_type': f"{dir1}-{dir2}"
                })
        
        # 对于3个及以上账户：允许多种分布
        else:
            for opposites in self.config.opposite_groups:
                dir1, dir2 = list(opposites)
                
                # 对于n个账户，允许从1到n-1的各种分布
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
        """为单个期号检测组合 - 修复版本"""
        patterns = []
        
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
            # 检查这个账户组是否有有效的方向组合
            group_directions = []
            group_amounts = []
            
            # 收集这个账户组的所有方向和金额
            for account in account_group:
                if account in account_info:
                    # 只取每个账户的第一个投注记录（假设一个账户在一个期号只有一个方向）
                    if account_info[account]:
                        first_bet = account_info[account][0]
                        group_directions.append(first_bet['direction'])
                        group_amounts.append(first_bet['amount'])
            
            # 如果收集到的方向数量不等于账户数量，跳过
            if len(group_directions) != n_accounts:
                continue
            
            # 检查是否匹配任何有效的方向组合
            for combo in valid_combinations:
                target_directions = combo['directions']
                
                # 检查实际方向是否与目标方向匹配（考虑顺序）
                actual_directions_sorted = sorted(group_directions)
                target_directions_sorted = sorted(target_directions)
                
                if actual_directions_sorted == target_directions_sorted:
                    # 计算两个方向的总金额
                    dir1_total = 0
                    dir2_total = 0
                    dir1 = combo['opposite_type'].split('-')[0]
                    
                    for direction, amount in zip(group_directions, group_amounts):
                        if direction == dir1:
                            dir1_total += amount
                        else:
                            dir2_total += amount
                    
                    # 检查金额相似度
                    if dir1_total > 0 and dir2_total > 0:
                        similarity = min(dir1_total, dir2_total) / max(dir1_total, dir2_total)
                        
                        if similarity >= self.config.amount_similarity_threshold:
                            # 获取彩种信息 - 使用原始彩种名称
                            lottery = period_data['原始彩种'].iloc[0] if '原始彩种' in period_data.columns else period_data['彩种'].iloc[0]
                            lottery_type = period_data['彩种类型'].iloc[0] if '彩种类型' in period_data.columns else '未知'
                            
                            record = {
                                '期号': period_data['期号'].iloc[0],
                                '彩种': lottery,  # 使用原始彩种名称
                                '彩种类型': lottery_type,  # 添加彩种类型
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
    
    def find_continuous_patterns_optimized(self, wash_records):
        """优化版的连续对刷模式检测 - 使用原始彩种名称"""
        if not wash_records:
            return []
        
        account_group_patterns = defaultdict(list)
        for record in wash_records:
            # 使用原始彩种名称进行分组
            account_group_key = (tuple(sorted(record['账户组'])), record['彩种'])
            account_group_patterns[account_group_key].append(record)
        
        continuous_patterns = []
        
        for (account_group, lottery), records in account_group_patterns.items():
            sorted_records = sorted(records, key=lambda x: x['期号'])
            
            # 修正：根据账户组的总投注期数确定最小对刷期数要求
            required_min_periods = self.get_required_min_periods(account_group, lottery)
            
            # 调试信息
            account_count = len(account_group)
            if account_count > 2:  # 只对3个及以上账户显示调试信息
                st.write(f"  调试: 账户组{account_group}在{lottery}有{len(sorted_records)}期对刷，要求{required_min_periods}期")
            
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
                
                # 修正：显示每个账户的详细统计信息
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
                    '彩种': lottery,  # 完整的原始彩种名称
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
        """修正：根据账户组在特定彩种的总投注期数获取活跃度水平"""
        if lottery not in self.account_total_periods_by_lottery:
            return 'unknown'
        
        total_periods_stats = self.account_total_periods_by_lottery[lottery]
        
        # 计算账户组中在指定彩种的最小总投注期数（用于活跃度判断）
        min_total_periods = min(total_periods_stats.get(account, 0) for account in account_group)
        
        # 按照您要求的活跃度阈值设置
        if min_total_periods <= self.config.period_thresholds['low_activity']:
            return 'low'        # 总投注期数≤10
        elif min_total_periods <= self.config.period_thresholds['medium_activity_high']:
            return 'medium'     # 总投注期数11-200
        else:
            return 'high'       # 总投注期数≥201
    
    def get_required_min_periods(self, account_group, lottery):
        """修正：根据账户组的总投注期数活跃度获取所需的最小对刷期数"""
        activity_level = self.get_account_group_activity_level(account_group, lottery)
        
        if activity_level == 'low':
            return self.config.period_thresholds['min_periods_low']    # 3期
        elif activity_level == 'medium':
            return self.config.period_thresholds['min_periods_medium'] # 5期
        else:
            return self.config.period_thresholds['min_periods_high']   # 8期
    
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
                
                if self.performance_stats['detection_time'] > 0:
                    records_per_second = self.performance_stats['total_records'] / self.performance_stats['detection_time']
                    st.write(f"- 处理速度: {records_per_second:.1f} 条记录/秒")
    
    def display_detailed_results(self, patterns):
        """显示详细检测结果 - 使用完整的原始彩种名称"""
        st.write("\n" + "="*60)
        st.write("🎯 多账户对刷检测结果")
        st.write("="*60)
        
        if not patterns:
            st.error("❌ 未发现符合阈值条件的连续对刷模式")
            return
        
        # 按完整的原始彩种名称分组
        patterns_by_lottery = defaultdict(list)
        for pattern in patterns:
            # 使用完整的原始彩种名称进行分组
            lottery_key = pattern['彩种']
            patterns_by_lottery[lottery_key].append(pattern)
        
        for lottery, lottery_patterns in patterns_by_lottery.items():
            # 使用expander包装每个彩种，默认展开
            with st.expander(f"🎲 彩种：{lottery}（发现{len(lottery_patterns)}组）", expanded=True):
                for i, pattern in enumerate(lottery_patterns, 1):
                    # 对刷组信息
                    st.markdown(f"**对刷组 {i}:** {' ↔ '.join(pattern['账户组'])}")
                    
                    # 活跃度信息
                    activity_icon = "🟢" if pattern['账户活跃度'] == 'low' else "🟡" if pattern['账户活跃度'] == 'medium' else "🔴"
                    st.markdown(f"**活跃度:** {activity_icon} {pattern['账户活跃度']} | **彩种:** {pattern['彩种']} | **主要类型:** {pattern['主要对立类型']}")
                    
                    # 账户统计信息
                    st.markdown(f"**账户在该彩种投注期数/记录数:** {', '.join(pattern['账户统计信息'])}")
                    
                    # 对刷期数
                    st.markdown(f"**对刷期数:** {pattern['对刷期数']}期 (要求≥{pattern['要求最小对刷期数']}期)")
                    
                    # 金额信息
                    st.markdown(f"**总金额:** {pattern['总投注金额']:.2f}元 | **平均匹配:** {pattern['平均相似度']:.2%}")
                    
                    # 详细记录 - 直接展开显示
                    st.markdown("**详细记录:**")
                    for j, record in enumerate(pattern['详细记录'], 1):
                        account_directions = []
                        for account, direction, amount in zip(record['账户组'], record['方向组'], record['金额组']):
                            account_directions.append(f"{account}({direction}:{amount})")
                        
                        st.markdown(f"{j}. **期号:** {record['期号']} | **模式:** {record['模式']} | **方向:** {' ↔ '.join(account_directions)} | **匹配度:** {record['相似度']:.2%}")
                    
                    # 对刷组之间的分隔线
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
        
        # 活跃度分布
        activity_stats = defaultdict(int)
        for pattern in patterns:
            activity_stats[pattern['账户活跃度']] += 1
        
        # 对立类型分布
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
    
    def export_to_excel(self, patterns, filename):
        """导出检测结果到Excel文件"""
        if not patterns:
            st.error("❌ 没有对刷数据可导出")
            return None, None
        
        export_data = []
        
        for group_idx, pattern in enumerate(patterns, 1):
            for record_idx, record in enumerate(pattern['详细记录'], 1):
                account_directions = []
                for account, direction, amount in zip(record['账户组'], record['方向组'], record['金额组']):
                    account_directions.append(f"{account}({direction}:{amount})")
                
                export_data.append({
                    '对刷组编号': group_idx,
                    '账户组': ' ↔ '.join(pattern['账户组']),
                    '彩种': pattern['彩种'],
                    '账户数量': pattern['账户数量'],
                    '账户活跃度': pattern['账户活跃度'],
                    '账户统计信息': ', '.join(pattern['账户统计信息']),
                    '要求最小对刷期数': pattern['要求最小对刷期数'],
                    '主要对立类型': pattern['主要对立类型'],
                    '对立类型分布': str(pattern['对立类型分布']),
                    '对刷期数': pattern['对刷期数'],
                    '总投注金额': pattern['总投注金额'],
                    '平均相似度': f"{pattern['平均相似度']:.2%}",
                    '模式分布': str(pattern['模式分布']),
                    '期号': record['期号'],
                    '对立类型': record['对立类型'],
                    '模式': record['模式'],
                    '金额': record['总金额'],
                    '匹配度': f"{record['相似度']:.2%}",
                    '账户方向': ' | '.join(account_directions)
                })
        
        df_export = pd.DataFrame(export_data)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_filename = f"对刷检测报告_智能版_{timestamp}.xlsx"
        
        try:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_export.to_excel(writer, sheet_name='详细记录', index=False)
                
                summary_data = []
                for group_idx, pattern in enumerate(patterns, 1):
                    summary_data.append({
                        '对刷组编号': group_idx,
                        '账户组': ' ↔ '.join(pattern['账户组']),
                        '彩种': pattern['彩种'],
                        '账户数量': pattern['账户数量'],
                        '账户活跃度': pattern['账户活跃度'],
                        '账户统计信息': ', '.join(pattern['账户统计信息']),
                        '要求最小对刷期数': pattern['要求最小对刷期数'],
                        '主要对立类型': pattern['主要对立类型'],
                        '对立类型分布': str(pattern['对立类型分布']),
                        '对刷期数': pattern['对刷期数'],
                        '总投注金额': pattern['总投注金额'],
                        '平均相似度': f"{pattern['平均相似度']:.2%}",
                        '模式分布': str(pattern['模式分布'])
                    })
                
                df_summary = pd.DataFrame(summary_data)
                df_summary.to_excel(writer, sheet_name='对刷组汇总', index=False)
            
            output.seek(0)
            st.success(f"✅ Excel报告已生成: {export_filename}")
            
            return output, export_filename
            
        except Exception as e:
            st.error(f"❌ 导出Excel失败: {str(e)}")
            return None, None

# ==================== 主函数 ====================
def main():
    """主函数"""
    st.title("🎯 智能多账户对刷检测系统")
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
            
            detector = WashTradeDetector(config)
            
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
                            st.metric("彩种类型数", f"{df_enhanced['彩种类型'].nunique()}")
                    
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
            - 智能金额匹配分析
            - 活跃度自适应阈值
            - 实时进度监控
            """)
        
        with col2:
            st.subheader("📊 专业分析")
            st.markdown("""
            - 完整彩种支持
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
    
    # 使用说明
    with st.expander("📖 系统使用说明", expanded=False):
        st.markdown("""
        ### 系统功能说明

        **🎯 检测逻辑：**
        - **总投注期数**：账户在特定彩种中的所有期号投注次数
        - **对刷期数**：账户组实际发生对刷行为的期数
        - 根据**总投注期数**判定账户活跃度，设置不同的**对刷期数**阈值

        **📊 活跃度判定：**
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

        **⚡ 自动检测：**
        - 数据上传后自动开始处理和分析
        - 无需手动点击开始检测按钮
        """)

if __name__ == "__main__":
    main()
