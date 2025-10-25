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

# 配置日志和警告
warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('K3WashTrade')

# Streamlit 页面配置
st.set_page_config(
    page_title="快三多账户对刷检测系统",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

print("=== 🎯 快三多账户对刷检测系统（多格式兼容版）===")
print("=" * 50)

class Config:
    """配置参数类"""
    def __init__(self):
        self.min_amount = 10
        self.amount_similarity_threshold = 0.9
        self.min_continuous_periods = 3
        self.max_accounts_in_group = 5
        self.supported_file_types = ['.xlsx', '.xls', '.csv']
        
        # 列名映射配置
        self.column_mappings = {
            '会员账号': ['会员账号', '会员账户', '账号', '账户', '用户账号'],
            '彩种': ['彩种', '彩票种类', '游戏类型'],
            '期号': ['期号', '期数', '期次', '期'],
            '玩法': ['玩法', '玩法分类', '投注类型', '类型'],
            '内容': ['内容', '投注内容', '下注内容', '注单内容'],
            '金额': ['金额', '下注总额', '投注金额', '总额', '下注金额']
        }
        
        # 新增：根据账户投注期数设置不同的对刷期数阈值
        self.period_thresholds = {
            'low_activity': 10,  # 低活跃度账户阈值
            'min_periods_low': 3,   # 低活跃度账户最小对刷期数
            'min_periods_high': 5   # 高活跃度账户最小对刷期数
        }
        
        self.direction_patterns = {
            '小': ['两面-小', '和值-小', '小', 'small', 'xia'],
            '大': ['两面-大', '和值-大', '大', 'big', 'da'], 
            '单': ['两面-单', '和值-单', '单', 'odd', 'dan'],
            '双': ['两面-双', '和值-双', '双', 'even', 'shuang']
        }
        
        self.opposite_groups = [{'大', '小'}, {'单', '双'}]

class WashTradeDetector:
    def __init__(self, config=None):
        self.config = config or Config()
        self.data_processed = False
        self.df_valid = None
        self.export_data = []
        # 修改：按彩种存储账户投注期数统计
        self.account_period_stats_by_lottery = defaultdict(dict)
        self.account_record_stats_by_lottery = defaultdict(dict)  # 新增：记录数统计
        self.column_mapping_used = {}  # 记录使用的列名映射
    
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
            
            if filename.endswith('.csv'):
                df = pd.read_csv(uploaded_file, encoding='utf-8')
            else:
                df = pd.read_excel(uploaded_file)
            
            logger.info(f"原始数据维度: {df.shape}")
            st.info(f"原始数据列名: {df.columns.tolist()}")
            
            return df, filename
            
        except Exception as e:
            logger.error(f"文件处理失败: {str(e)}")
            st.error(f"文件处理失败: {str(e)}")
            return None, None
    
    def map_columns(self, df):
        """映射列名到标准格式"""
        st.write("\n🔄 开始列名映射...")
        
        # 创建反向映射：从可能的列名到标准列名
        reverse_mapping = {}
        for standard_col, possible_cols in self.config.column_mappings.items():
            for col in possible_cols:
                reverse_mapping[col] = standard_col
        
        # 查找匹配的列
        column_mapping = {}
        used_columns = set()
        
        for df_col in df.columns:
            df_col_clean = str(df_col).strip()
            
            # 尝试完全匹配
            if df_col_clean in reverse_mapping:
                standard_col = reverse_mapping[df_col_clean]
                if standard_col not in used_columns:
                    column_mapping[df_col] = standard_col
                    used_columns.add(standard_col)
                    st.write(f"  ✅ 列映射: '{df_col}' -> '{standard_col}'")
                continue
            
            # 尝试部分匹配
            for possible_col in reverse_mapping.keys():
                if possible_col in df_col_clean:
                    standard_col = reverse_mapping[possible_col]
                    if standard_col not in used_columns:
                        column_mapping[df_col] = standard_col
                        used_columns.add(standard_col)
                        st.write(f"  ✅ 列映射: '{df_col}' -> '{standard_col}'")
                    break
        
        # 重命名列
        if column_mapping:
            df_renamed = df.rename(columns=column_mapping)
            self.column_mapping_used = column_mapping
            st.success(f"✅ 列名映射完成，使用了 {len(column_mapping)} 个映射")
            return df_renamed
        else:
            st.warning("❌ 无法识别列名格式，使用原始列名")
            return df
    
    def check_required_columns(self, df):
        """检查必要列是否存在"""
        required_cols = ['会员账号', '期号', '内容', '金额']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            st.error(f"❌ 缺少必要列: {missing_cols}")
            st.write("可用的列:", df.columns.tolist())
            return False
        
        # 检查彩种列，如果没有则创建
        if '彩种' not in df.columns:
            df['彩种'] = '未知彩种'
            st.warning("⚠️ 未找到彩种列，已添加默认彩种列")
        
        st.success(f"✅ 必要列检查通过")
        return True
    
    def parse_column_data(self, df):
        """解析列结构数据"""
        st.write("\n开始解析列结构数据...")
        
        try:
            # 第一步：列名映射
            df_mapped = self.map_columns(df)
            
            # 第二步：检查必要列
            if not self.check_required_columns(df_mapped):
                return pd.DataFrame()
            
            # 数据清理
            df_clean = df_mapped[['会员账号', '期号', '内容', '金额', '彩种']].copy()
            df_clean = df_clean.dropna(subset=['会员账号', '期号', '内容', '金额'])
            
            # 对每个列单独处理
            for col in ['会员账号', '期号', '内容', '彩种']:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].astype(str).str.strip()
            
            st.write(f"清洗后记录数: {len(df_clean)}")
            
            # 提取投注金额
            df_clean['投注金额'] = df_clean['金额'].apply(lambda x: self.extract_bet_amount(x))
            
            # 提取投注方向
            df_clean['投注方向'] = df_clean['内容'].apply(lambda x: self.extract_direction_from_content(x))
            
            # 过滤有效记录
            df_valid = df_clean[
                (df_clean['投注方向'] != '') & 
                (df_clean['投注金额'] >= self.config.min_amount)
            ].copy()
            
            if len(df_valid) == 0:
                st.error("❌ 过滤后没有有效记录")
                return pd.DataFrame()
            
            # 修改：按彩种计算每个账户的投注期数统计
            self.calculate_account_period_stats_by_lottery(df_valid)
            
            st.write(f"\n=== 数据统计 ===")
            st.write(f"总记录数: {len(df_clean)}")
            st.write(f"有效记录数: {len(df_valid)}")
            st.write(f"唯一期号数: {df_valid['期号'].nunique()}")
            st.write(f"唯一账户数: {df_valid['会员账号'].nunique()}")
            
            # 彩种分布统计
            if len(df_valid) > 0:
                lottery_stats = df_valid['彩种'].value_counts()
                st.write(f"彩种分布: {dict(lottery_stats)}")
            
            # 方向分布统计
            if len(df_valid) > 0:
                direction_stats = df_valid['投注方向'].value_counts()
                st.write(f"投注方向分布: {dict(direction_stats)}")
            
            # 新增：显示账户投注期数分布（按彩种）
            self.display_account_period_distribution_by_lottery()
            
            # 新增：显示详细账户统计（调试用）
            self.display_detailed_account_stats()
            
            self.data_processed = True
            self.df_valid = df_valid
            return df_valid
            
        except Exception as e:
            logger.error(f"数据解析失败: {str(e)}")
            st.error(f"数据解析失败: {str(e)}")
            st.error(f"详细错误: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def calculate_account_period_stats_by_lottery(self, df_valid):
        """按彩种计算每个账户的投注期数统计 - 修复版本"""
        st.write("\n📊 按彩种计算账户投注期数统计...")
        
        # 重置统计字典
        self.account_period_stats_by_lottery = defaultdict(dict)
        self.account_record_stats_by_lottery = defaultdict(dict)
        
        # 按彩种和账户分组，计算每个账户在每个彩种的投注期数和记录数
        for lottery in df_valid['彩种'].unique():
            df_lottery = df_valid[df_valid['彩种'] == lottery]
            
            # 计算每个账户的投注期数（唯一期号数）
            period_counts = df_lottery.groupby('会员账号')['期号'].nunique().to_dict()
            self.account_period_stats_by_lottery[lottery] = period_counts
            
            # 计算每个账户的记录数
            record_counts = df_lottery.groupby('会员账号').size().to_dict()
            self.account_record_stats_by_lottery[lottery] = record_counts
        
        # 计算每个彩种的统计信息
        for lottery, period_stats in self.account_period_stats_by_lottery.items():
            total_accounts = len(period_stats)
            if total_accounts > 0:
                periods = list(period_stats.values())
                max_periods = max(periods)
                min_periods = min(periods)
                avg_periods = np.mean(periods)
                
                # 计算记录数统计
                record_stats = self.account_record_stats_by_lottery[lottery]
                total_records = sum(record_stats.values())
                avg_records = total_records / total_accounts
                
                # 计算不同活跃度账户数量
                low_activity = sum(1 for periods in period_stats.values() 
                                 if periods < self.config.period_thresholds['low_activity'])
                high_activity = total_accounts - low_activity
                
                st.write(f"\n🎲 彩种 '{lottery}' 账户统计:")
                st.write(f"  总账户数: {total_accounts}")
                st.write(f"  总记录数: {total_records}")
                st.write(f"  最大投注期数: {max_periods}")
                st.write(f"  最小投注期数: {min_periods}")
                st.write(f"  平均投注期数: {avg_periods:.1f}")
                st.write(f"  平均记录数: {avg_records:.1f}")
                st.write(f"  低活跃账户(<{self.config.period_thresholds['low_activity']}期): {low_activity}个")
                st.write(f"  高活跃账户(≥{self.config.period_thresholds['low_activity']}期): {high_activity}个")
    
    def display_detailed_account_stats(self):
        """显示详细账户统计（用于调试）"""
        st.write("\n🔍 详细账户统计（前20个账户）:")
        
        for lottery, period_stats in self.account_period_stats_by_lottery.items():
            st.write(f"\n🎯 彩种 '{lottery}':")
            
            # 按记录数排序，显示前20个账户
            record_stats = self.account_record_stats_by_lottery[lottery]
            sorted_accounts = sorted(record_stats.items(), key=lambda x: x[1], reverse=True)[:20]
            
            for account, record_count in sorted_accounts:
                period_count = period_stats.get(account, 0)
                st.write(f"  账户: {account} | 记录数: {record_count} | 期数: {period_count}")
    
    def display_account_period_distribution_by_lottery(self):
        """按彩种显示账户投注期数分布"""
        if not self.account_period_stats_by_lottery:
            return
        
        # 定义分布区间
        bins = [0, 3, 5, 10, 20, 30, 50, 100, 1000]
        bin_labels = ['1-3期', '4-5期', '6-10期', '11-20期', '21-30期', '31-50期', '51-100期', '100+期']
        
        for lottery, account_stats in self.account_period_stats_by_lottery.items():
            periods = list(account_stats.values())
            
            distribution = {}
            for i in range(len(bins) - 1):
                count = sum(1 for p in periods if bins[i] <= p < bins[i+1])
                if count > 0:
                    distribution[bin_labels[i]] = count
            
            if distribution:
                st.write(f"\n📈 彩种 '{lottery}' 账户投注期数分布:")
                for range_label, count in distribution.items():
                    percentage = (count / len(periods)) * 100
                    st.write(f"  {range_label}: {count}个账户 ({percentage:.1f}%)")
    
    def extract_bet_amount(self, amount_text):
        """从复杂文本中提取投注金额"""
        try:
            if pd.isna(amount_text):
                return 0
            
            text = str(amount_text).strip()
            
            # 先尝试直接转换
            try:
                cleaned_text = text.replace(',', '').replace('，', '')
                amount = float(cleaned_text)
                if amount >= self.config.min_amount:
                    return amount
            except:
                pass
            
            # 多种金额提取模式
            patterns = [
                r'投注[:：]?\s*(\d+[,，]?\d*\.?\d*)',
                r'投注\s*(\d+[,，]?\d*\.?\d*)',
                r'金额[:：]?\s*(\d+[,，]?\d*\.?\d*)',
                r'(\d+[,，]?\d*\.?\d*)\s*元',
                r'￥\s*(\d+[,，]?\d*\.?\d*)',
                r'¥\s*(\d+[,，]?\d*\.?\d*)',
                r'(\d+[,，]?\d*\.?\d*)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    amount_str = match.group(1).replace(',', '').replace('，', '')
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
    
    def extract_direction_from_content(self, content):
        """从内容列提取投注方向"""
        try:
            if pd.isna(content):
                return ""
            
            content_str = str(content).strip().lower()
            
            for direction, patterns in self.config.direction_patterns.items():
                for pattern in patterns:
                    if pattern.lower() in content_str:
                        return direction
            
            return ""
        except Exception as e:
            logger.warning(f"方向提取失败: {content}, 错误: {e}")
            return ""
    
    def detect_all_wash_trades(self):
        """检测所有类型的对刷交易"""
        if not self.data_processed or self.df_valid is None or len(self.df_valid) == 0:
            st.error("❌ 没有有效数据可用于检测")
            return []
        
        st.write(f"\n=== 开始多账户对刷检测 (2-{self.config.max_accounts_in_group}个账户) ===")
        
        # 排除同一账户多方向下注
        df_filtered = self.exclude_multi_direction_accounts(self.df_valid)
        
        if len(df_filtered) == 0:
            st.error("❌ 过滤后无有效数据")
            return []
        
        all_patterns = []
        
        # 检测不同账户数量的对刷
        for account_count in range(2, self.config.max_accounts_in_group + 1):
            st.write(f"\n🔍 检测{account_count}个账户对刷...")
            patterns = self.detect_n_account_patterns(df_filtered, account_count)
            all_patterns.extend(patterns)
            st.write(f"  发现 {len(patterns)} 个{account_count}账户对刷组")
        
        return all_patterns
    
    def exclude_multi_direction_accounts(self, df_valid):
        """排除同一账户多方向下注"""
        st.write("排除同一账户多方向下注...")
        multi_direction_accounts = set()
        
        account_period_groups = df_valid.groupby(['期号', '会员账号'])
        for (period, account), group in account_period_groups:
            directions = group['投注方向'].unique()
            if len(directions) > 1:
                multi_direction_accounts.add((period, account))
        
        df_filtered = df_valid.copy()
        mask = df_filtered.apply(
            lambda row: (row['期号'], row['会员账号']) not in multi_direction_accounts, 
            axis=1
        )
        df_filtered = df_filtered[mask]
        
        st.write(f"过滤后记录数: {len(df_filtered)}")
        return df_filtered
    
    def detect_n_account_patterns(self, df_filtered, n_accounts):
        """检测N个账户对刷模式"""
        wash_records = []
        
        # 按期号和彩种分组
        period_groups = df_filtered.groupby(['期号', '彩种'])
        
        for (period, lottery), period_data in period_groups:
            period_accounts = period_data['会员账号'].unique()
            
            if len(period_accounts) < n_accounts:
                continue
            
            for account_group in combinations(period_accounts, n_accounts):
                group_data = period_data[period_data['会员账号'].isin(account_group)]
                if len(group_data) != n_accounts:
                    continue
                
                # 检查方向一致性
                result = self._check_direction_consistency(group_data)
                if not result['valid']:
                    continue
                
                opposite_type = result['opposite_type']
                dir1, dir2 = opposite_type.split('-')
                
                # 计算两个方向的总金额
                dir1_total = group_data[group_data['投注方向'] == dir1]['投注金额'].sum()
                dir2_total = group_data[group_data['投注方向'] == dir2]['投注金额'].sum()
                
                if dir1_total == 0 or dir2_total == 0:
                    continue
                
                similarity = min(dir1_total, dir2_total) / max(dir1_total, dir2_total)
                
                if similarity >= self.config.amount_similarity_threshold:
                    direction_counts = group_data['投注方向'].value_counts()
                    dir1_count = direction_counts.get(dir1, 0)
                    dir2_count = direction_counts.get(dir2, 0)
                    
                    record = {
                        '期号': period,
                        '彩种': lottery,
                        '账户组': list(account_group),
                        '方向组': group_data['投注方向'].tolist(),
                        '金额组': group_data['投注金额'].tolist(),
                        '总金额': dir1_total + dir2_total,
                        '相似度': similarity,
                        '账户数量': n_accounts,
                        '模式': f"{dir1}({dir1_count}个) vs {dir2}({dir2_count}个)",
                        '对立类型': opposite_type
                    }
                    
                    wash_records.append(record)
        
        return self.find_continuous_patterns_corrected(wash_records)
    
    def _check_direction_consistency(self, group_data):
        """检查方向一致性"""
        direction_counts = group_data['投注方向'].value_counts()
        directions = set(direction_counts.index)
        
        for opposites in self.config.opposite_groups:
            if directions.issubset(opposites):
                opposite_type = f"{list(opposites)[0]}-{list(opposites)[1]}"
                return {
                    'valid': True,
                    'opposite_type': opposite_type,
                    'directions': directions
                }
        
        return {'valid': False}
    
    def get_account_group_activity_level(self, account_group, lottery):
        """获取账户组在特定彩种的活跃度水平"""
        if lottery not in self.account_period_stats_by_lottery:
            return 'unknown'
        
        lottery_stats = self.account_period_stats_by_lottery[lottery]
        
        # 计算账户组中在指定彩种的最小投注期数
        min_periods = min(lottery_stats.get(account, 0) for account in account_group)
        
        if min_periods < self.config.period_thresholds['low_activity']:
            return 'low'
        else:
            return 'high'
    
    def get_required_min_periods(self, account_group, lottery):
        """根据账户组在特定彩种的活跃度获取所需的最小对刷期数"""
        activity_level = self.get_account_group_activity_level(account_group, lottery)
        
        if activity_level == 'low':
            return self.config.period_thresholds['min_periods_low']
        else:
            return self.config.period_thresholds['min_periods_high']
    
    def find_continuous_patterns_corrected(self, wash_records):
        """修复的连续对刷模式检测 - 按彩种计算账户活跃度"""
        if not wash_records:
            return []
        
        # 按账户组和彩种分组
        account_group_patterns = defaultdict(list)
        for record in wash_records:
            account_group_key = (tuple(sorted(record['账户组'])), record['彩种'])
            account_group_patterns[account_group_key].append(record)
        
        continuous_patterns = []
        
        for (account_group, lottery), records in account_group_patterns.items():
            # 按期号排序
            sorted_records = sorted(records, key=lambda x: x['期号'])
            
            # 修改：根据账户在特定彩种的活跃度确定最小对刷期数要求
            required_min_periods = self.get_required_min_periods(account_group, lottery)
            activity_level = self.get_account_group_activity_level(account_group, lottery)
            
            # 应用新的阈值规则
            if len(sorted_records) >= required_min_periods:
                # 统计总金额和平均相似度
                total_investment = sum(r['总金额'] for r in sorted_records)
                avg_similarity = np.mean([r['相似度'] for r in sorted_records])
                
                # 分析对立类型分布
                opposite_type_counts = defaultdict(int)
                for record in sorted_records:
                    opposite_type_counts[record['对立类型']] += 1
                
                # 分析模式分布
                patterns = [r['模式'] for r in sorted_records]
                pattern_count = defaultdict(int)
                for pattern in patterns:
                    pattern_count[pattern] += 1
                
                # 主要对立类型
                main_opposite_type = max(opposite_type_counts.items(), key=lambda x: x[1])[0]
                
                # 获取账户组在指定彩种的投注期数信息
                lottery_stats = self.account_period_stats_by_lottery.get(lottery, {})
                record_stats = self.account_record_stats_by_lottery.get(lottery, {})
                account_periods_info = []
                for account in account_group:
                    periods = lottery_stats.get(account, 0)
                    records_count = record_stats.get(account, 0)
                    account_periods_info.append(f"{account}({periods}期/{records_count}记录)")
                
                continuous_patterns.append({
                    '账户组': list(account_group),
                    '彩种': lottery,
                    '账户数量': len(account_group),
                    '主要对立类型': main_opposite_type,
                    '对立类型分布': dict(opposite_type_counts),
                    '总期数': len(sorted_records),
                    '总投注金额': total_investment,
                    '平均相似度': avg_similarity,
                    '模式分布': dict(pattern_count),
                    '详细记录': sorted_records,
                    # 修改字段：按彩种计算
                    '账户活跃度': activity_level,
                    '账户投注期数': account_periods_info,
                    '最小投注期数': min(lottery_stats.get(account, 0) for account in account_group),
                    '要求最小对刷期数': required_min_periods
                })
                
                st.write(f"  🎯 发现对刷组: {len(account_group)}账户组[{activity_level}活跃] 彩种:{lottery} 总{len(sorted_records)}期(要求≥{required_min_periods}期) 总金额:{total_investment:.2f}")
        
        return continuous_patterns
    
    def display_detailed_results(self, patterns):
        """显示详细检测结果 - 将统计放到最后"""
        st.write("\n" + "="*80)
        st.write("🎯 多账户对刷检测最终结果")
        st.write("="*80)
        
        if not patterns:
            st.error("❌ 未发现符合阈值条件的连续对刷模式")
            return
        
        # 按彩种分组
        patterns_by_lottery = defaultdict(list)
        for pattern in patterns:
            patterns_by_lottery[pattern['彩种']].append(pattern)
        
        # 先显示详细对刷组信息
        for lottery, lottery_patterns in patterns_by_lottery.items():
            st.write(f"\n{'='*60}")
            st.write(f"🎲 彩种: {lottery} (发现{len(lottery_patterns)}组)")
            st.write(f"{'='*60}")
            
            # 按账户数量分组
            patterns_by_count = defaultdict(list)
            for pattern in lottery_patterns:
                patterns_by_count[pattern['账户数量']].append(pattern)
            
            for account_count in sorted(patterns_by_count.keys(), reverse=True):
                group_patterns = patterns_by_count[account_count]
                st.write(f"\n👥 {account_count} 个账户对刷组 (发现{len(group_patterns)}组)")
                
                for i, pattern in enumerate(group_patterns, 1):
                    st.write(f"\n🔍 对刷组 {i}:")
                    
                    # 简化的视觉标识：账户组
                    st.write(f"   🔥 账户组: {' ↔ '.join(pattern['账户组'])}")
                    
                    # 新增：显示账户活跃度信息（按彩种）
                    activity_icon = "🟡" if pattern['账户活跃度'] == 'low' else "🔴"
                    st.write(f"   {activity_icon} 活跃度: {pattern['账户活跃度']}活跃 (在该彩种最小投注期数: {pattern['最小投注期数']}期)")
                    st.write(f"   📊 账户在该彩种投注期数/记录数: {', '.join(pattern['账户投注期数'])}")
                    
                    # 简化的视觉标识：彩种
                    st.write(f"   🎯 彩种: {pattern['彩种']}")
                    
                    st.write(f"   主要对立类型: {pattern['主要对立类型']}")
                    st.write(f"   对立类型分布: {pattern['对立类型分布']}")
                    st.write(f"   总期数: {pattern['总期数']} 期 (要求≥{pattern['要求最小对刷期数']}期)")
                    st.write(f"   总投注金额: {pattern['总投注金额']:.2f} 元")
                    
                    # 简化的视觉标识：平均金额匹配
                    similarity_percent = pattern['平均相似度'] * 100
                    if similarity_percent >= 95:
                        symbol = "💯"  # 100%匹配
                        color_indicator = "🟢"  # 绿色高匹配
                    elif similarity_percent >= 90:
                        symbol = "✅"  # 高匹配
                        color_indicator = "🟡"  # 黄色中等匹配
                    else:
                        symbol = "⚠️"  # 警告
                        color_indicator = "🔴"  # 红色低匹配
                    
                    st.write(f"   {symbol} 平均金额匹配: {pattern['平均相似度']:.2%} {color_indicator}")
                    
                    st.write(f"   模式分布: {pattern['模式分布']}")
                    
                    st.write(f"   详细记录:")
                    for j, record in enumerate(pattern['详细记录'], 1):
                        st.write(f"     {j}. 期号: {record['期号']}")
                        st.write(f"         彩种: {record['彩种']}")
                        st.write(f"         对立类型: {record['对立类型']}")
                        st.write(f"         模式: {record['模式']}")
                        account_directions = list(zip(record['账户组'], record['方向组'], record['金额组']))
                        st.write(f"         账户方向: {account_directions}")
                        st.write(f"         匹配度: {record['相似度']:.2%}")
        
        # 最后显示总体统计
        self.display_summary_statistics(patterns)
    
    def display_summary_statistics(self, patterns):
        """显示总体统计 - 放在最后"""
        if not patterns:
            return
            
        st.write(f"\n{'='*80}")
        st.write("📊 总体统计")
        st.write(f"{'='*80}")
        
        total_groups = len(patterns)
        total_accounts = sum(p['账户数量'] for p in patterns)
        total_periods = sum(p['总期数'] for p in patterns)
        total_amount = sum(p['总投注金额'] for p in patterns)
        
        # 按账户数量统计
        account_count_stats = defaultdict(int)
        for pattern in patterns:
            account_count_stats[pattern['账户数量']] += 1
        
        # 按彩种统计
        lottery_stats = defaultdict(int)
        for pattern in patterns:
            lottery_stats[pattern['彩种']] += 1
        
        # 新增：按活跃度统计
        activity_stats = defaultdict(int)
        for pattern in patterns:
            activity_stats[pattern['账户活跃度']] += 1
        
        st.write(f"\n🎯 检测结果汇总:")
        st.write(f"   对刷组数: {total_groups} 组")
        st.write(f"   涉及账户: {total_accounts} 个")
        st.write(f"   总对刷期数: {total_periods} 期")
        st.write(f"   总涉及金额: {total_amount:.2f} 元")
        
        st.write(f"\n👥 按账户数量分布:")
        for account_count, count in sorted(account_count_stats.items()):
            st.write(f"   {account_count}个账户组: {count} 组")
        
        st.write(f"\n🎲 按彩种分布:")
        for lottery, count in lottery_stats.items():
            st.write(f"   {lottery}: {count} 组")
        
        st.write(f"\n📈 按活跃度分布:")
        for activity, count in activity_stats.items():
            st.write(f"   {activity}活跃账户组: {count} 组")
        
        st.write(f"\n💰 金额统计:")
        avg_amount_per_group = total_amount / total_groups if total_groups > 0 else 0
        avg_periods_per_group = total_periods / total_groups if total_groups > 0 else 0
        st.write(f"   平均每组金额: {avg_amount_per_group:.2f} 元")
        st.write(f"   平均每期金额: {total_amount/total_periods:.2f} 元" if total_periods > 0 else "   平均每期金额: 0 元")
        st.write(f"   平均每组期数: {avg_periods_per_group:.1f} 期")
        
        # 新增：显示阈值配置信息
        st.write(f"\n⚙️ 检测阈值配置:")
        st.write(f"   低活跃度账户阈值: <{self.config.period_thresholds['low_activity']}期")
        st.write(f"   低活跃度账户最小对刷期数: ≥{self.config.period_thresholds['min_periods_low']}期")
        st.write(f"   高活跃度账户最小对刷期数: ≥{self.config.period_thresholds['min_periods_high']}期")
        st.write(f"   📝 注意: 活跃度按彩种单独计算，显示格式为'期数/记录数'")
    
    def export_to_excel(self, patterns, filename):
        """导出检测结果到Excel文件"""
        if not patterns:
            st.error("❌ 没有对刷数据可导出")
            return None
        
        st.write(f"\n📊 正在生成Excel报告...")
        
        # 准备导出数据
        export_data = []
        
        for group_idx, pattern in enumerate(patterns, 1):
            for record_idx, record in enumerate(pattern['详细记录'], 1):
                # 格式化账户方向信息
                account_directions = []
                for account, direction, amount in zip(record['账户组'], record['方向组'], record['金额组']):
                    account_directions.append(f"{account}({direction}:{amount})")
                
                export_data.append({
                    '对刷组编号': group_idx,
                    '账户组': ' ↔ '.join(pattern['账户组']),
                    '彩种': pattern['彩种'],
                    '账户数量': pattern['账户数量'],
                    '账户活跃度': pattern['账户活跃度'],
                    '最小投注期数': pattern['最小投注期数'],
                    '要求最小对刷期数': pattern['要求最小对刷期数'],
                    '主要对立类型': pattern['主要对立类型'],
                    '对立类型分布': str(pattern['对立类型分布']),
                    '总期数': pattern['总期数'],
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
        
        # 创建DataFrame
        df_export = pd.DataFrame(export_data)
        
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_filename = f"对刷检测报告_多格式兼容版_{timestamp}.xlsx"
        
        # 导出到Excel
        try:
            # 创建Excel写入对象
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # 主表 - 详细记录
                df_export.to_excel(writer, sheet_name='详细记录', index=False)
                
                # 汇总表 - 对刷组统计
                summary_data = []
                for group_idx, pattern in enumerate(patterns, 1):
                    summary_data.append({
                        '对刷组编号': group_idx,
                        '账户组': ' ↔ '.join(pattern['账户组']),
                        '彩种': pattern['彩种'],
                        '账户数量': pattern['账户数量'],
                        '账户活跃度': pattern['账户活跃度'],
                        '最小投注期数': pattern['最小投注期数'],
                        '要求最小对刷期数': pattern['要求最小对刷期数'],
                        '主要对立类型': pattern['主要对立类型'],
                        '对立类型分布': str(pattern['对立类型分布']),
                        '总期数': pattern['总期数'],
                        '总投注金额': pattern['总投注金额'],
                        '平均相似度': f"{pattern['平均相似度']:.2%}",
                        '模式分布': str(pattern['模式分布'])
                    })
                
                df_summary = pd.DataFrame(summary_data)
                df_summary.to_excel(writer, sheet_name='对刷组汇总', index=False)
                
                # 统计表 - 总体统计
                stats_data = [{
                    '对刷组数': len(patterns),
                    '涉及账户数': sum(p['账户数量'] for p in patterns),
                    '总对刷期数': sum(p['总期数'] for p in patterns),
                    '总涉及金额': sum(p['总投注金额'] for p in patterns),
                    '平均每组金额': sum(p['总投注金额'] for p in patterns) / len(patterns) if patterns else 0,
                    '平均每期金额': sum(p['总投注金额'] for p in patterns) / sum(p['总期数'] for p in patterns) if patterns else 0,
                    '低活跃度账户阈值': self.config.period_thresholds['low_activity'],
                    '低活跃度最小对刷期数': self.config.period_thresholds['min_periods_low'],
                    '高活跃度最小对刷期数': self.config.period_thresholds['min_periods_high'],
                    '活跃度计算方式': '按彩种单独计算',
                    '列名映射': str(self.column_mapping_used),
                    '检测时间': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }]
                
                df_stats = pd.DataFrame(stats_data)
                df_stats.to_excel(writer, sheet_name='总体统计', index=False)
            
            output.seek(0)
            st.success(f"✅ Excel报告已生成: {export_filename}")
            st.write(f"📋 包含 {len(patterns)} 个对刷组的详细数据")
            
            return output, export_filename
            
        except Exception as e:
            st.error(f"❌ 导出Excel失败: {str(e)}")
            return None, None

def main():
    """主函数"""
    st.title("🎯 快三多账户对刷检测系统（多格式兼容版）")
    st.markdown("---")
    
    # 侧边栏配置
    st.sidebar.header("⚙️ 检测参数配置")
    
    min_amount = st.sidebar.number_input("最小投注金额", value=10, min_value=1, help="低于此金额的记录将被过滤")
    similarity_threshold = st.sidebar.slider("金额匹配度阈值", 0.8, 1.0, 0.9, 0.01, help="对立方向金额匹配度阈值")
    max_accounts = st.sidebar.slider("最大检测账户数", 2, 8, 5, help="检测的最大账户组合数量")
    
    # 活跃度阈值配置
    st.sidebar.subheader("活跃度阈值配置")
    low_activity_threshold = st.sidebar.number_input("低活跃度账户阈值(期)", value=10, min_value=1, help="低于此期数的账户视为低活跃度")
    min_periods_low = st.sidebar.number_input("低活跃度最小对刷期数", value=3, min_value=1, help="低活跃度账户最小连续对刷期数")
    min_periods_high = st.sidebar.number_input("高活跃度最小对刷期数", value=5, min_value=1, help="高活跃度账户最小连续对刷期数")
    
    # 文件上传
    st.header("📁 数据上传")
    uploaded_file = st.file_uploader(
        "请上传数据文件 (支持 .xlsx, .xls, .csv)", 
        type=['xlsx', 'xls', 'csv'],
        help="请确保文件包含必要的列：会员账号、期号、内容、金额"
    )
    
    if uploaded_file is not None:
        try:
            # 更新配置参数
            config = Config()
            config.min_amount = min_amount
            config.amount_similarity_threshold = similarity_threshold
            config.max_accounts_in_group = max_accounts
            config.period_thresholds = {
                'low_activity': low_activity_threshold,
                'min_periods_low': min_periods_low,
                'min_periods_high': min_periods_high
            }
            
            detector = WashTradeDetector(config)
            
            # 显示文件信息
            st.success(f"✅ 已上传文件: {uploaded_file.name}")
            
            # 解析数据
            with st.spinner("🔄 正在解析数据..."):
                df, filename = detector.upload_and_process(uploaded_file)
                if df is not None:
                    df_valid = detector.parse_column_data(df)
                    
                    if len(df_valid) > 0:
                        st.success("✅ 数据解析完成")
                        
                        # 检测对刷交易
                        if st.button("🚀 开始检测对刷交易", type="primary"):
                            with st.spinner("🔍 正在检测对刷交易..."):
                                patterns = detector.detect_all_wash_trades()
                            
                            # 显示结果
                            if patterns:
                                st.success(f"✅ 检测完成！发现 {len(patterns)} 个对刷组")
                                
                                # 显示详细结果
                                detector.display_detailed_results(patterns)
                                
                                # 导出Excel报告
                                excel_output, export_filename = detector.export_to_excel(patterns, filename)
                                
                                if excel_output is not None:
                                    st.download_button(
                                        label="📥 下载检测报告",
                                        data=excel_output,
                                        file_name=export_filename,
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                    )
                            else:
                                st.warning("⚠️ 未发现符合阈值条件的对刷行为")
                    else:
                        st.error("❌ 数据解析失败，请检查文件格式和内容")
            
        except Exception as e:
            st.error(f"❌ 程序执行失败: {str(e)}")
            st.error(f"详细错误信息:\n{traceback.format_exc()}")
    
    # 使用说明
    with st.expander("📖 使用说明"):
        st.markdown("""
        ### 系统功能说明
        
        **🎯 检测逻辑：**
        - 检测2-5个账户之间的对刷行为
        - 支持大-小、单-双等对立投注方向
        - 金额匹配度 ≥ 90%
        - 根据账户活跃度自适应阈值
        
        **📊 活跃度判定：**
        - **低活跃度账户**：投注期数 < 配置阈值，要求 ≥ 3期连续对刷
        - **高活跃度账户**：投注期数 ≥ 配置阈值，要求 ≥ 5期连续对刷
        
        **📁 数据格式要求：**
        - 必须包含：会员账号、期号、内容、金额
        - 可选包含：彩种（如无则自动添加默认值）
        - 支持自动列名映射
        
        **⚙️ 参数配置：**
        - 可在侧边栏调整检测参数
        - 支持实时预览配置效果
        """)
    
    # 系统信息
    st.sidebar.markdown("---")
    st.sidebar.info("""
    **系统信息**
    - 版本: 2.0
    - 更新: 2024
    - 作者: 快三检测系统
    """)

if __name__ == "__main__":
    main()
