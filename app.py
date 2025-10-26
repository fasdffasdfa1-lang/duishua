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
        
        # 更宽松的阈值配置（用于诊断）
        self.period_thresholds = {
            'low_activity_max': 100,     # 提高低活跃度阈值
            'medium1_activity_min': 101,  
            'medium1_activity_max': 500, 
            'medium2_activity_min': 501, 
            
            # 降低最小对刷期数要求
            'min_periods_low': 2,        
            'min_periods_medium1': 2,    
            'min_periods_medium2': 2,    
            
            # 放宽活跃度差异检查
            'max_period_difference': 1000  # 大幅放宽差异限制
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
        self.account_period_stats_by_lottery = defaultdict(dict)
        self.account_record_stats_by_lottery = defaultdict(dict)
        self.column_mapping_used = {}
        self.performance_stats = {}
        self.debug_info = []  # 新增：存储调试信息
    
    def add_debug_info(self, message):
        """添加调试信息"""
        self.debug_info.append(f"{datetime.now().strftime('%H:%M:%S')} - {message}")
    
    # 其他方法保持不变，只修改关键检测逻辑
    
    def detect_all_wash_trades(self):
        """检测所有类型的对刷交易 - 诊断版本"""
        if not self.data_processed or self.df_valid is None or len(self.df_valid) == 0:
            st.error("❌ 没有有效数据可用于检测")
            return []
        
        # 重置调试信息
        self.debug_info = []
        self.add_debug_info("开始对刷检测")
        
        # 性能统计
        self.performance_stats = {
            'start_time': datetime.now(),
            'total_records': len(self.df_valid),
            'total_periods': self.df_valid['期号'].nunique(),
            'total_accounts': self.df_valid['会员账号'].nunique()
        }
        
        self.add_debug_info(f"数据统计: {len(self.df_valid)}记录, {self.df_valid['期号'].nunique()}期号, {self.df_valid['会员账号'].nunique()}账户")
        
        # 排除同一账户多方向下注
        df_filtered = self.exclude_multi_direction_accounts(self.df_valid)
        self.add_debug_info(f"过滤多方向下注后: {len(df_filtered)}记录")
        
        if len(df_filtered) == 0:
            st.error("❌ 过滤后无有效数据")
            return []
        
        # 显示进度条
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        all_patterns = []
        total_steps = self.config.max_accounts_in_group - 1
        
        # 检测不同账户数量的对刷
        for account_count in range(2, self.config.max_accounts_in_group + 1):
            status_text.text(f"🔍 检测{account_count}个账户对刷模式...")
            self.add_debug_info(f"开始检测{account_count}账户组合")
            
            patterns = self.detect_n_account_patterns(df_filtered, account_count)
            self.add_debug_info(f"发现{account_count}账户对刷记录: {len(patterns)}条")
            
            all_patterns.extend(patterns)
            
            # 更新进度
            progress = (account_count - 1) / total_steps
            progress_bar.progress(progress)
        
        # 完成进度
        progress_bar.progress(1.0)
        status_text.text("✅ 检测完成")
        
        # 记录性能统计
        self.performance_stats['end_time'] = datetime.now()
        self.performance_stats['detection_time'] = (
            self.performance_stats['end_time'] - self.performance_stats['start_time']
        ).total_seconds()
        self.performance_stats['total_patterns'] = len(all_patterns)
        
        # 显示调试信息
        self.display_debug_info()
        
        # 显示性能统计
        self.display_performance_stats()
        
        return all_patterns
    
    def detect_n_account_patterns(self, df_filtered, n_accounts):
        """检测N个账户对刷模式 - 诊断版本"""
        wash_records = []
        
        # 按期号和彩种分组
        period_groups = df_filtered.groupby(['期号', '彩种'])
        total_periods = len(period_groups)
        processed_periods = 0
        
        self.add_debug_info(f"开始处理{total_periods}个期号组")
        
        for (period, lottery), period_data in period_groups:
            processed_periods += 1
            if processed_periods % 1000 == 0:  # 每1000期输出一次进度
                self.add_debug_info(f"已处理{processed_periods}/{total_periods}期号")
            
            period_accounts = period_data['会员账号'].unique()
            
            if len(period_accounts) < n_accounts:
                continue
            
            account_combinations = list(combinations(period_accounts, n_accounts))
            self.add_debug_info(f"期号{period}有{len(period_accounts)}账户，生成{len(account_combinations)}个组合")
            
            for account_group in account_combinations:
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
        
        self.add_debug_info(f"发现{len(wash_records)}条对刷记录")
        return self.find_continuous_patterns_diagnostic(wash_records)
    
    def find_continuous_patterns_diagnostic(self, wash_records):
        """诊断版本的连续对刷模式检测"""
        if not wash_records:
            self.add_debug_info("没有对刷记录需要处理")
            return []
        
        self.add_debug_info(f"开始处理{len(wash_records)}条对刷记录")
        
        # 使用字典进行快速分组
        account_group_patterns = defaultdict(list)
        for record in wash_records:
            account_group_key = (tuple(sorted(record['账户组'])), record['彩种'])
            account_group_patterns[account_group_key].append(record)
        
        self.add_debug_info(f"发现{len(account_group_patterns)}个账户组")
        
        continuous_patterns = []
        excluded_groups = []
        
        for (account_group, lottery), records in account_group_patterns.items():
            # 按期号排序
            sorted_records = sorted(records, key=lambda x: x['期号'])
            
            # 检查活跃度差异
            exclude_due_to_disparity, disparity_reason = self.should_exclude_due_to_activity_disparity(account_group, lottery)
            if exclude_due_to_disparity:
                excluded_groups.append({
                    '账户组': account_group,
                    '彩种': lottery,
                    '原因': disparity_reason,
                    '期数': len(sorted_records)
                })
                continue
            
            # 根据三档活跃度确定最小对刷期数要求
            required_min_periods = self.get_required_min_periods(account_group, lottery)
            activity_level = self.get_account_group_activity_level(account_group, lottery)
            
            self.add_debug_info(f"账户组{account_group} 活跃度:{activity_level} 要求期数:{required_min_periods} 实际期数:{len(sorted_records)}")
            
            if len(sorted_records) >= required_min_periods:
                # 使用向量化计算统计信息
                total_investment = sum(r['总金额'] for r in sorted_records)
                similarities = [r['相似度'] for r in sorted_records]
                avg_similarity = np.mean(similarities) if similarities else 0
                
                # 分析对立类型分布
                opposite_type_counts = defaultdict(int)
                for record in sorted_records:
                    opposite_type_counts[record['对立类型']] += 1
                
                # 分析模式分布
                pattern_count = defaultdict(int)
                for record in sorted_records:
                    pattern_count[record['模式']] += 1
                
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
                    '账户活跃度': activity_level,
                    '账户投注期数': account_periods_info,
                    '最小投注期数': min(lottery_stats.get(account, 0) for account in account_group),
                    '要求最小对刷期数': required_min_periods,
                    '活跃度差异检查': disparity_reason
                })
                self.add_debug_info(f"✅ 接受账户组{account_group}: {len(sorted_records)}期")
            else:
                self.add_debug_info(f"❌ 拒绝账户组{account_group}: 只有{len(sorted_records)}期，要求{required_min_periods}期")
        
        # 显示被排除的组
        if excluded_groups:
            self.add_debug_info(f"因活跃度差异排除了{len(excluded_groups)}个组")
            for excluded in excluded_groups[:5]:  # 只显示前5个
                self.add_debug_info(f"  排除: {excluded['账户组']} - {excluded['原因']}")
        
        self.add_debug_info(f"最终发现{len(continuous_patterns)}个连续对刷模式")
        return continuous_patterns
    
    def display_debug_info(self):
        """显示调试信息"""
        if not self.debug_info:
            return
        
        with st.expander("🐛 调试信息", expanded=True):
            st.write("### 检测过程详情")
            for info in self.debug_info[-50:]:  # 只显示最后50条
                st.write(f"`{info}`")
    
    def display_detailed_results(self, patterns):
        """显示详细检测结果 - 简化版本"""
        st.write("\n" + "="*60)
        st.write("🎯 多账户对刷检测结果")
        st.write("="*60)
        
        if not patterns:
            st.error("❌ 未发现符合阈值条件的连续对刷模式")
            
            # 提供诊断建议
            st.info("""
            **💡 诊断建议:**
            1. 检查数据是否包含有效的对刷行为
            2. 尝试调整检测参数（降低阈值）
            3. 查看调试信息了解检测过程
            4. 确认数据格式是否正确
            """)
            return
        
        # 显示发现的对刷组
        st.success(f"✅ 发现 {len(patterns)} 个对刷组")
        
        for i, pattern in enumerate(patterns, 1):
            with st.expander(f"对刷组 {i}: {' ↔ '.join(pattern['账户组'])}", expanded=True):
                st.write(f"**活跃度:** {pattern['账户活跃度']}")
                st.write(f"**彩种:** {pattern['彩种']}")
                st.write(f"**主要类型:** {pattern['主要对立类型']}")
                st.write(f"**期数:** {pattern['总期数']}期 (要求≥{pattern['要求最小对刷期数']}期)")
                st.write(f"**总金额:** {pattern['总投注金额']:.2f}元")
                st.write(f"**平均匹配:** {pattern['平均相似度']:.2%}")
                
                # 显示账户统计
                st.write("**账户统计:**")
                for account_info in pattern['账户投注期数']:
                    st.write(f"- {account_info}")
                
                # 显示前5条详细记录
                st.write("**前5条对刷记录:**")
                for j, record in enumerate(pattern['详细记录'][:5], 1):
                    account_directions = [f"{acc}({dir}:{amt})" for acc, dir, amt in zip(record['账户组'], record['方向组'], record['金额组'])]
                    st.write(f"{j}. 期号:{record['期号']} | 匹配度:{record['相似度']:.2%}")
                    st.write(f"   方向:{' ↔ '.join(account_directions)}")

def main():
    """主函数"""
    st.title("🎯 快三多账户对刷检测系统（诊断版）")
    st.markdown("---")
    
    # 侧边栏配置 - 使用宽松的默认值
    st.sidebar.header("⚙️ 检测参数配置")
    
    st.sidebar.info("💡 使用宽松参数进行诊断")
    
    min_amount = st.sidebar.number_input("最小投注金额", value=1, min_value=1, help="诊断时使用较低值")
    similarity_threshold = st.sidebar.slider("金额匹配度阈值", 0.5, 1.0, 0.8, 0.05, help="诊断时使用较低阈值")
    max_accounts = st.sidebar.slider("最大检测账户数", 2, 8, 5, help="检测的最大账户组合数量")
    
    # 宽松的阈值配置
    st.sidebar.subheader("🎯 宽松阈值配置（诊断用）")
    low_activity_max = st.sidebar.number_input("低活跃度上限(期)", value=100, min_value=10, help="诊断时使用较高值")
    medium1_activity_min = st.sidebar.number_input("中活跃度1下限(期)", value=101, min_value=11, help="诊断时使用较高值")
    medium1_activity_max = st.sidebar.number_input("中活跃度1上限(期)", value=500, min_value=50, help="诊断时使用较高值")
    medium2_activity_min = st.sidebar.number_input("中活跃度2下限(期)", value=501, min_value=51, help="诊断时使用较高值")
    
    # 最小对刷期数要求 - 使用较低值
    st.sidebar.subheader("📊 最小对刷期数要求")
    min_periods_low = st.sidebar.number_input("低活跃度最小对刷期数", value=2, min_value=1, help="诊断时使用较低值")
    min_periods_medium1 = st.sidebar.number_input("中活跃度1最小对刷期数", value=2, min_value=1, help="诊断时使用较低值")
    min_periods_medium2 = st.sidebar.number_input("中活跃度2最小对刷期数", value=2, min_value=1, help="诊断时使用较低值")
    
    # 活跃度差异检查 - 放宽限制
    st.sidebar.subheader("🔍 活跃度差异检查")
    max_period_difference = st.sidebar.number_input("最大期数差异", value=1000, min_value=100, help="诊断时放宽限制")
    
    # 文件上传
    st.header("📁 数据上传")
    uploaded_file = st.file_uploader(
        "请上传数据文件 (支持 .xlsx, .xls, .csv)", 
        type=['xlsx', 'xls', 'csv'],
        help="请确保文件包含必要的列：会员账号、期号、内容、金额"
    )
    
    if uploaded_file is not None:
        try:
            # 更新配置参数 - 使用宽松值
            config = Config()
            config.min_amount = min_amount
            config.amount_similarity_threshold = similarity_threshold
            config.max_accounts_in_group = max_accounts
            config.period_thresholds = {
                'low_activity_max': low_activity_max,
                'medium1_activity_min': medium1_activity_min,
                'medium1_activity_max': medium1_activity_max,
                'medium2_activity_min': medium2_activity_min,
                'min_periods_low': min_periods_low,
                'min_periods_medium1': min_periods_medium1,
                'min_periods_medium2': min_periods_medium2,
                'max_period_difference': max_period_difference
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
                        
                        # 显示数据统计
                        with st.expander("📊 数据统计", expanded=True):
                            st.write(f"有效记录数: {len(df_valid):,}")
                            st.write(f"唯一期号数: {df_valid['期号'].nunique():,}")
                            st.write(f"唯一账户数: {df_valid['会员账号'].nunique():,}")
                            
                            # 显示前10个账户
                            st.write("**前10个账户记录数:**")
                            account_counts = df_valid['会员账号'].value_counts().head(10)
                            for account, count in account_counts.items():
                                st.write(f"- {account}: {count}条记录")
                        
                        # 检测对刷交易
                        if st.button("🚀 开始诊断检测", type="primary"):
                            with st.spinner("🔍 正在检测对刷交易..."):
                                patterns = detector.detect_all_wash_trades()
                            
                            # 显示结果
                            detector.display_detailed_results(patterns)
                            
                    else:
                        st.error("❌ 数据解析失败，请检查文件格式和内容")
            
        except Exception as e:
            st.error(f"❌ 程序执行失败: {str(e)}")
            st.error(f"详细错误信息:\n{traceback.format_exc()}")
    
    # 使用说明
    with st.expander("📖 诊断说明", expanded=True):
        st.markdown("""
        ### 诊断模式说明
        
        **🔍 当前问题：** 完善后检测不到对刷行为
        
        **💡 诊断方案：**
        1. **使用宽松参数**：降低所有阈值限制
        2. **显示详细日志**：查看检测过程的每一步
        3. **提供诊断建议**：根据结果给出调整建议
        
        **🎯 诊断步骤：**
        1. 上传您的测试文件
        2. 点击"开始诊断检测"
        3. 查看调试信息了解检测过程
        4. 根据结果调整参数
        
        **⚙️ 当前使用的宽松参数：**
        - 最小投注金额: 1元
        - 匹配度阈值: 80%
        - 最小对刷期数: 2期
        - 活跃度差异: 允许1000期差异
        
        如果这样能检测到对刷，说明之前的阈值设置过于严格。
        """)

if __name__ == "__main__":
    main()
