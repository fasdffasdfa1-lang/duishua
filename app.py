import streamlit as st
import pandas as pd
import re
from datetime import datetime

class LotteryDisplayFramework:
    def __init__(self):
        self.lottery_types = {
            "fast_three": "大发快三",
            "pk_ten": "PK10",
            "three_d": "3D/排三",
            "ssq": "双色球",
            "dlt": "大乐透"
        }
    
    def detect_lottery_type(self, content):
        """自动检测彩种类型"""
        content_lower = str(content).lower()
        
        if '快三' in content_lower:
            return 'fast_three'
        elif 'pk10' in content_lower or '赛车' in content_lower:
            return 'pk_ten'
        elif '3d' in content_lower or '排三' in content_lower:
            return 'three_d'
        elif '双色球' in content_lower:
            return 'ssq'
        elif '大乐透' in content_lower:
            return 'dlt'
        else:
            return 'fast_three'  # 默认
    
    def create_group_display(self, group_data):
        """创建单个组合的显示框架"""
        display_html = f"""
        <div style="background: #f8f9fa; padding: 15px; border-radius: 10px; margin: 10px 0; border-left: 4px solid #4CAF50;">
            <div style="display: flex; align-items: center; margin-bottom: 10px;">
                <span style="font-weight: bold; color: #2c3e50;">组合 {group_data.get('group_number', 1)}</span>
            </div>
            
            <div style="margin: 8px 0;">
                <span style="color: #7f8c8d;">账户:</span> 
                <span style="font-weight: bold; color: #2c3e50;">{group_data.get('account1', 'N/A')}</span>
                <span style="color: #7f8c8d;"> ↔ </span>
                <span style="font-weight: bold; color: #2c3e50;">{group_data.get('account2', 'N/A')}</span>
            </div>
            
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; margin: 10px 0;">
                <div>
                    <span style="color: #3498db;">📊 活跃度:</span> 
                    <span style="color: {'#e74c3c' if group_data.get('activity', 'low') == 'low' else '#27ae60'}">
                        {group_data.get('activity', 'low')}
                    </span>
                </div>
                <div>
                    <span style="color: #3498db;">🎲 彩种:</span> 
                    <span>{group_data.get('lottery_type', '大发快三')}</span>
                </div>
                <div>
                    <span style="color: #3498db;">🎯 主要类型:</span> 
                    <span>{group_data.get('main_type', '单-双')}</span>
                </div>
            </div>
            
            <div style="margin: 8px 0;">
                <span style="color: #3498db;">📈 账户在该彩种投注期数/记录数:</span> 
                <span>{group_data.get('betting_records', 'N/A')}</span>
            </div>
            
            <div style="display: flex; gap: 20px; margin: 10px 0;">
                <div>
                    <span style="color: #3498db;">🎯 对刷期数:</span> 
                    <span style="font-weight: bold; color: #e67e22;">{group_data.get('brush_periods', 0)}期</span>
                    <span style="color: #7f8c8d; font-size: 0.9em;">(要求≥3期)</span>
                </div>
                <div>
                    <span style="color: #3498db;">💰 总金额:</span> 
                    <span style="font-weight: bold; color: #27ae60;">{group_data.get('total_amount', '0.00')}元</span>
                </div>
                <div>
                    <span style="color: #3498db;">📊 平均匹配:</span> 
                    <span style="font-weight: bold; color: {'#e74c3c' if float(group_data.get('avg_match', 0)) < 50 else '#27ae60'}">
                        {group_data.get('avg_match', '0.00')}%
                    </span>
                </div>
            </div>
        """
        
        # 添加投注记录
        records = group_data.get('betting_records_list', [])
        if records:
            display_html += '<div style="margin-top: 15px;">'
            for i, record in enumerate(records, 1):
                display_html += f"""
                <div style="background: white; padding: 10px; margin: 5px 0; border-radius: 5px; border-left: 3px solid #3498db;">
                    <div style="font-weight: bold; color: #2c3e50;">
                        {i}. 期号:{record.get('period', 'N/A')} | 模式:{record.get('mode', 'N/A')} | 方向:{record.get('direction', 'N/A')} | 匹配度:{record.get('match_rate', '0.00')}%
                    </div>
                </div>
                """
            display_html += '</div>'
        
        display_html += '</div>'
        return display_html
    
    def display_analysis_result(self, uploaded_content, filename):
        """显示分析结果框架"""
        # 检测彩种类型
        lottery_key = self.detect_lottery_type(uploaded_content)
        lottery_name = self.lottery_types.get(lottery_key, "未知彩种")
        
        # 模拟数据 - 在实际应用中，这里会解析上传的内容
        groups_data = self.parse_uploaded_content(uploaded_content)
        
        # 显示标题
        st.markdown(f"""
        <div style="text-align: center; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin-bottom: 20px;">
            <h1 style="color: white; margin: 0;">🎲 彩种分析结果</h1>
            <p style="color: #e0e0e0; margin: 5px 0 0 0;">文件: {filename} | 彩种: {lottery_name} | 发现 {len(groups_data)} 组</p>
        </div>
        """, unsafe_allow_html=True)
        
        # 显示检测到的组数
        st.info(f"✅ 自动检测完成！发现 {len(groups_data)} 个有效组合")
        
        # 显示每个组合
        for group_data in groups_data:
            display_html = self.create_group_display(group_data)
            st.markdown(display_html, unsafe_allow_html=True)
    
    def parse_uploaded_content(self, content):
        """解析上传的内容 - 这里返回模拟数据"""
        # 在实际应用中，这里会解析上传文件的具体内容
        # 现在返回模拟数据来展示框架
        
        return [
            {
                "group_number": 1,
                "account1": "h1857625635",
                "account2": "dhy20",
                "activity": "low",
                "lottery_type": "大发快三",
                "main_type": "单-双",
                "betting_records": "qaz9818mn(9期/9记录), yo3658(9期/9记录), zijingyy19(10期/10记录)",
                "brush_periods": 4,
                "total_amount": "3948.00",
                "avg_match": "100.00",
                "betting_records_list": [
                    {
                        "period": "202510250431",
                        "mode": "单(1个) vs 双(2个)",
                        "direction": "zijingyy19(双:50.0) ↔ yo3658(双:74.0) ↔ qaz9818mn(单:124.0)",
                        "match_rate": "100.00"
                    },
                    {
                        "period": "202510250432", 
                        "mode": "大(2个) vs 小(1个)",
                        "direction": "zijingyy19(小:1500.0) ↔ yo3658(大:536.0) ↔ qaz9818mn(大:964.0)",
                        "match_rate": "100.00"
                    }
                ]
            },
            {
                "group_number": 2,
                "account1": "h1857625635", 
                "account2": "13866605165",
                "activity": "low",
                "lottery_type": "大发快三", 
                "main_type": "单-双",
                "betting_records": "abc7393(6期/8记录), mm1928(6期/6记录)",
                "brush_periods": 6,
                "total_amount": "1102.00",
                "avg_match": "100.00",
                "betting_records_list": [
                    {
                        "period": "202510250223",
                        "mode": "单(1个) vs 双(1个)", 
                        "direction": "mm1928(单:69.0) ↔ abc7393(双:69.0)",
                        "match_rate": "100.00"
                    },
                    {
                        "period": "202510250224",
                        "mode": "大(1个) vs 小(1个)",
                        "direction": "mm1928(大:195.0) ↔ abc7393(小:195.0)", 
                        "match_rate": "100.00"
                    }
                ]
            }
        ]

def main():
    st.set_page_config(
        page_title="彩种分析系统",
        page_icon="🎲",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # 自定义CSS样式
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #2c3e50;
        text-align: center;
        margin-bottom: 2rem;
    }
    .upload-section {
        background: #f8f9fa;
        padding: 2rem;
        border-radius: 10px;
        border: 2px dashed #dee2e6;
        text-align: center;
    }
    .info-box {
        background: #e3f2fd;
        padding: 1rem;
        border-radius: 5px;
        border-left: 4px solid #2196f3;
        margin: 1rem 0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # 标题
    st.markdown('<div class="main-header">🎲 彩种数据分析系统</div>', unsafe_allow_html=True)
    
    # 初始化显示框架
    framework = LotteryDisplayFramework()
    
    # 文件上传区域
    st.markdown("""
    <div class="upload-section">
        <h3>📁 上传数据文件</h3>
        <p>支持格式: .txt, .csv, .xlsx, 图片文件</p>
    </div>
    """, unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader(
        "选择文件",
        type=['txt', 'csv', 'xlsx', 'png', 'jpg', 'jpeg'],
        label_visibility="collapsed"
    )
    
    # 处理上传的文件
    if uploaded_file is not None:
        # 显示文件信息
        file_details = {
            "文件名": uploaded_file.name,
            "文件类型": uploaded_file.type,
            "文件大小": f"{uploaded_file.size / 1024:.2f} KB"
        }
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("文件名", uploaded_file.name)
        with col2:
            st.metric("文件类型", uploaded_file.type)
        with col3:
            st.metric("文件大小", f"{uploaded_file.size / 1024:.2f} KB")
        
        # 读取文件内容
        try:
            if uploaded_file.type == "text/plain":
                content = str(uploaded_file.read(), "utf-8")
            else:
                content = uploaded_file.read()
            
            # 显示分析按钮
            if st.button("🚀 开始分析", type="primary", use_container_width=True):
                with st.spinner("正在分析数据..."):
                    # 模拟分析过程
                    import time
                    time.sleep(2)
                    
                    # 显示分析结果
                    framework.display_analysis_result(content, uploaded_file.name)
                    
        except Exception as e:
            st.error(f"文件读取错误: {str(e)}")
    
    else:
        # 显示使用说明
        st.markdown("""
        <div class="info-box">
            <h4>💡 使用说明</h4>
            <ol>
                <li>点击上方区域选择数据文件</li>
                <li>系统会自动检测文件格式和彩种类型</li>
                <li>点击"开始分析"按钮查看结果</li>
                <li>结果将按照标准框架显示</li>
            </ol>
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
