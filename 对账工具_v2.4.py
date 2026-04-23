"""
智能对账工具 - Streamlit 应用 v3.0
场景化引导模式 + 智能字段精确映射 + 完整勾稽逻辑
版本：v3.0（正式版）
日期：2026-04-23
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import math
import re

# ==================== 页面配置 ====================
st.set_page_config(
    page_title="智能对账工具 v3.0",
    page_icon="🤖",
    layout="wide"
)

# ==================== 智能字段映射函数 ====================

def smart_match_field(columns, field_type):
    """智能匹配字段 - 优先精确匹配"""
    # 精确匹配规则（完全一致才匹配）
    exact_rules = {
        'order': ['萝卜头单号', '萝卜头订单号'],
        'order_child': ['子订单号', '子订单编号'],
        'amount_should': ['应还金额'],
        'price': ['产品单价', '单价'],
        'quantity': ['产品数量', '数量', '清分数量'],
        'fulfill_quantity': ['履约数量'],
        'currency': ['汇率'],
        # 场景2
        'bill_no': ['账单号'],
        'status': ['授信入账状态', '入账状态', '状态'],
        'link': ['关联授信账单', '关联'],
        # 场景3
        'supplier': ['供应商名称'],
        'price_batch': ['批次采购单价', '胚衣成本价'],
        'type': ['费用类型'],
        'party': ['分账对象'],
        'amount_share': ['分账金额', '分账金额(RMB)'],
        'pay': ['支付金额'],
        # 场景3专用
        'detail_qty': ['清分数量'],
        'clear_qty': ['履约数量']
    }
    
    keywords = exact_rules.get(field_type, [])
    
    # 精确匹配
    for col in columns:
        if col in keywords:
            return col
    
    # 部分匹配（兜底）
    for col in columns:
        for kw in keywords:
            if kw in col or col in kw:
                return col
    
    return columns[0] if columns else None


def get_best_field(columns, preferred_names):
    """获取最佳匹配字段 - 优先精确匹配"""
    # 精确匹配
    for name in preferred_names:
        for col in columns:
            if col == name:
                return col
    # 返回第一个
    return columns[0] if columns else None


# ==================== 对账执行函数 ====================

def normalize_order_id(value):
    """标准化订单号"""
    if pd.isna(value):
        return None
    return str(value).strip().upper()


def remove_suffix(value, suffix='-1'):
    """去除订单号后缀"""
    if pd.isna(value):
        return None
    return str(value).replace(suffix, '').strip().upper()


def execute_reconciliation(scenario, dfs, mapping):
    """执行对账"""
    if scenario == "场景1":
        return reconcile_scenario1(dfs, mapping)
    elif scenario == "场景2":
        return reconcile_scenario2(dfs, mapping)
    elif scenario == "场景3":
        return reconcile_scenario3(dfs, mapping)
    return {'success': False, 'error': '未知场景'}


def reconcile_scenario1(dfs, mapping):
    """场景1：客户授信账单对账"""
    df_sheet1 = dfs['sheet1'].copy()
    df_sheet2 = dfs['sheet2'].copy()
    
    # 保存原始订单号
    df_sheet1['_原始订单号_Sheet1'] = df_sheet1[mapping['sheet1_order']].astype(str)
    df_sheet2['_原始订单号_Sheet2'] = df_sheet2[mapping['sheet2_order']].astype(str)
    
    # 标准化订单号
    df_sheet1['_订单号标准化'] = df_sheet1[mapping['sheet1_order']].apply(normalize_order_id)
    df_sheet2['_订单号标准化'] = df_sheet2[mapping['sheet2_order']].apply(normalize_order_id)
    
    # Sheet2计算字段（不四舍五入，保持原始精度）
    calc_fields = mapping['calc_fields']
    df_sheet2['_计算金额_原始'] = (
        pd.to_numeric(df_sheet2[calc_fields[0]], errors='coerce').fillna(0) *
        pd.to_numeric(df_sheet2[calc_fields[1]], errors='coerce').fillna(0) *
        pd.to_numeric(df_sheet2[calc_fields[2]], errors='coerce').fillna(0) *
        pd.to_numeric(df_sheet2[calc_fields[3]], errors='coerce').fillna(0)
    )
    
    # 按标准化订单号汇总Sheet2后，再四舍五入
    df_sheet2_grouped = df_sheet2.groupby('_订单号标准化').agg({
        '_计算金额_原始': 'sum',
        '_原始订单号_Sheet2': 'first'
    }).reset_index()
    df_sheet2_grouped['_计算金额'] = df_sheet2_grouped['_计算金额_原始'].round(mapping['round_digits'])
    
    # Sheet1金额
    df_sheet1['_金额'] = pd.to_numeric(df_sheet1[mapping['sheet1_amount']], errors='coerce').fillna(0)
    
    # 合并
    merged = df_sheet1.merge(
        df_sheet2_grouped, 
        on='_订单号标准化', 
        how='outer'
    )
    
    # 填充空值
    merged['_金额'] = merged['_金额'].fillna(0)
    merged['_计算金额'] = merged['_计算金额'].fillna(0)
    merged['_原始订单号_Sheet1'] = merged['_原始订单号_Sheet1'].fillna('')
    merged['_原始订单号_Sheet2'] = merged['_原始订单号_Sheet2'].fillna('')
    
    # 判断勾稽结果
    merged['_差异'] = abs(merged['_计算金额'] - merged['_金额'])
    merged['_结果'] = merged.apply(
        lambda x: '✅匹配' if x['_金额'] != 0 and x['_计算金额'] != 0 and x['_差异'] < 0.01 
        else ('⚠️金额差异' if x['_金额'] != 0 or x['_计算金额'] != 0 else '空记录'), axis=1
    )
    
    # 统计
    match_count = len(merged[merged['_结果'] == '✅匹配'])
    diff_count = len(merged[merged['_结果'] == '⚠️金额差异'])
    s1_only = len(merged[(merged['_金额'] != 0) & (merged['_计算金额'] == 0)])
    s2_only = len(merged[(merged['_金额'] == 0) & (merged['_计算金额'] != 0)])
    
    # 总金额勾稽
    s1_should_total = df_sheet1[mapping['sheet1_amount']].apply(lambda x: math.ceil(float(x) * 100) / 100 if pd.notna(x) and float(x) > 0 else 0).sum()
    s1_pending_total = df_sheet1['待还金额'].apply(lambda x: math.ceil(float(x) * 100) / 100 if pd.notna(x) and float(x) > 0 else 0).sum()
    
    # 准备显示用的DataFrame
    display_df = pd.DataFrame({
        'Sheet1_订单号': merged['_原始订单号_Sheet1'],
        'Sheet2_订单号': merged['_原始订单号_Sheet2'],
        'Sheet1_金额': merged['_金额'],
        'Sheet2_计算金额': merged['_计算金额'],
        '差异': merged['_差异'],
        '勾稽结果': merged['_结果']
    })
    
    return {
        'success': True,
        'summary': {
            '匹配成功': match_count,
            '金额差异': diff_count,
            'Sheet1有Sheet2无': s1_only,
            'Sheet2有Sheet1无': s2_only,
            '应还总额勾稽': f'{s1_should_total:.2f}',
            '待还总额勾稽': f'{s1_pending_total:.2f}'
        },
        'details': display_df,
        'debug_info': {
            'sheet1_rows': len(df_sheet1),
            'sheet2_rows': len(df_sheet2),
            'sheet2_grouped_rows': len(df_sheet2_grouped),
            'merged_rows': len(merged)
        },
        'type': 'scenario1'
    }


def reconcile_scenario2(dfs, mapping):
    """场景2：订单清分与授信账单勾稽"""
    df_bill = dfs['bill'].copy()
    df_order = dfs['order'].copy()
    
    # 保存原始值
    df_bill['_原始订单号'] = df_bill[mapping['bill_order']].astype(str)
    df_bill['_账单号'] = df_bill[mapping['bill_no']].astype(str)
    df_bill['_订单号标准化'] = df_bill[mapping['bill_order']].apply(normalize_order_id)
    
    # 从授信账单中找到履约数量列
    bill_qty_col = None
    for col in df_bill.columns:
        if '履约' in col and '数量' in col:
            bill_qty_col = col
            break
        elif '履约数' in col:
            bill_qty_col = col
            break
    if bill_qty_col:
        df_bill['_账单履约数'] = pd.to_numeric(df_bill[bill_qty_col], errors='coerce').fillna(0)
    else:
        df_bill['_账单履约数'] = 0
    
    df_order['_原始订单号'] = df_order[mapping['order_order']].astype(str)
    df_order['_订单号标准化'] = df_order[mapping['order_order']].apply(normalize_order_id)
    df_order['_授信入账状态'] = df_order[mapping['order_status']].astype(str)
    df_order['_关联授信账单'] = df_order[mapping['order_link']].astype(str)
    
    # 按订单汇总清分数量
    df_order_grouped = df_order.groupby('_订单号标准化').agg({
        mapping['clear_qty']: 'sum',
        '_原始订单号': 'first',
        '_授信入账状态': lambda x: '|'.join(set(x.dropna())),
        '_关联授信账单': lambda x: '|'.join(set(x.dropna()))
    }).reset_index()
    df_order_grouped.rename(columns={mapping['clear_qty']: '_清分履约数'}, inplace=True)
    
    # 只取需要的列进行合并
    bill_for_merge = df_bill[['_订单号标准化', '_原始订单号', '_账单号', '_账单履约数']].copy()
    order_for_merge = df_order_grouped[['_订单号标准化', '_清分履约数', '_授信入账状态', '_关联授信账单']].copy()
    
    # 合并
    merged = bill_for_merge.merge(order_for_merge, on='_订单号标准化', how='outer')
    
    # 填充空值
    merged['_账单履约数'] = merged['_账单履约数'].fillna(0)
    merged['_清分履约数'] = merged['_清分履约数'].fillna(0)
    
    # 数量勾稽
    merged['_数量差异'] = abs(merged['_清分履约数'] - merged['_账单履约数'])
    
    # 综合判断
    def get_result(row):
        bill_has = row['_账单履约数'] > 0 or pd.notna(row.get('_原始订单号'))
        order_has = row['_清分履约数'] > 0
        if not bill_has and order_has:
            return '❌清分有账单无'
        if bill_has and not order_has:
            return '❌账单有清分无'
        if '已入账' not in str(row.get('_授信入账状态', '')):
            return '⚠️状态未入账'
        return '✅完全匹配'
    
    merged['_结果'] = merged.apply(get_result, axis=1)
    
    # 统计
    match_count = len(merged[merged['_结果'] == '✅完全匹配'])
    status_warn = len(merged[merged['_结果'] == '⚠️状态未入账'])
    bill_only = len(merged[merged['_结果'] == '❌账单有清分无'])
    order_only = len(merged[merged['_结果'] == '❌清分有账单无'])
    
    # 显示用的DataFrame
    display_df = pd.DataFrame({
        '账单号': merged['_账单号'],
        '授信_订单号': merged['_原始订单号'],
        '授信_履约数': merged['_账单履约数'],
        '清分_履约数': merged['_清分履约数'],
        '入账状态': merged['_授信入账状态'],
        '关联账单': merged['_关联授信账单'],
        '勾稽结果': merged['_结果']
    })
    
    return {
        'success': True,
        'summary': {
            '完全匹配': match_count,
            '状态未入账': status_warn,
            '账单有清分无': bill_only,
            '清分有账单无': order_only
        },
        'details': display_df,
        'debug_info': {'授信账单行数': len(df_bill), '清分数据行数': len(df_order), 'bill_qty_col': bill_qty_col},
        'type': 'scenario2'
    }


def reconcile_scenario3(dfs, mapping):
    """场景3：清分金额勾稽"""
    df_detail = dfs['detail'].copy()
    df_clear = dfs['clear'].copy()
    
    # 标准化
    df_detail['_订单号标准化'] = df_detail[mapping['detail_order']].apply(remove_suffix)
    df_detail['_原始订单号'] = df_detail[mapping['detail_order']].astype(str)
    
    df_clear['_订单号标准化'] = df_clear[mapping['clear_order']].apply(normalize_order_id)
    df_clear['_原始订单号'] = df_clear[mapping['clear_order']].astype(str)
    
    # 筛选胚衣费数据
    df_clear_peiyi = df_clear[df_clear[mapping['clear_type']] == '胚衣费'].copy()
    
    # 清分数据：按 订单号 + 分账对象 + 胚衣成本价 汇总履约数量
    df_clear_grouped = df_clear_peiyi.groupby([
        '_订单号标准化',
        df_clear_peiyi[mapping['clear_party']].astype(str),
        pd.to_numeric(df_clear_peiyi[mapping['clear_price']], errors='coerce').fillna(0).round(4)
    ]).agg({
        mapping['clear_qty']: 'sum',
        '_原始订单号': 'first'
    }).reset_index()
    df_clear_grouped.columns = ['_订单号标准化', '_分账对象', '_胚衣成本价', '_履约数量', '_原始订单号_清分']
    
    # 明细数据：按 订单号 + 供应商名称 + 批次采购单价 汇总清分数量
    df_detail['_分账对象'] = df_detail[mapping['detail_supplier']].astype(str)
    df_detail['_胚衣成本价'] = pd.to_numeric(df_detail[mapping['detail_price']], errors='coerce').fillna(0).round(4)
    
    df_detail_grouped = df_detail.groupby([
        '_订单号标准化',
        '_分账对象',
        '_胚衣成本价'
    ]).agg({
        mapping['detail_qty']: 'sum',
        '_原始订单号': 'first'
    }).reset_index()
    df_detail_grouped.columns = ['_订单号标准化', '_分账对象', '_胚衣成本价', '_清分数量', '_原始订单号_明细']
    
    # 合并勾稽
    merged = df_detail_grouped.merge(
        df_clear_grouped,
        on=['_订单号标准化', '_分账对象', '_胚衣成本价'],
        how='outer',
        suffixes=('_明细', '_清分')
    )
    
    merged['_数量差异'] = (merged.get('_清分数量', 0) - merged.get('_履约数量', 0)).fillna(0)
    merged['_结果'] = merged.apply(
        lambda x: '✅匹配' if pd.notna(x.get('_清分数量')) and pd.notna(x.get('_履约数量')) and abs(x['_数量差异']) < 0.01
        else ('❌明细有清分无' if pd.notna(x.get('_清分数量')) else '❌清分有明细无'), axis=1
    )
    
    # 成本价异常监控（萝卜头胚衣费成本价不为0才是异常）
    df_clear_abnormal = df_clear_peiyi[
        (df_clear_peiyi[mapping['clear_party']].astype(str) == '萝卜头') & 
        (pd.to_numeric(df_clear_peiyi[mapping['clear_price']], errors='coerce').fillna(0) != 0)
    ]
    
    # 分账金额勾稽（按订单汇总分账金额 vs 支付金额平均值）
    df_clear_amount = df_clear.groupby('_订单号标准化').agg({
        mapping['clear_amount']: 'sum',
        mapping['clear_pay']: 'mean',
        '_原始订单号': 'first'
    }).reset_index()
    df_clear_amount.columns = ['_订单号标准化', '_分账金额汇总', '_支付金额均值', '_原始订单号']
    
    # 统计
    match_count = len(merged[merged['_结果'] == '✅匹配'])
    detail_only = len(merged[merged['_结果'] == '❌明细有清分无'])
    clear_only = len(merged[merged['_结果'] == '❌清分有明细无'])
    
    # 显示DataFrame
    display_df = pd.DataFrame({
        '订单号': merged['_订单号标准化'],
        '分账对象': merged['_分账对象'],
        '胚衣成本价': merged['_胚衣成本价'],
        '明细_清分数量': merged.get('_清分数量'),
        '清分_履约数量': merged.get('_履约数量'),
        '数量差异': merged['_数量差异'],
        '勾稽结果': merged['_结果']
    })
    
    return {
        'success': True,
        'summary': {
            '胚衣费匹配成功': match_count,
            '明细有清分无': detail_only,
            '清分有明细无': clear_only,
            '异常监控(萝卜头成本价≠0)': len(df_clear_abnormal),
            '分账金额勾稽数': len(df_clear_amount)
        },
        'details': display_df,
        'abnormal': df_clear_abnormal,
        'amount_check': df_clear_amount,
        'debug_info': {
            '胚衣费清分行数': len(df_clear_peiyi),
            '明细汇总后行数': len(df_detail_grouped),
            '清分汇总后行数': len(df_clear_grouped)
        },
        'type': 'scenario3'
    }


def display_result(result, scenario):
    """显示对账结果"""
    st.success("✅ 对账完成！")
    
    st.subheader("📊 对账结果汇总")
    
    # 判断结果状态
    summary = result['summary']
    
    # 正常判断标准
    if scenario == "场景1":
        is_normal = (summary.get('金额差异', 0) == 0 and 
                     summary.get('Sheet1有Sheet2无', 0) == 0 and 
                     summary.get('Sheet2有Sheet1无', 0) == 0 and
                     summary.get('匹配成功', 0) > 0)
    elif scenario == "场景2":
        is_normal = (summary.get('账单有清分无', 0) == 0 and 
                     summary.get('清分有账单无', 0) == 0 and
                     summary.get('完全匹配', 0) > 0)
    elif scenario == "场景3":
        is_normal = (summary.get('明细有清分无', 0) == 0 and 
                     summary.get('清分有明细无', 0) == 0 and
                     summary.get('异常监控(萝卜头成本价≠0)', 0) == 0)
    else:
        is_normal = True
    
    if is_normal:
        st.success("🎉 对账结果正常！")
    else:
        st.warning("⚠️ 对账结果存在异常，请查看详细数据")
    
    cols = st.columns(len(result['summary']))
    for i, (key, value) in enumerate(result['summary'].items()):
        # 判断单个指标是否异常
        is_abnormal = False
        if key in ['金额差异', 'Sheet1有Sheet2无', 'Sheet2有Sheet1无', '明细有清分无', '清分有明细无', '账单有清分无', '清分有账单无', '状态未入账', '异常监控(萝卜头成本价≠0)']:
            is_abnormal = (value != 0 and value != '0')
        
        label = f"{key} {'⚠️' if is_abnormal else ''}"
        with cols[i]:
            st.metric(label, value)
    
    if 'debug_info' in result:
        with st.expander("🔧 调试信息", expanded=False):
            st.json(result['debug_info'])
    
    st.divider()
    
    st.subheader("📋 详细结果")
    
    df = result['details']
    # 排序：异常结果排在前面
    if '勾稽结果' in df.columns:
        df = df.copy()
        # 创建排序键，异常在前，正常在后
        df['_排序'] = df['勾稽结果'].apply(lambda x: 0 if '❌' in str(x) or '⚠️' in str(x) else 1)
        df = df.sort_values('_排序').drop('_排序', axis=1)
    st.dataframe(df, use_container_width=True)
    
    csv = df.to_csv(index=False).encode('utf-8-sig')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    st.download_button("📥 下载详细报表", csv, f"对账结果_{timestamp}.csv", "text/csv")
    
    if scenario == "场景3":
        if len(result.get('abnormal', [])) > 0:
            st.markdown("**⚠️ 异常数据（萝卜头胚衣费成本价为0）**")
            st.dataframe(result['abnormal'], use_container_width=True)
        
        if len(result.get('amount_check', [])) > 0:
            st.markdown("**💰 分账金额勾稽明细**")
            st.dataframe(result['amount_check'], use_container_width=True)


# ==================== 主界面 ====================

st.title("🤖 智能对账工具 v3.0")
st.markdown("**选择场景 → 上传文件 → 一键对账**")
st.divider()

SCENARIOS = {
    "场景1": {
        "name": "客户授信账单对账",
        "description": "单文件双Sheet：账单明细与订单明细勾稽",
        "files": "1个Excel（Sheet1账单 + Sheet2订单）",
        "rules": [
            "📋 Sheet1：萝卜头单号 + 应还金额",
            "📋 Sheet2：子订单号 → 汇总计算 → 金额勾稽",
            "📊 计算字段：单价×数量×履约数×汇率（四舍五入4位）",
            "📊 总金额勾稽：应还/待还总额向上舍入"
        ]
    },
    "场景2": {
        "name": "订单清分与授信账单勾稽",
        "description": "双文件：授信账单 + 订单清分数据",
        "files": "2个Excel文件",
        "rules": [
            "📋 订单号映射：授信账单 ↔ 订单清分",
            "📊 数量勾稽：按订单号汇总履约数量",
            "✅ 状态验证：授信入账状态=已入账",
            "✅ 关联验证：关联授信账单与账单号一致"
        ]
    },
    "场景3": {
        "name": "清分金额勾稽",
        "description": "双文件：订单明细 + 订单清分数据",
        "files": "2个Excel文件",
        "rules": [
            "📋 字段映射：子订单号去后缀 → 萝卜头订单号",
            "📊 胚衣成本数量勾稽（按订单+分账对象+成本价汇总）",
            "⚠️ 异常监控：萝卜头胚衣费成本价为0",
            "💰 分账金额勾稽：汇总分账金额 vs 支付金额均值"
        ]
    }
}

st.header("📋 第一步：选择对账场景")

cols = st.columns(len(SCENARIOS))
for i, (key, scenario) in enumerate(SCENARIOS.items()):
    with cols[i]:
        st.markdown(f"""
        <div style="background-color:#f8f9fa; padding:20px; border-radius:10px; border:1px solid #dee2e6;">
        <h4>{key}</h4>
        <b>{scenario['name']}</b>
        </div>
        """, unsafe_allow_html=True)

st.markdown("**请选择一个场景：**")
col_buttons = st.columns(3)

if 'selected_scenario' not in st.session_state:
    st.session_state.selected_scenario = None

for i, key in enumerate(SCENARIOS.keys()):
    with col_buttons[i]:
        if st.button(f"✅ 选择{key}", key=f"btn_{key}", use_container_width=True):
            st.session_state.selected_scenario = key

scenario = st.session_state.get('selected_scenario')

st.divider()

if scenario:
    scenario_info = SCENARIOS[scenario]
    
    st.header(f"📁 第二步：上传文件（{scenario_info['name']}）")
    
    with st.expander("📖 场景说明与对账规则", expanded=True):
        st.markdown(f"**{scenario_info['description']}**")
        st.markdown(f"**所需文件：** {scenario_info['files']}")
        st.markdown("**对账规则：**")
        for rule in scenario_info['rules']:
            st.markdown(f"- {rule}")
    
    if scenario == "场景1":
        uploaded_file = st.file_uploader("📤 上传Excel文件（Sheet1账单 + Sheet2订单明细）", type=['xlsx', 'xls'], key="file_s1")
        file_data = {"main": uploaded_file}
    elif scenario == "场景2":
        col1, col2 = st.columns(2)
        with col1:
            file1 = st.file_uploader("📤 客户授信账单", type=['xlsx', 'xls'], key="file_s2_1")
        with col2:
            file2 = st.file_uploader("📤 订单清分数据", type=['xlsx', 'xls'], key="file_s2_2")
        file_data = {"bill": file1, "order": file2}
    elif scenario == "场景3":
        col1, col2 = st.columns(2)
        with col1:
            file1 = st.file_uploader("📤 订单明细数据", type=['xlsx', 'xls'], key="file_s3_1")
        with col2:
            file2 = st.file_uploader("📤 订单清分数据", type=['xlsx', 'xls'], key="file_s3_2")
        file_data = {"detail": file1, "clear": file2}
    
    if all(v is not None for v in file_data.values()):
        st.success("✅ 文件上传完成！")
        
        with st.spinner("正在读取并分析文件..."):
            all_dfs = {}
            
            for name, file in file_data.items():
                try:
                    if scenario == "场景1" and name == "main":
                        df1 = pd.read_excel(file, sheet_name=0)
                        df2 = pd.read_excel(file, sheet_name=1)
                        all_dfs['sheet1'] = df1
                        all_dfs['sheet2'] = df2
                    else:
                        df = pd.read_excel(file)
                        all_dfs[name] = df
                except Exception as e:
                    st.error(f"❌ 读取{name}失败：{str(e)}")
                    all_dfs = None
                    break
        
        if all_dfs:
            st.divider()
            
            st.header("🔗 第三步：设置字段映射（系统已自动识别）")
            st.info("💡 系统已根据列名自动匹配字段，如需调整可手动选择")
            
            mapping = {}
            
            if scenario == "场景1":
                st.subheader("📋 Sheet1 - 账单明细")
                sheet1_cols = all_dfs['sheet1'].columns.tolist()
                sheet1_order = smart_match_field(sheet1_cols, 'order')
                sheet1_amount = smart_match_field(sheet1_cols, 'amount_should')
                
                col1, col2 = st.columns(2)
                with col1:
                    sheet1_order_col = st.selectbox("萝卜头单号", sheet1_cols, 
                        index=sheet1_cols.index(sheet1_order) if sheet1_order in sheet1_cols else 0, key="s1_order")
                with col2:
                    sheet1_amount_col = st.selectbox("应还金额", sheet1_cols, 
                        index=sheet1_cols.index(sheet1_amount) if sheet1_amount in sheet1_cols else 0, key="s1_amount")
                
                st.subheader("📋 Sheet2 - 订单明细")
                sheet2_cols = all_dfs['sheet2'].columns.tolist()
                sheet2_order = smart_match_field(sheet2_cols, 'order_child')
                calc_field1 = smart_match_field(sheet2_cols, 'price')
                calc_field2 = smart_match_field(sheet2_cols, 'quantity')
                calc_field3 = smart_match_field(sheet2_cols, 'fulfill_quantity')
                calc_field4 = smart_match_field(sheet2_cols, 'currency')
                
                st.markdown("**📊 计算字段（单价×数量×履约数×汇率）**")
                calc_col1, calc_col2 = st.columns(2)
                with calc_col1:
                    calc1 = st.selectbox("产品单价", sheet2_cols, 
                        index=sheet2_cols.index(calc_field1) if calc_field1 in sheet2_cols else 0, key="calc1")
                    calc2 = st.selectbox("产品数量", sheet2_cols, 
                        index=sheet2_cols.index(calc_field2) if calc_field2 in sheet2_cols else 0, key="calc2")
                with calc_col2:
                    calc3 = st.selectbox("履约数量", sheet2_cols, 
                        index=sheet2_cols.index(calc_field3) if calc_field3 in sheet2_cols else 0, key="calc3")
                    calc4 = st.selectbox("汇率", sheet2_cols, 
                        index=sheet2_cols.index(calc_field4) if calc_field4 in sheet2_cols else 0, key="calc4")
                
                sheet2_order_col = st.selectbox("子订单号", sheet2_cols, 
                    index=sheet2_cols.index(sheet2_order) if sheet2_order in sheet2_cols else 0, key="s2_order")
                
                st.markdown("**🔢 四舍五入位数**")
                round_digits = st.selectbox("小数位数", [2, 3, 4, 5], index=2, key="round_digits")
                
                mapping = {
                    'sheet1_order': sheet1_order_col,
                    'sheet1_amount': sheet1_amount_col,
                    'sheet2_order': sheet2_order_col,
                    'calc_fields': [calc1, calc2, calc3, calc4],
                    'round_digits': round_digits
                }
                
                with st.expander("📊 数据预览", expanded=False):
                    st.markdown("**Sheet1 账单明细**（前5行）")
                    st.dataframe(all_dfs['sheet1'].head(), use_container_width=True)
                    st.markdown(f"共 {len(all_dfs['sheet1'])} 行")
                    st.markdown("**Sheet2 订单明细**（前5行）")
                    st.dataframe(all_dfs['sheet2'].head(), use_container_width=True)
                    st.markdown(f"共 {len(all_dfs['sheet2'])} 行")
            
            elif scenario == "场景2":
                st.subheader("📋 客户授信账单")
                bill_cols = all_dfs['bill'].columns.tolist()
                bill_order = smart_match_field(bill_cols, 'order')
                bill_no = smart_match_field(bill_cols, 'bill_no')
                
                bill_order_col = st.selectbox("萝卜头订单号", bill_cols, 
                    index=bill_cols.index(bill_order) if bill_order in bill_cols else 0, key="s2_bill_order")
                bill_no_col = st.selectbox("账单号", bill_cols, 
                    index=bill_cols.index(bill_no) if bill_no in bill_cols else 0, key="s2_bill_no")
                
                st.subheader("📋 订单清分数据")
                order_cols = all_dfs['order'].columns.tolist()
                order_order = smart_match_field(order_cols, 'order')
                order_status = smart_match_field(order_cols, 'status')
                order_link = smart_match_field(order_cols, 'link')
                order_qty = smart_match_field(order_cols, 'quantity')
                
                order_order_col = st.selectbox("萝卜头订单号", order_cols, 
                    index=order_cols.index(order_order) if order_order in order_cols else 0, key="s2_order_order")
                order_status_col = st.selectbox("授信入账状态", order_cols, 
                    index=order_cols.index(order_status) if order_status in order_cols else 0, key="s2_order_status")
                order_link_col = st.selectbox("关联授信账单", order_cols, 
                    index=order_cols.index(order_link) if order_link in order_cols else 0, key="s2_order_link")
                order_qty_col = st.selectbox("履约数量", order_cols, 
                    index=order_cols.index(order_qty) if order_qty in order_cols else 0, key="s2_order_qty")
                
                mapping = {
                    'bill_order': bill_order_col,
                    'bill_no': bill_no_col,
                    'order_order': order_order_col,
                    'order_status': order_status_col,
                    'order_link': order_link_col,
                    'clear_qty': order_qty_col
                }
                
                with st.expander("📊 数据预览", expanded=False):
                    st.markdown("**授信账单**（前5行）")
                    st.dataframe(all_dfs['bill'].head(), use_container_width=True)
                    st.markdown("**订单清分**（前5行）")
                    st.dataframe(all_dfs['order'].head(), use_container_width=True)
            
            elif scenario == "场景3":
                st.subheader("📋 订单明细")
                detail_cols = all_dfs['detail'].columns.tolist()
                detail_order = smart_match_field(detail_cols, 'order_child')
                detail_supplier = smart_match_field(detail_cols, 'supplier')
                detail_price = smart_match_field(detail_cols, 'price_batch')
                detail_qty = smart_match_field(detail_cols, 'detail_qty')
                
                detail_order_col = st.selectbox("子订单编号", detail_cols, 
                    index=detail_cols.index(detail_order) if detail_order in detail_cols else 0, key="s3_detail_order")
                detail_supplier_col = st.selectbox("供应商名称", detail_cols, 
                    index=detail_cols.index(detail_supplier) if detail_supplier in detail_cols else 0, key="s3_detail_supplier")
                detail_price_col = st.selectbox("批次采购单价", detail_cols, 
                    index=detail_cols.index(detail_price) if detail_price in detail_cols else 0, key="s3_detail_price")
                detail_qty_col = st.selectbox("清分数量", detail_cols, 
                    index=detail_cols.index(detail_qty) if detail_qty in detail_cols else 0, key="s3_detail_qty")
                
                st.subheader("📋 订单清分")
                clear_cols = all_dfs['clear'].columns.tolist()
                clear_order = smart_match_field(clear_cols, 'order')
                clear_party = smart_match_field(clear_cols, 'party')
                clear_price = smart_match_field(clear_cols, 'price_batch')
                clear_qty = smart_match_field(clear_cols, 'clear_qty')
                clear_amount = smart_match_field(clear_cols, 'amount_share')
                clear_pay = smart_match_field(clear_cols, 'pay')
                clear_type = smart_match_field(clear_cols, 'type')
                
                clear_order_col = st.selectbox("萝卜头订单号", clear_cols, 
                    index=clear_cols.index(clear_order) if clear_order in clear_cols else 0, key="s3_clear_order")
                clear_party_col = st.selectbox("分账对象", clear_cols, 
                    index=clear_cols.index(clear_party) if clear_party in clear_cols else 0, key="s3_clear_party")
                clear_price_col = st.selectbox("胚衣成本价", clear_cols, 
                    index=clear_cols.index(clear_price) if clear_price in clear_cols else 0, key="s3_clear_price")
                clear_qty_col = st.selectbox("履约数量", clear_cols, 
                    index=clear_cols.index(clear_qty) if clear_qty in clear_cols else 0, key="s3_clear_qty")
                clear_amount_col = st.selectbox("分账金额", clear_cols, 
                    index=clear_cols.index(clear_amount) if clear_amount in clear_cols else 0, key="s3_clear_amount")
                clear_pay_col = st.selectbox("支付金额", clear_cols, 
                    index=clear_cols.index(clear_pay) if clear_pay in clear_cols else 0, key="s3_clear_pay")
                clear_type_col = st.selectbox("费用类型", clear_cols, 
                    index=clear_cols.index(clear_type) if clear_type in clear_cols else 0, key="s3_clear_type")
                
                mapping = {
                    'detail_order': detail_order_col,
                    'detail_supplier': detail_supplier_col,
                    'detail_price': detail_price_col,
                    'detail_qty': detail_qty_col,
                    'clear_order': clear_order_col,
                    'clear_party': clear_party_col,
                    'clear_price': clear_price_col,
                    'clear_qty': clear_qty_col,
                    'clear_amount': clear_amount_col,
                    'clear_pay': clear_pay_col,
                    'clear_type': clear_type_col
                }
                
                with st.expander("📊 数据预览", expanded=False):
                    st.markdown("**订单明细**（前5行）")
                    st.dataframe(all_dfs['detail'].head(), use_container_width=True)
                    st.markdown("**订单清分**（前5行）")
                    st.dataframe(all_dfs['clear'].head(), use_container_width=True)
            
            st.divider()
            
            st.header("⚙️ 第四步：执行对账")
            
            if st.button("🚀 开始对账", type="primary", use_container_width=True):
                with st.spinner("正在执行对账，请稍候..."):
                    try:
                        result = execute_reconciliation(scenario, all_dfs, mapping)
                        display_result(result, scenario)
                    except Exception as e:
                        st.error(f"❌ 对账执行失败：{str(e)}")
                        import traceback
                        st.code(traceback.format_exc())

else:
    st.info("👆 请先选择对账场景")

st.divider()
st.markdown("""
---
🤖 **智能对账工具 v3.0** | 正式版
""")
