import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import datetime

is_local = False
ttl = 3600
height = 740
if is_local:
    qdata_prefix = "data/"
    ttl = 0
else:
    qdata_prefix = "https://ghproxy.com/https://raw.githubusercontent.com/zuoxiaolei/qdata/main/data/"


@st.cache_resource(ttl=ttl)
def load_stock_data():
    df = pd.read_csv(qdata_prefix + 'rsrs.csv', dtype={'code': object})
    return df


@st.cache_resource(ttl=ttl)
def load_fund_data():
    df = pd.read_csv(qdata_prefix + 'rsrs_fund.csv', dtype={'code': object})
    return df


stock_df = load_stock_data()
fund_df = load_fund_data()

col1, col2 = st.columns(2)
with col1:
    code1 = st.text_input('股代码/ETF基金代码', '159915')
    select_stock_df = stock_df[stock_df.code == code1]
    st.dataframe(select_stock_df, height=height, hide_index=True)
    if len(select_stock_df) > 0:
        x = [datetime.datetime.strptime(ele, '%Y-%m-%d') for ele in select_stock_df.date]
        y = select_stock_df['slope_standard']
        plt.plot(x, y)
        plt.title(code1)
        st.pyplot(plt.gcf())

with col2:
    code2 = st.text_input('支付宝基金代码', '162605')
    select_fund_df = fund_df[fund_df.code == code2]
    st.dataframe(select_fund_df, height=height, hide_index=True)
    if len(select_fund_df) > 0:
        x = [datetime.datetime.strptime(ele, '%Y-%m-%d') for ele in select_fund_df.date]
        y = select_fund_df['slope_standard']
        plt.figure()
        plt.plot(x, y)
        plt.title(code2)
        st.pyplot(plt.gcf())
