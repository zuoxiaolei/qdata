import streamlit as st
import pandas as pd

qdata_prefix = "https://raw.githubusercontent.com/zuoxiaolei/qdata/main/data/"
github_proxy_prefix = "https://ghproxy.com/"


@st.cache_resource(ttl=3600)
def load_data():
    df = pd.read_csv(github_proxy_prefix + qdata_prefix + 'rsrs.csv', dtype={'code': object})
    return df


code = st.text_input('股代码/基金代码', '159915')
df = load_data()
select_df = df[df.code == code]
st.dataframe(select_df)
