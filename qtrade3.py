import random
from datetime import datetime
import numpy as np
import psutil
import pytz
import pandas as pd
from pyspark.sql import SparkSession, udf
from pyspark.sql.types import *

cpu_count = psutil.cpu_count()
windows = 110
qdata_prefix = "https://raw.githubusercontent.com/zuoxiaolei/qdata/main/data/"
github_proxy_prefix = "https://ghproxy.com/"
sequence_length = 10
seq_length = 30
tz = pytz.timezone('Asia/Shanghai')
now = datetime.now(tz).strftime("%Y%m%d")


def load_spark_sql():
    sql_text = open('data/spark.sql', encoding='utf-8').read()
    spark_sql = [ele for ele in sql_text.split(";") if ele]
    return spark_sql


spark_sql = load_spark_sql()


def get_spark():
    parallel_num = str(cpu_count * 3)
    spark = SparkSession.builder \
        .appName("chain merge") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", parallel_num) \
        .config("spark.default.parallelism", parallel_num) \
        .config("spark.ui.showConsoleProgress", True) \
        .config("spark.executor.memory", '1g') \
        .config("spark.driver.memory", '2g') \
        .config("spark.driver.maxResultSize", '2g') \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.executor.extraJavaOptions", "-Xss1024M") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


spark = get_spark()


def get_ols(x, y):
    if not x or not y:
        return float(np.NAN), float(np.NAN), float(np.NAN)
    x = np.array(x)
    y = np.array(y)
    slope, intercept = np.polyfit(x, y, 1)
    r2 = (sum((y - (slope * x + intercept)) ** 2) / ((len(y) - 1) * np.var(y, ddof=1)))
    return float(slope), float(intercept), float(r2)


schema = StructType().add("slope", DoubleType()).add("intercept", DoubleType()).add("r2", DoubleType())
spark.udf.register('get_ols', get_ols, schema)


def run_qtrade4():
    stock_df_filename = "data/ads/exchang_fund_rt.csv"
    scale_df_filename = "data/dim/scale.csv"
    fund_etf_fund_daily_em_df_filename = "data/dim/exchang_eft_basic_info.csv"
    stock_df = pd.read_csv(stock_df_filename, dtype={"code": object})
    scale_df = pd.read_csv(scale_df_filename, dtype={"code": object})
    fund_etf_fund_daily_em_df = pd.read_csv(fund_etf_fund_daily_em_df_filename, dtype={'基金代码': object})
    fund_etf_fund_daily_em_df = fund_etf_fund_daily_em_df[['基金代码', '基金简称', '类型']]
    fund_etf_fund_daily_em_df.columns = ['code', 'name', 'type']

    df = spark.createDataFrame(stock_df)
    scale_df = spark.createDataFrame(scale_df)
    fund_etf_fund_daily_em_df = spark.createDataFrame(fund_etf_fund_daily_em_df)
    df.createOrReplaceTempView("df")
    scale_df.createOrReplaceTempView("scale_df")
    fund_etf_fund_daily_em_df.createOrReplaceTempView("fund_etf_fund_daily_em_df")

    result = spark.sql(spark_sql[-1])
    result_df = result.toPandas()
    result_df_latest = result_df.groupby('code').tail(20)
    result_df.to_csv("data/rsrs.csv", index=False)
    result_df_latest.to_csv("data/rsrs_latest.csv", index=False)


def tune_best_param(df, low=-0.5, high=1.5):
    if low >= high:
        return 0
    length = len(df)
    i = 0
    code = df['code'].unique().tolist()[0]
    name = df['name'].unique().tolist()[0]
    slope_standard = df['slope_standard'].tolist()
    close = df['close'].tolist()
    date = df['date'].tolist()
    results = []
    previous_rsrs = None
    current_rsrs = None
    while i < length:
        previous_rsrs = current_rsrs
        current_rsrs = slope_standard[i]
        current_close = close[i]
        current_date = date[i]
        if previous_rsrs and current_rsrs >= low > previous_rsrs:
            j = i
            while j < length:
                next_rsrs = slope_standard[j]
                next_close = close[j]
                next_date = date[j]
                if next_rsrs >= high:
                    results.append([code, name, current_date,
                                    next_date, current_close, next_close, next_close / current_close - 1])
                    i = j
                    break
                j += 1
        i += 1
    return results


from itertools import product


def find_best_param():
    dfs = pd.read_csv('temp.csv', dtype={'code': object})
    dfs = dfs[dfs.date >= '2013-01-01']
    codes = dfs[['code', 'name']].drop_duplicates().values.tolist()
    all_result = []
    for code, name in codes:
        df = dfs[dfs.code == code]
        lows = [-i * 0.1 for i in range(11)]
        highs = [0.1 * i for i in range(1, 20)]
        find_result = []
        for low, high in product(lows, highs):
            results = tune_best_param(df, low=low, high=high)
            df1 = pd.DataFrame(results, columns=['code', 'name', 'current_date', 'next_date',
                                                 'current_close', 'next_close', 'profit'])
            find_result.append([low, high, df1['profit'].sum()])
        find_result = sorted(find_result, key=lambda x: x[-1], reverse=True)
        all_result.append([code, name] + find_result[0])
    all_result = pd.DataFrame(all_result, columns=['code', 'name', 'low', 'high', 'profit'])
    all_result.to_csv('temp4.csv', index=False)


def get_history_df():
    best_param = pd.read_csv('best_param.csv', dtype={'code': object})
    best_param = best_param.sort_values(by='profit', ascending=False)
    best_param = best_param.head(20)
    all_result = []
    dfs = pd.read_csv('temp.csv', dtype={'code': object})
    dfs = dfs[dfs.date >= '2013-01-01']
    for code, name, low, high, profit in best_param.values.tolist():
        df = dfs[dfs.code == code]
        results = tune_best_param(df, low=low, high=high)
        all_result.extend(results)
    all_result = pd.DataFrame(all_result, columns=['code', 'name', 'current_date', 'next_date',
                                                   'current_close', 'next_close', 'profit'])
    all_result.to_csv('temp5.csv', index=False)


if __name__ == '__main__':
    # main(is_local=False)
    # run_qtrade4()

    # df = pd.read_csv('temp.csv', dtype={'code': object})
    # df = df[df.code == '510300']
    # print(df)
    # results = get_best_param(df)
    # df1 = pd.DataFrame(results, columns=['code', 'name', 'current_date', 'next_date',
    #                                      'current_close', 'next_close', 'profit'])
    # df1.to_csv('temp2.csv', index=False)
    run_qtrade4()
