```
# -*- coding:utf-8 -*-

"""
@Author:    hogan.chen@ymail.com
@Date:      2018-11-15
@Version:   V0.1
"""

import sys
import time
import pandas
import random
import asyncio
import logging
import datetime
import threading
import tushare as ts
import mysql.connector
from sqlalchemy import create_engine


PROGRESS_FLAG = '.'

HOLIDAY_LIST = [20170101, 20170102, 20170127, 20170128, 20170129, 20170130, 20170131, 20170201, 20170202, 20170403,
                20170404, 20170501, 20170529, 20170530, 20171002, 20171003, 20171004, 20171005, 20171006,
                20180101, 20180215, 20180216, 20180219, 20180220, 20180221, 20180405, 20180406, 20180430, 20180501,
                20180618, 20180924, 20181001, 20181002, 20181003, 20181004, 20181005]

MAX_THREAD_NUM = 50
THREAD_LOCK = threading.Lock()
ENABLE_MYSQL_CONNECTOR_FLAG = False

MIN_SLEEP_TIME = 2
MAX_SLEEP_TIME = 10  # retry to get the trading data after sleep
MAX_TRY_TIME = 5

TODAY_CODE_INDEX = 0
TODAY_NAME_INDEX = 1
TODAY_CHANGE_PERCENT_INDEX = 2
TODAY_TRADE_INDEX = 3
TODAY_OPEN_INDEX = 4
TODAY_HIGH_INDEX = 5
TODAY_LOW_INDEX = 6
TODAY_SETTLEMENT_INDEX = 7
TODAY_VOLUME_INDEX = 8
TODAY_TURN_OVER_RATIO_INDEX = 9

ALL_TRADING_MIN = 60 * 4  # The total trading minutes
START_MIN = 9 * 60 + 30  # The start minutes of trading
HALF_END_MIN = 11 * 60 + 30  # The half end minutes of trading
HALF_START_MIN = 13 * 60  # The half start minutes of trading
END_MIN = 15 * 60  # The end minutes of trading

HIST_DAY_TABLE = 'hist_day_data'
HIST_WEEK_TABLE = 'hist_week_data'
HIST_MONTH_TABLE = 'hist_month_data'
HIST_5M_TABLE = 'hist_5m_data'
HIST_15M_TABLE = 'hist_15m_data'
HIST_30M_TABLE = 'hist_30m_data'
HIST_60M_TABLE = 'hist_60m_data'
HIST_TABLE_LIST = [{'type': 'D', 'name': HIST_DAY_TABLE}, {'type': 'W', 'name': HIST_WEEK_TABLE},
                   {'type': 'M', 'name': HIST_MONTH_TABLE}, {'type': '5', 'name': HIST_5M_TABLE},
                   {'type': '15', 'name': HIST_15M_TABLE}, {'type': '30', 'name': HIST_30M_TABLE},
                   {'type': '60', 'name': HIST_60M_TABLE}]
# HIST_TABLE_LIST = [{'type': 'D', 'name': HIST_DAY_TABLE}, {'type': 'W', 'name': HIST_WEEK_TABLE},
#                    {'type': 'M', 'name': HIST_MONTH_TABLE}]

K_DAY_TABLE = 'k_day_data'
K_WEEK_TABLE = 'k_week_data'
K_MONTH_TABLE = 'k_month_data'
K_5M_TABLE = 'k_5m_data'
K_15M_TABLE = 'k_15m_data'
K_30M_TABLE = 'k_30m_data'
K_60M_TABLE = 'k_60m_data'
# K_TABLE_LIST = [{'type': 'D', 'name': K_DAY_TABLE}, {'type': 'W', 'name': K_WEEK_TABLE},
#                 {'type': 'M', 'name': K_MONTH_TABLE}, {'type': '5', 'name': K_5M_TABLE},
#                 {'type': '15', 'name': K_15M_TABLE}, {'type': '30', 'name': K_30M_TABLE},
#                 {'type': '60', 'name': K_60M_TABLE}]
K_TABLE_LIST = [{'type': 'D', 'name': K_DAY_TABLE}, {'type': 'W', 'name': K_WEEK_TABLE},
                {'type': 'M', 'name': K_MONTH_TABLE}]

H_TABLE = 'h_data'

BASICS_TABLE = 'basics_data'
CODE_TABLE = 'code_data'
TODAY_CODE_TABLE = 'today_code_data'
CONCEPT_TABLE = 'concept_data'
INDUSTRY_TABLE = 'industry_data'
AREA_TABLE = 'area_data'

TRADE_DAY_TABLE = 'trade_day_data'
TRADE_MINUTE_TABLE = 'trade_minute_data'

# 现在的上海证券交易所是在1990年重新开办的。1990年11月26日成立，同年12月19日开业
# 2000年10月，为配合创业版筹建，深交所A股新股发行及上市被全部停止（除了深沪两交易所外，
# 包括北京在内的多个城市也迅速展开了围绕创业板市场地点的激烈竞争。2000年8月，深交所公布创业板的相关规则，
# 标志深圳在这场角逐中最终胜出）。[9]直到2004年5月17日设立中小企业板（简称中小板）后，
# 2004年6月25日新和成等八家公司挂牌上市，才标志着深交所又重新恢复了新企业上市
OLDEST_TRADE_DATA = '1990-12-19'

db_settings = {
    'user': 'stck',
    'password': 'stck&sql',
    'host': '127.0.0.1',
    'database': 'stock',
    'port': 3306,
}

# log level
LOGGING_LEVEL = logging.INFO


def logging_config(logging_level):
    # log_format = "%(asctime)s - %(levelname)s - %(message)s"
    # log_format = "%(asctime)s [line: %(lineno)d] - %(levelname)s - %(message)s"
    # log_format = "[line: %(lineno)d] - %(levelname)s - %(message)s"
    # log_format = "[%(asctime)s - [File: %(filename)s line: %(lineno)d] - %(levelname)s]: %(message)s"
    log_format = "[%(asctime)s - [line: %(lineno)d] - %(levelname)s]: %(message)s"
    logging.basicConfig(level=logging_level, format=log_format)


def write_console(string):
    sys.stdout.write(string)
    sys.stdout.flush()


def connect_database(config):
    if ENABLE_MYSQL_CONNECTOR_FLAG:
        # cnx = mysql.connector.connect(**config)
        db_connector = mysql.connector.connect(**config)

        return db_connector
    else:
        engine_str = 'mysql+mysqlconnector://' + config['user'] + ':' + config['password'] \
            + '@localhost/' + config['database']
        db_engine = create_engine(engine_str, echo=False, pool_size=50, max_overflow=100)

        return db_engine


def disconnect_database(connect):
    if ENABLE_MYSQL_CONNECTOR_FLAG:
        connect.close()
    else:
        connect.dispose()


def get_table_data_from_mysql(cnx_engine, table_name):
    table_df = pandas.read_sql_table(table_name, con=cnx_engine)

    return table_df


def get_sql_query_from_mysql(cnx_engine, query_statement):
    query_df = pandas.read_sql_query(query_statement, con=cnx_engine)

    return query_df


def get_sql_from_mysql(cnx_engine, table_query):
    table_query_df = pandas.read_sql(table_query, con=cnx_engine)

    return table_query_df


# 实时行情
def get_today_all_data():
    while True:
        try:
            today_df = ts.get_today_all()
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    # remove duplicates entries
    today_df = today_df.drop_duplicates(today_df.columns[TODAY_CODE_INDEX])
    return today_df


# k线数据
def get_stock_history_k_data(code, start_date='', end_date='', k_type='D'):
    try_time = 0

    while True:
        try:
            # get_k_data已经catch了错误，然后只是打印了错误信息，所以这儿的代码永远不能catch到错误
            # 唯一的办法只有检查k_df的值，如果为None，则重新去获取数据
            k_df = ts.get_k_data(code, start=start_date, end=end_date, ktype=k_type)
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            if len(k_df) or try_time >= MAX_TRY_TIME:
                break
            else:
                try_time += 1

    logging.debug('code: {}, len(k_df): {}'.format(code, len(k_df)))
    return k_df


async def async_get_k_data(code, start_date='', end_date='', k_type='D'):
    return ts.get_k_data(code, start=start_date, end=end_date, ktype=k_type)


# k线数据
async def async_get_stock_history_k_data(code, start_date='', end_date='', k_type='D'):
    try_time = 0

    while True:
        try:
            # get_k_data已经catch了错误，然后只是打印了错误信息，所以这儿的代码永远不能catch到错误
            # 唯一的办法只有检查k_df的值，如果为None，则重新去获取数据
            k_df = await async_get_k_data(code, start_date, end_date, k_type)
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            if len(k_df) or try_time >= MAX_TRY_TIME:
                break
            else:
                try_time += 1

    logging.debug('code: {}, len(k_df): {}'.format(code, len(k_df)))
    return k_df


def async_get_all_k_data(today_df):
    tasks = []

    loop = asyncio.get_event_loop()

    for index in range(len(today_df)):
        code = today_df.iloc[index, TODAY_CODE_INDEX]
        tasks.append(async_get_stock_history_k_data(code))

    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()


def get_all_k_data(today_df):
    for index in range(len(today_df)):
        code = today_df.iloc[index, TODAY_CODE_INDEX]
        get_stock_history_k_data(code)


def main():
    today_df = get_today_all_data()
    today_trunc_df = today_df[0:100]

    start_time = time.time()
    async_get_all_k_data(today_trunc_df)
    logging.info('Total Elapsed Time: {} seconds\n'.format(time.time() - start_time))

    start_time = time.time()
    get_all_k_data(today_trunc_df)
    logging.info('Total Elapsed Time: {} seconds\n'.format(time.time() - start_time))


if __name__ == '__main__':
    logging_config(LOGGING_LEVEL)

    logging.info('Script start execution at {}\n'.format(datetime.datetime.now()))

    time_start = time.time()
    main()

    logging.info('Script end execution at {}\n'.format(datetime.datetime.now()))
    logging.info('Total Elapsed Time: {} seconds\n'.format(time.time() - time_start))

```



