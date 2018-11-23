```
# -*- coding:utf-8 -*-

"""
@Author:    hogan.chen@ymail.com
@Date:      2018-11-15
@Version:   V0.1
"""

import sys
import time
import queue
import pandas
import random
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


class TradeTimeClass:
    def __index__(self, date_time):
        self.date_time = date_time

    @staticmethod
    def is_weekend_day(date_time):
        if isinstance(date_time, datetime.datetime) or isinstance(date_time, datetime.date):
            pass

        if 6 == date_time.isoweekday() or 7 == date_time.isoweekday():
            return True
        else:
            return False

    @staticmethod
    def is_holiday_day(date_time):
        date_time_int = int(date_time.strftime('%Y%m%d'))
        if date_time_int in HOLIDAY_LIST:
            return True
        else:
            return False

    @staticmethod
    def is_trade_day(date_time):
        if not TradeTimeClass.is_weekend_day(date_time):
            if not TradeTimeClass.is_holiday_day(date_time):
                return True

        return False

    @staticmethod
    def is_trade_time(date_time):
        ret = False

        if TradeTimeClass.is_trade_day(date_time):
            if date_time.date() == datetime.date.today():
                today_pass_min = date_time.hour * 60 + date_time.minute
                if (START_MIN > today_pass_min) or (START_MIN <= today_pass_min < END_MIN):
                    ret = True

        return ret

    """
    >>> datetime.datetime.now()
    datetime.datetime(2017, 9, 22, 17, 20, 37, 992161)

    >>> k_df = ts.get_k_data('600000', ktype='W')
    >>> k_df
               date    open   close    high     low      volume    code
    0    2004-12-10   1.200   1.216   1.263   1.191   199360.71  600000
    1    2004-12-17   1.205   1.190   1.226   1.183   184445.22  600000
    2    2004-12-24   1.186   1.180   1.213   1.155   246866.95  600000
    3    2004-12-31   1.178   1.161   1.186   1.153   114512.06  600000
    4    2005-01-07   1.158   1.112   1.158   1.100   176951.46  600000
    5    2005-01-14   1.128   1.148   1.175   1.117   198237.59  600000
    ..          ...     ...     ...     ...     ...         ...     ...
    635  2017-08-25  12.500  12.780  12.800  12.410  3972553.00  600000
    636  2017-09-01  12.830  12.770  12.980  12.650  3087113.00  600000
    637  2017-09-08  12.780  13.030  13.120  12.650  2459866.00  600000
    638  2017-09-15  13.150  12.830  13.150  12.750  1842549.00  600000
    639  2017-09-22  12.820  12.890  12.960  12.790  1301175.00  600000

    [640 rows x 7 columns]
    >>>
    >>> k_df = ts.get_k_data('600000', start='2017-09-16', end='2017-09-22', ktype='W')
    >>> k_df
              date   open  close   high   low     volume    code
    37  2017-09-21  12.82  12.87  12.96  12.8  1059249.0  600000

    >>> k_df = ts.get_k_data('900951', start='2017-09-16', end='2017-09-22', ktype='W')
    >>> k_df
              date  open  close   high   low   volume    code
    37  2017-09-22  0.88  0.901  0.919  0.87  26955.0  900951
    >>>

    >>> k_df = ts.get_hist_data('600000', start='2017-09-16', end='2017-09-22', ktype='W')
    >>> k_df
                 open   high  close    low      volume  price_change  p_change  \
    date
    2017-09-22  12.82  12.96  12.89  12.79  1301177.62          0.06      0.47

                  ma5    ma10  ma20       v_ma5     v_ma10      v_ma20  turnover
    date
    2017-09-22  12.86  12.928  13.1  2532653.85  2661790.1  2808490.83      0.46
    """
    @staticmethod
    def get_last_trade_day(date_time, k_type='D'):
        if 'D' == k_type:
            new_date_time = date_time
        elif 'W' == k_type:
            if 5 < date_time.isoweekday():
                new_date_time = date_time
            else:
                new_date_time = date_time - datetime.timedelta(days=date_time.isoweekday())
        elif 'M' == k_type:
            month_first_day = datetime.datetime(year=date_time.year, month=date_time.month, day=1, hour=date_time.hour,
                                                minute=date_time.minute, second=date_time.second,
                                                microsecond=date_time.microsecond)
            new_date_time = month_first_day - datetime.timedelta(days=1)
        else:
            new_date_time = date_time

        while True:
            if not TradeTimeClass.is_trade_day(new_date_time):
                new_date_time = new_date_time - datetime.timedelta(days=1)
            else:
                if new_date_time.date() == datetime.date.today():
                    today_pass_min = new_date_time.hour * 60 + new_date_time.minute
                    if (START_MIN > today_pass_min) or (START_MIN <= today_pass_min < END_MIN):
                        new_date_time = new_date_time - datetime.timedelta(days=1)
                    else:
                        break
                else:
                    break

        return new_date_time


def get_trading_minutes():
    # get the trading time of today, use to predict the volume of today
    today_pass_min = datetime.datetime.now().hour * 60 + datetime.datetime.now().minute

    # calculate the trading minutes, this is used to predict the volume of today
    if START_MIN < today_pass_min <= HALF_END_MIN:
        trading_min = today_pass_min - START_MIN
    elif HALF_END_MIN < today_pass_min <= HALF_START_MIN:
        trading_min = HALF_END_MIN - START_MIN
    elif HALF_START_MIN < today_pass_min <= END_MIN:
        trading_min = ALL_TRADING_MIN / 2 + today_pass_min - HALF_START_MIN
    else:
        trading_min = ALL_TRADING_MIN

    return trading_min


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


# 沪深上市公司基本情况
# 获取个股首个上市日期，请参考以下方法
# df = ts.get_stock_basics()
# date = df.ix['600848']['timeToMarket'] #上市日期YYYYMMDD
def get_stock_basics_data(date=None):
    while True:
        try:
            basics_df = ts.get_stock_basics(date)
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return basics_df


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

    return k_df


# 个股历史交易记录, 可以通过参数设置获取日k线、周k线、月k线，以及5分钟、15分钟、30分钟和60分钟k线数据。本接口只能获取近3年的日线数据，适合搭配均线数据进行选股和分析
def get_stock_history_hist_data(code, start_date=None, end_date=None, k_type='D'):
    try_time = 0

    while True:
        try:
            hist_df = ts.get_hist_data(code, start=start_date, end=end_date, ktype=k_type)
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            if hist_df is None or 0 == len(hist_df) or try_time < MAX_TRY_TIME:
                try_time += 1
            else:
                break

            # if hist_df is not None or len(hist_df) or try_time >= MAX_TRY_TIME:
            #     break
            # else:
            #     try_time += 1

    return hist_df


# 历史复权数据, 分为前复权和后复权数据，接口提供股票上市以来所有历史数据，默认为前复权。
# 如果不设定开始和结束日期，则返回近一年的复权数据，从性能上考虑，推荐设定开始日期和结束日期，而且最好不要超过三年以上，获取全部历史数据，请分年段分步获取，取到数据后，请及时在本地存储。
def get_stock_history_h_data(code, start_date='', end_date=''):
    while True:
        try:
            if not start_date:
                h_df = ts.get_h_data(code)
            else:
                h_df = ts.get_h_data(code, start=start_date, end=end_date)
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return h_df


# 历史分笔
def get_stock_history_tick_data(code, date=''):
    while True:
        try:
            h_tick_df = ts.get_tick_data(code, date)
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return h_tick_df


# 当日历史分笔
def get_today_ticks_data(code):
    while True:
        try:
            ticks_df = ts.get_today_ticks(code)
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return ticks_df


# 实时交易数据
def get_real_time_quotes_data(code):
    while True:
        try:
            quotes_df = ts.get_realtime_quotes(code)
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return quotes_df


# 大盘指数行情列表
def get_today_index_data():
    while True:
        try:
            index_df = ts.get_index()
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return index_df


# 大单交易数据
def get_sina_dd_data(code, date=''):
    while True:
        try:
            dd_df = ts.get_sina_dd(code, date)
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return dd_df


# 行业分类
def get_industry_classified_data():
    while True:
        try:
            industry_df = ts.get_industry_classified()
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return industry_df


# 概念分类
def get_concept_classified_data():
    while True:
        try:
            concept_df = ts.get_concept_classified()
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return concept_df


# 地域分类
def get_area_classified_data():
    while True:
        try:
            area_df = ts.get_area_classified()
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return area_df


# 中小板分类
def get_sme_classified_data():
    while True:
        try:
            sme_df = ts.get_sme_classified()
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return sme_df


# 创业板分类
def get_gem_classified_data():
    while True:
        try:
            gem_df = ts.get_gem_classified()
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return gem_df


# 风险警示板分类
def get_st_classified_data():
    while True:
        try:
            st_df = ts.get_st_classified()
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return st_df


# 沪深300成份及权重
def get_hs300s_data():
    while True:
        try:
            hs300s_df = ts.get_hs300s()
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return hs300s_df


# 上证50成份股
def get_sz50s_data():
    while True:
        try:
            sz50s_df = ts.get_sz50s()
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return sz50s_df


# 中证500成份股
def get_zz500s_data():
    while True:
        try:
            zz500s_df = ts.get_zz500s()
        except Exception as err:
            logging.debug('{}'.format(err))
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
        else:
            break

    return zz500s_df


def df_to_sql_by_threading_lock(df, table, engine):
    THREAD_LOCK.acquire()
    df.to_sql(table, con=engine, if_exists='append', index=False)
    THREAD_LOCK.release()


def df_to_sql_by_queue(df_queue, table, engine):
    while True:
        df = df_queue.get()
        if df is None:
            break

        df.to_sql(table, con=engine, if_exists='append', index=False)


def update_today_data_and_code_data_from_today_all_to_mysql(cnx_engine):
    today_df = None
    today_date = TradeTimeClass.get_last_trade_day(datetime.datetime.today(), k_type='D')
    today_date_str = today_date.strftime('%Y-%m-%d')

    today_select_str = 'select date from {} order by date desc limit 1'.format(TODAY_CODE_TABLE)
    today_select_df = get_sql_query_from_mysql(cnx_engine, today_select_str)

    if (not len(today_select_df)) \
            or (today_date_str != str(today_select_df.iloc[0, 0])
                and not TradeTimeClass.is_trade_time(datetime.datetime.today())):
        logging.info('Begin to update the today code data table...')

        if today_df is None:
            today_df = get_today_all_data()

        if len(today_df) > 0:
            today_trade_df = today_df[today_df.volume > 0]
            today_code_df = today_trade_df[list(today_trade_df.columns[0:2])]
            today_code_df.insert(0, 'date', today_date_str)

            query_statement_str = 'truncate table {}'.format(TODAY_CODE_TABLE)
            cnx_engine.execute(query_statement_str)
            today_code_df.to_sql(TODAY_CODE_TABLE, con=cnx_engine, if_exists='append', index=False)
    else:
        logging.info('The today code data table is updated to latest date...')

    today_select_str = 'select date from {} order by date desc limit 1'.format(CODE_TABLE)
    today_select_df = get_sql_query_from_mysql(cnx_engine, today_select_str)

    if (not len(today_select_df)) \
            or (today_date_str != str(today_select_df.iloc[0, 0])
                and not TradeTimeClass.is_trade_time(datetime.datetime.today())):
        logging.info('Begin to update the code data table...')

        if today_df is None:
            today_df = get_today_all_data()

        if len(today_df) > 0:
            code_df = today_df[list(today_df.columns[0:2])]
            code_df.insert(0, 'date', today_date_str)

            query_statement_str = 'truncate table {}'.format(CODE_TABLE)
            cnx_engine.execute(query_statement_str)
            code_df.to_sql(CODE_TABLE, con=cnx_engine, if_exists='append', index=False)
    else:
        logging.info('The code data table is updated to latest date...')

    today_select_str = 'select date from {} order by date desc limit 1'.format(TRADE_DAY_TABLE)
    today_select_df = get_sql_query_from_mysql(cnx_engine, today_select_str)

    if (not len(today_select_df)) \
            or (today_date_str != str(today_select_df.iloc[0, 0])
                and not TradeTimeClass.is_trade_time(datetime.datetime.today())):
        logging.info('Begin to update the trade day data table...')

        if today_df is None:
            today_df = get_today_all_data()

        if len(today_df) > 0:
            today_df.insert(0, 'date', today_date_str)
            today_df.to_sql(TRADE_DAY_TABLE, con=cnx_engine, if_exists='append', index=False)
    else:
        logging.info('The trade day data table is updated to latest date...')


def update_basics_data_to_mysql(cnx_engine):
    today_date = TradeTimeClass.get_last_trade_day(datetime.datetime.today(), k_type='D')
    today_date_str = today_date.strftime('%Y-%m-%d')

    today_select_str = 'select date from {} order by date desc limit 1'.format(BASICS_TABLE)
    today_select_df = get_sql_query_from_mysql(cnx_engine, today_select_str)

    if (not len(today_select_df)) or (today_date_str != str(today_select_df.iloc[0, 0])):
        logging.info('Begin to update the basics data table...')
        basics_df = get_stock_basics_data()

        if len(basics_df) > 0:
            basics_df.insert(0, 'date', today_date_str)
            basics_df.insert(1, 'code', basics_df.index)
            query_statement_str = 'truncate table {}'.format(BASICS_TABLE)
            cnx_engine.execute(query_statement_str)
            basics_df.to_sql(BASICS_TABLE, con=cnx_engine, if_exists='append', index=False)
    else:
        logging.info('The basics data table is updated to latest date...')


def update_industry_data_to_mysql(cnx_engine):
    today_date = TradeTimeClass.get_last_trade_day(datetime.datetime.today(), k_type='D')
    today_date_str = today_date.strftime('%Y-%m-%d')

    today_select_str = 'select date from {} order by date desc limit 1'.format(INDUSTRY_TABLE)
    today_select_df = get_sql_query_from_mysql(cnx_engine, today_select_str)

    if (not len(today_select_df)) or (today_date_str != str(today_select_df.iloc[0, 0])):
        logging.info('Begin to update the industry data table...')
        industry_df = get_industry_classified_data()

        if len(industry_df) > 0:
            industry_df.insert(0, 'date', today_date_str)
            query_statement_str = 'truncate table {}'.format(INDUSTRY_TABLE)
            cnx_engine.execute(query_statement_str)
            industry_df.to_sql(INDUSTRY_TABLE, con=cnx_engine, if_exists='append', index=False)
    else:
        logging.info('The industry data table is updated to latest date...')


def update_concept_data_to_mysql(cnx_engine):
    today_date = TradeTimeClass.get_last_trade_day(datetime.datetime.today(), k_type='D')
    today_date_str = today_date.strftime('%Y-%m-%d')

    today_select_str = 'select date from {} order by date desc limit 1'.format(CONCEPT_TABLE)
    today_select_df = get_sql_query_from_mysql(cnx_engine, today_select_str)

    if (not len(today_select_df)) or (today_date_str != str(today_select_df.iloc[0, 0])):
        logging.info('Begin to update the concept data table...')
        concept_df = get_concept_classified_data()

        if len(concept_df) > 0:
            concept_df.insert(0, 'date', today_date_str)
            query_statement_str = 'truncate table {}'.format(CONCEPT_TABLE)
            cnx_engine.execute(query_statement_str)
            concept_df.to_sql(CONCEPT_TABLE, con=cnx_engine, if_exists='append', index=False)
    else:
        logging.info('The concept data table is updated to latest date...')


def update_area_data_to_mysql(cnx_engine):
    today_date = TradeTimeClass.get_last_trade_day(datetime.datetime.today(), k_type='D')
    today_date_str = today_date.strftime('%Y-%m-%d')

    today_select_str = 'select date from {} order by date desc limit 1'.format(AREA_TABLE)
    today_select_df = get_sql_query_from_mysql(cnx_engine, today_select_str)

    if (not len(today_select_df)) or (today_date_str != str(today_select_df.iloc[0, 0])):
        logging.info('Begin to update the area data table...')
        area_df = get_area_classified_data()

        if len(area_df) > 0:
            area_df.insert(0, 'date', today_date_str)
            query_statement_str = 'truncate table {}'.format(AREA_TABLE)
            cnx_engine.execute(query_statement_str)
            area_df.to_sql(AREA_TABLE, con=cnx_engine, if_exists='append', index=False)
    else:
        logging.info('The area data table is updated to latest date...')


def update_k_data_to_mysql(cnx_engine):
    code_select_str = 'select code from {}'.format(TODAY_CODE_TABLE)
    code_df = get_sql_query_from_mysql(cnx_engine, code_select_str)

    for k_table_dict in K_TABLE_LIST:
        logging.info('Begin to update the {} table\n'.format(k_table_dict['name']))

        for code in code_df[code_df.columns[0]]:
            code = str(code).zfill(6)

            # 不能获取今天的数据，即昨天的数据为最新的数据，0点后才更新昨天数据
            # last_trade_date = TradeTimeClass.get_last_trade_day(
            #     datetime.datetime.today() - datetime.timedelta(days=1), self.k_dict['type'])
            # 最近测试发现，收盘后，可以获得今天数据，但是加上开始日期或结束日期参数，就不能获取今天数据，待调查
            last_trade_date = TradeTimeClass.get_last_trade_day(datetime.datetime.today(), k_table_dict['type'])
            end_date_str = last_trade_date.strftime('%Y-%m-%d')

            k_select_str = 'select date, code from {} where code = "{}" order by date desc limit 1'. \
                format(k_table_dict['name'], code)
            last_date_df = get_sql_query_from_mysql(cnx_engine, k_select_str)
            time_to_market_str = ''

            if not len(last_date_df):
                basics_select_str = 'select timeToMarket from {} where code = "{}"'.format(BASICS_TABLE, code)
                time_to_market_df = get_sql_query_from_mysql(cnx_engine, basics_select_str)

                if len(time_to_market_df) > 0:
                    time_to_market_str = str(time_to_market_df.iloc[0, 0])

                    if '0' == time_to_market_str:
                        # 未上市
                        continue
                    else:
                        start_date_str = '{}-{}-{}'.format(time_to_market_str[0:4], time_to_market_str[4:6],
                                                           time_to_market_str[6:])
                        time_to_market_str = start_date_str

                        # 当月或者当周上市，所以不能获取上周或者上月的数据
                        if datetime.datetime.strptime(start_date_str, '%Y-%m-%d') \
                                >= datetime.datetime.strptime(end_date_str, '%Y-%m-%d'):
                            continue
                else:
                    start_date_str = OLDEST_TRADE_DATA
            else:
                last_date = str(last_date_df.iloc[0, 0])

                if datetime.datetime.strptime(last_date, '%Y-%m-%d') \
                        >= datetime.datetime.strptime(end_date_str, '%Y-%m-%d'):
                    continue
                else:
                    datetime_last_date = datetime.datetime.strptime(last_date, '%Y-%m-%d')
                    next_date = datetime_last_date + datetime.timedelta(days=1)
                    start_date_str = next_date.strftime('%Y-%m-%d')

            logging.info('code: {}, start_date_str = {}, end_date_str = {}'.format(code, start_date_str, end_date_str))
            # 对周K和月K，必须加上结束日期，否则为中间日期数据，即这周未到周五，但是返回的数据有到这周当前日期的数据，
            # 如今天是周三，则返回的最后一条数据是周一到周三的统计数据，这不是想要的数据，所以需要加上结束日期为上周末，
            # 统计到上周五的数据即可。
            # 或这个月未到月末，但是返回的最后一条数据是这月当前日期的数据，如今天是20号，
            # 则返回的数据中有1号到20号的累计统计数据，这不是想要的数据，所以加上结束日期为上月末，统计到上月末的数据即可
            if 'D' == k_table_dict['type'] and start_date_str != time_to_market_str \
                    and start_date_str != OLDEST_TRADE_DATA:
                k_df = get_stock_history_k_data(code, start_date='', end_date='',
                                                k_type=k_table_dict['type'])

                if len(k_df):
                    k_df = k_df[k_df['date'] >= start_date_str]
                else:
                    logging.info('The k_df[{}] is empty while the code is: {}'.format(k_table_dict['type'], code))
            else:
                k_df = get_stock_history_k_data(code, start_date=start_date_str, end_date=end_date_str,
                                                k_type=k_table_dict['type'])

            if len(k_df):
                # 获取df的列
                k_df_column = list(k_df)
                k_df_column.insert(0, k_df_column.pop(k_df_column.index(k_df.columns[-1])))  # 把最后一行弹出插入第一行
                k_df = k_df[k_df_column]
                k_df.to_sql(k_table_dict['name'], con=cnx_engine, if_exists='append', index=False)

                # self.k_df_lists.append(k_df)
            else:
                logging.info('The k_df[{}] is empty while the code is: {}'.format(k_table_dict['type'], code))


class GetKDataAsDfToMysqlClass(threading.Thread):
    def __init__(self, k_df_lists, last_date_df, basics_table_df, k_dict, code_df, df_queue, index, engine):
        threading.Thread.__init__(self)
        self.k_df_lists = k_df_lists
        self.last_date_df = last_date_df
        self.basics_table_df = basics_table_df
        self.k_dict = k_dict
        self.code_df = code_df
        self.df_queue = df_queue
        self.index = index
        self.engine = engine

    # The all threading average the all mod data
    def get_k_data_as_df(self):
        value_mod = len(self.code_df) % MAX_THREAD_NUM

        if self.index < value_mod:
            start_index = self.index * int(len(self.code_df) / MAX_THREAD_NUM + 1)
            end_index = (self.index + 1) * int(len(self.code_df) / MAX_THREAD_NUM + 1)
        else:
            start_index = self.index * int(len(self.code_df) / MAX_THREAD_NUM) + value_mod
            end_index = (self.index + 1) * int(len(self.code_df) / MAX_THREAD_NUM) + value_mod

        if start_index >= len(self.code_df):
            return

        for index in range(start_index, end_index):
            code = self.code_df.iloc[index, TODAY_CODE_INDEX]
            code = str(code).zfill(6)

            # 不能获取今天的数据，即昨天的数据为最新的数据，0点后才更新昨天数据
            # last_trade_date = TradeTimeClass.get_last_trade_day(
            #     datetime.datetime.today() - datetime.timedelta(days=1), self.k_dict['type'])
            # 最近测试发现，收盘后，可以获得今天数据，但是加上开始日期或结束日期参数，就不能获取今天数据，待调查
            last_trade_date = TradeTimeClass.get_last_trade_day(datetime.datetime.today(), self.k_dict['type'])
            end_date_str = last_trade_date.strftime('%Y-%m-%d')

            # k_select_str = 'select date, code from {} where code = "{}" order by date desc limit 1'.\
            #     format(self.k_dict['name'], code)
            # code_last_date_df = get_sql_query_from_mysql(self.engine, k_select_str)
            code_last_date_df = self.last_date_df[self.last_date_df['code'] == code]

            time_to_market_str = ''

            if not len(code_last_date_df):
                # basics_select_str = 'select timeToMarket from {} where code = "{}"'.format(BASICS_TABLE, code)
                # time_to_market_df = get_sql_query_from_mysql(self.engine, basics_select_str)
                time_to_market_df = self.basics_table_df[['timeToMarket']][self.basics_table_df['code'] == code]

                if len(time_to_market_df) > 0:
                    time_to_market_str = str(time_to_market_df.iloc[0, 0])

                    if '0' == time_to_market_str:
                        # 未上市
                        continue
                    else:
                        start_date_str = '{}-{}-{}'.format(time_to_market_str[0:4], time_to_market_str[4:6],
                                                           time_to_market_str[6:])
                        time_to_market_str = start_date_str

                        # 当月或者当周上市，所以不能获取上周或者上月的数据
                        if datetime.datetime.strptime(start_date_str, '%Y-%m-%d') \
                                >= datetime.datetime.strptime(end_date_str, '%Y-%m-%d'):
                            continue
                else:
                    start_date_str = OLDEST_TRADE_DATA
            else:
                last_date = str(code_last_date_df.iloc[0, 0])

                if datetime.datetime.strptime(last_date, '%Y-%m-%d') \
                        >= datetime.datetime.strptime(end_date_str, '%Y-%m-%d'):
                    continue
                else:
                    datetime_last_date = datetime.datetime.strptime(last_date, '%Y-%m-%d')
                    next_date = datetime_last_date + datetime.timedelta(days=1)
                    start_date_str = next_date.strftime('%Y-%m-%d')

            logging.info('code: {}, start_date_str = {}, end_date_str = {}'.format(code, start_date_str, end_date_str))
            # 对周K和月K，必须加上结束日期，否则为中间日期数据，即这周未到周五，但是返回的数据有到这周当前日期的数据，
            # 如今天是周三，则返回的最后一条数据是周一到周三的统计数据，这不是想要的数据，所以需要加上结束日期为上周末，
            # 统计到上周五的数据即可。
            # 或这个月未到月末，但是返回的最后一条数据是这月当前日期的数据，如今天是20号，
            # 则返回的数据中有1号到20号的累计统计数据，这不是想要的数据，所以加上结束日期为上月末，统计到上月末的数据即可
            if 'D' == self.k_dict['type'] and start_date_str != time_to_market_str \
                    and start_date_str != OLDEST_TRADE_DATA:
                k_df = get_stock_history_k_data(code, start_date='', end_date='',
                                                k_type=self.k_dict['type'])

                if len(k_df):
                    k_df = k_df[k_df['date'] >= start_date_str]
                else:
                    logging.info('The k_df[{}] is empty while the code is: {}'.format(self.k_dict['type'], code))
            else:
                k_df = get_stock_history_k_data(code, start_date=start_date_str, end_date=end_date_str,
                                                k_type=self.k_dict['type'])

            if len(k_df):
                # 获取df的列
                k_df_column = list(k_df)
                k_df_column.insert(0, k_df_column.pop(k_df_column.index(k_df.columns[-1])))  # 把最后一行弹出插入第一行
                k_df = k_df[k_df_column]

                # k_df.to_sql(self.k_dict['name'], con=self.engine, if_exists='append', index=False)
                # df_to_sql_by_threading_lock(k_df, self.k_dict['name'], self.engine)
                self.df_queue.put(k_df)

                # self.k_df_lists.append(k_df)
            else:
                logging.info('The k_df[{}] is empty while the code is: {}'.format(self.k_dict['type'], code))

    def run(self):
        self.get_k_data_as_df()


def get_basics_table_df(cnx_engine):
    logging.info('Start to get time to market date from table {}...'.format(BASICS_TABLE))

    basics_table_df = get_table_data_from_mysql(cnx_engine, BASICS_TABLE)

    return basics_table_df


def get_last_trade_date_df(cnx_engine, table_name):
    logging.info('Start to get last trade date from table {}...'.format(table_name))

    code_select_str = 'select code from {}'.format(TODAY_CODE_TABLE)
    code_df = get_sql_query_from_mysql(cnx_engine, code_select_str)

    last_date_df = pandas.DataFrame(columns=['date', 'code'])

    write_console('[Progess]')

    for index in range(len(code_df)):
        if 0 == index % 50:
            write_console(PROGRESS_FLAG)
        code = code_df.iloc[index, TODAY_CODE_INDEX]
        code = str(code).zfill(6)

        k_select_str = 'select date, code from {} where code = "{}" order by date desc limit 1'. \
            format(table_name, code)
        code_last_date_df = get_sql_query_from_mysql(cnx_engine, k_select_str)

        if len(code_last_date_df):
            last_date_df = last_date_df.append(code_last_date_df.iloc[len(code_last_date_df) - 1], ignore_index=True)

    write_console('\n')
    logging.debug('last_date_df:\n{}'.format(last_date_df))
    return last_date_df


def update_k_data_as_df_to_mysql(cnx_engine):
    code_select_str = 'select code from {}'.format(TODAY_CODE_TABLE)
    code_df = get_sql_query_from_mysql(cnx_engine, code_select_str)
    basics_table_df = get_basics_table_df(cnx_engine)

    for k_table_dict in K_TABLE_LIST:
        logging.info('Begin to update the {} table\n'.format(k_table_dict['name']))

        last_date_df = get_last_trade_date_df(cnx_engine, k_table_dict['name'])

        get_k_data_threads = []
        k_df_lists = []
        df_queue = queue.Queue()

        for thread_index in range(MAX_THREAD_NUM):
            threads = GetKDataAsDfToMysqlClass(k_df_lists, last_date_df, basics_table_df, k_table_dict,
                                               code_df, df_queue, thread_index, cnx_engine)
            get_k_data_threads.append(threads)

        for thread_index in range(len(get_k_data_threads)):
            get_k_data_threads[thread_index].start()

        df_to_sql_thread = threading.Thread(target=df_to_sql_by_queue,
                                            args=(df_queue, k_table_dict['name'], cnx_engine))
        df_to_sql_thread.start()

        for thread_index in range(len(get_k_data_threads)):
            get_k_data_threads[thread_index].join()

        df_to_sql_thread.join()


def update_hist_data_to_mysql(cnx_engine):
    code_select_str = 'select code from {}'.format(TODAY_CODE_TABLE)
    code_df = get_sql_query_from_mysql(cnx_engine, code_select_str)

    logging.info('\nbegin to update the hist data table')
    for code in code_df[code_df.columns[0]]:
        code = str(code).zfill(6)

        last_trade_date = TradeTimeClass.get_last_trade_day(datetime.datetime.today(), k_type='D')
        end_date_str = last_trade_date.strftime('%Y-%m-%d')

        hist_select_str = 'select date, code from {} where code = "{}" order by date desc limit 1'.\
            format(HIST_DAY_TABLE, code)
        code_last_date_df = get_sql_query_from_mysql(cnx_engine, hist_select_str)

        if not len(code_last_date_df):
            start_date_str = None
        else:
            last_date = str(code_last_date_df.iloc[0, 0])

            if datetime.datetime.strptime(last_date, '%Y-%m-%d') \
                    >= datetime.datetime.strptime(end_date_str, '%Y-%m-%d'):
                continue
            else:
                datetime_last_date = datetime.datetime.strptime(last_date, '%Y-%m-%d')
                next_date = datetime_last_date + datetime.timedelta(days=1)
                start_date_str = next_date.strftime('%Y-%m-%d')

        logging.info('code: {}, start_date_str = {}'.format(code, start_date_str))
        hist_df = get_stock_history_hist_data(code, start_date=start_date_str, end_date=end_date_str,
                                              k_type='D')

        if len(hist_df):
            hist_df.insert(0, 'code', code)
            hist_df.insert(1, 'date', hist_df.index)
            hist_df.to_sql(HIST_DAY_TABLE, con=cnx_engine, if_exists='append', index=False)


class GetHistDataAsDfToMysqlClass(threading.Thread):
    def __init__(self, hist_df_lists, last_date_df, basics_table_df, hist_dict, code_df, df_queue, index, engine):
        threading.Thread.__init__(self)
        self.hist_df_lists = hist_df_lists
        self.last_date_df = last_date_df
        self.basics_table_df = basics_table_df
        self.hist_dict = hist_dict
        self.code_df = code_df
        self.df_queue = df_queue
        self.index = index
        self.engine = engine

    # The all threading average the all mod data
    def get_hist_data_as_df(self):
        value_mod = len(self.code_df) % MAX_THREAD_NUM

        if self.index < value_mod:
            start_index = self.index * int(len(self.code_df) / MAX_THREAD_NUM + 1)
            end_index = (self.index + 1) * int(len(self.code_df) / MAX_THREAD_NUM + 1)
        else:
            start_index = self.index * int(len(self.code_df) / MAX_THREAD_NUM) + value_mod
            end_index = (self.index + 1) * int(len(self.code_df) / MAX_THREAD_NUM) + value_mod

        if start_index >= len(self.code_df):
            return

        for index in range(start_index, end_index):
            code = self.code_df.iloc[index, TODAY_CODE_INDEX]
            code = str(code).zfill(6)

            last_trade_date = TradeTimeClass.get_last_trade_day(datetime.datetime.today(), self.hist_dict['type'])
            end_date_str = last_trade_date.strftime('%Y-%m-%d')

            # hist_select_str = 'select date, code from {} where code = "{}" order by date desc limit 1'.\
            #     format(self.hist_dict['name'], code)
            # code_last_date_df = get_sql_query_from_mysql(self.engine, hist_select_str)
            code_last_date_df = self.last_date_df[self.last_date_df['code'] == code]

            if not len(code_last_date_df):
                start_date_str = None

                # basics_select_str = 'select timeToMarket from {} where code = "{}"'.format(BASICS_TABLE, code)
                # time_to_market_df = get_sql_query_from_mysql(self.engine, basics_select_str)
                time_to_market_df = self.basics_table_df[['timeToMarket']][self.basics_table_df['code'] == code]

                if len(time_to_market_df) > 0:
                    time_to_market_str = str(time_to_market_df.iloc[0, 0])
                    if '0' == time_to_market_str:
                        # 未上市
                        continue
                    else:
                        time_to_market_str = '{}-{}-{}'.format(time_to_market_str[0:4], time_to_market_str[4:6],
                                                               time_to_market_str[6:])

                        # 当月或者当周上市，所以不能获取上周或者上月的数据
                        if datetime.datetime.strptime(time_to_market_str, '%Y-%m-%d') \
                                >= datetime.datetime.strptime(end_date_str, '%Y-%m-%d'):
                            continue
            else:
                last_date = str(code_last_date_df.iloc[0, 0])

                if self.hist_dict['type'].isalpha():
                    datetime_last_date = datetime.datetime.strptime(last_date, '%Y-%m-%d')

                    if datetime_last_date >= datetime.datetime.strptime(end_date_str, '%Y-%m-%d'):
                        continue
                    else:
                        next_date = datetime_last_date + datetime.timedelta(days=1)
                        start_date_str = next_date.strftime('%Y-%m-%d')
                else:
                    datetime_last_date = datetime.datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S')

                    if datetime_last_date.date() >= datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date():
                        continue
                    else:
                        end_date_datetime = datetime.datetime.strptime(end_date_str, '%Y-%m-%d') + \
                                            datetime.timedelta(days=1)
                        end_date_str = end_date_datetime.strftime('%Y-%m-%d')

                        next_date = datetime_last_date + datetime.timedelta(days=1)
                        start_date_str = next_date.strftime('%Y-%m-%d')

            logging.info('code: {}, start_date_str = {}, end_date_str = {}'.format(code, start_date_str, end_date_str))
            # 对周K和月K，必须加上结束日期，否则为中间日期数据，即这周未到周五，但是返回的数据有到这周当前日期的数据，
            # 如今天是周三，则返回的最后一条数据是周一到周三的统计数据，这不是想要的数据，所以需要加上结束日期为上周末，
            # 统计到上周五的数据即可。
            # 或这个月未到月末，但是返回的最后一条数据是这月当前日期的数据，如今天是20号，
            # 则返回的数据中有1号到20号的累计统计数据，这不是想要的数据，所以加上结束日期为上月末，统计到上月末的数据即可
            hist_df = get_stock_history_hist_data(code, start_date=start_date_str, end_date=end_date_str,
                                                  k_type=self.hist_dict['type'])

            # 当天上市的公司，在交易时间去获取历史数据，返回为None
            if hist_df is not None and len(hist_df):
                hist_df.insert(0, 'date', hist_df.index)
                hist_df.insert(1, 'code', code)

                # hist_df.to_sql(self.hist_dict['name'], con=self.engine, if_exists='append', index=False)
                # df_to_sql_by_threading_lock(hist_df, self.hist_dict['name'], self.engine)
                self.df_queue.put(hist_df)

                # self.hist_df_lists.append(hist_df)
            else:
                logging.info('The hist_df[{}] is empty while the code is: {}'.format(self.hist_dict['type'], code))

    def run(self):
        self.get_hist_data_as_df()


def update_hist_data_as_df_to_mysql(cnx_engine):
    code_select_str = 'select code from {}'.format(TODAY_CODE_TABLE)
    code_df = get_sql_query_from_mysql(cnx_engine, code_select_str)
    basics_table_df = get_basics_table_df(cnx_engine)

    for hist_table_dict in HIST_TABLE_LIST:
        logging.info('Begin to update the {} table\n'.format(hist_table_dict['name']))

        last_date_df = get_last_trade_date_df(cnx_engine, hist_table_dict['name'])

        get_hist_data_threads = []
        hist_df_lists = []
        df_queue = queue.Queue()

        for thread_index in range(MAX_THREAD_NUM):
            threads = GetHistDataAsDfToMysqlClass(hist_df_lists, last_date_df, basics_table_df,
                                                  hist_table_dict, code_df, df_queue, thread_index, cnx_engine)
            get_hist_data_threads.append(threads)

        for thread_index in range(len(get_hist_data_threads)):
            get_hist_data_threads[thread_index].start()

        df_to_sql_thread = threading.Thread(target=df_to_sql_by_queue,
                                            args=(df_queue, hist_table_dict['name'], cnx_engine))
        df_to_sql_thread.start()

        for thread_index in range(len(get_hist_data_threads)):
            get_hist_data_threads[thread_index].join()

        df_to_sql_thread.join()


class GetHDataAsDfToMysqlClass(threading.Thread):
    def __init__(self, h_df_lists, code_df, index, engine):
        threading.Thread.__init__(self)
        self.h_df_lists = h_df_lists
        self.code_df = code_df
        self.index = index
        self.engine = engine

    # The all threading average the all mod data
    def get_h_data_as_df(self):
        value_mod = len(self.code_df) % MAX_THREAD_NUM

        if self.index < value_mod:
            start_index = self.index * int(len(self.code_df) / MAX_THREAD_NUM + 1)
            end_index = (self.index + 1) * int(len(self.code_df) / MAX_THREAD_NUM + 1)
        else:
            start_index = self.index * int(len(self.code_df) / MAX_THREAD_NUM) + value_mod
            end_index = (self.index + 1) * int(len(self.code_df) / MAX_THREAD_NUM) + value_mod

        if start_index >= len(self.code_df):
            return

        for index in range(start_index, end_index):
            code = self.code_df.iloc[index, TODAY_CODE_INDEX]
            code = str(code).zfill(6)

            last_trade_date = TradeTimeClass.get_last_trade_day(datetime.datetime.today(), k_type='D')
            end_date_str = last_trade_date.strftime('%Y-%m-%d')

            h_select_str = 'select date, code from {} where code = "{}" order by date desc limit 1'.\
                format(H_TABLE, code)
            code_last_date_df = get_sql_query_from_mysql(self.engine, h_select_str)

            if not len(code_last_date_df):
                basics_select_str = 'select timeToMarket from {} where code = "{}"'.format(BASICS_TABLE, code)
                time_to_market_df = get_sql_query_from_mysql(self.engine, basics_select_str)

                # not the related code data in basics table
                if len(time_to_market_df) > 0:
                    start_date_str = str(time_to_market_df.iloc[0, 0])

                    if '0' == start_date_str:
                        # 未上市
                        continue
                    else:
                        start_date_str = '{}-{}-{}'.format(start_date_str[0:4], start_date_str[4:6], start_date_str[6:])
                else:
                    start_date_str = OLDEST_TRADE_DATA
            else:
                last_date = str(code_last_date_df.iloc[0, 0])

                if datetime.datetime.strptime(last_date, '%Y-%m-%d') \
                        >= datetime.datetime.strptime(end_date_str, '%Y-%m-%d'):
                    continue
                else:
                    datetime_last_date = datetime.datetime.strptime(last_date, '%Y-%m-%d')
                    next_date = datetime_last_date + datetime.timedelta(days=1)
                    start_date_str = next_date.strftime('%Y-%m-%d')

            logging.info('code: {}, start_date_str = {}, end_date_str = {}'.format(code, start_date_str, end_date_str))
            h_df = get_stock_history_h_data(code, start_date=start_date_str, end_date=end_date_str)

            if len(h_df):
                h_df.insert(0, 'date', h_df.index)
                h_df.insert(1, 'code', code)

                h_df.to_sql(H_TABLE, con=self.engine, if_exists='append', index=False)

                # self.h_df_lists.append(h_df)
            else:
                logging.info('The h_df[{}] is empty while the code is: {}'.format('D', code))

    def run(self):
        self.get_h_data_as_df()


def update_h_data_as_df_to_mysql(cnx_engine):
    logging.info('Begin to update the h data table')
    get_h_data_threads = []
    h_df_lists = []

    code_select_str = 'select code from {}'.format(TODAY_CODE_TABLE)
    code_df = get_sql_query_from_mysql(cnx_engine, code_select_str)

    for thread_index in range(MAX_THREAD_NUM):
        threads = GetHDataAsDfToMysqlClass(h_df_lists, code_df, thread_index, cnx_engine)
        get_h_data_threads.append(threads)

    for thread_index in range(len(get_h_data_threads)):
        get_h_data_threads[thread_index].start()

    for thread_index in range(len(get_h_data_threads)):
        get_h_data_threads[thread_index].join()


def main():
    mysql_engine = connect_database(db_settings)

    update_today_data_and_code_data_from_today_all_to_mysql(mysql_engine)
    update_basics_data_to_mysql(mysql_engine)
    # update_industry_data_to_mysql(mysql_engine)
    # update_concept_data_to_mysql(mysql_engine)
    # update_area_data_to_mysql(mysql_engine)

    # update_k_data_to_mysql(mysql_engine)
    # update_k_data_as_df_to_mysql(mysql_engine)

    # update_hist_data_to_mysql(mysql_engine)
    update_hist_data_as_df_to_mysql(mysql_engine)

    # 该接口获取数据较慢，所以最好少用，就比k_data多了一个成交金额
    # update_h_data_as_df_to_mysql(mysql_engine)

    disconnect_database(mysql_engine)


if __name__ == "__main__":
    logging_config(LOGGING_LEVEL)

    logging.info('Script start execution at {}\n'.format(datetime.datetime.now()))

    time_start = time.time()
    main()

    logging.info('Script end execution at {}\n'.format(datetime.datetime.now()))
    logging.info('Total Elapsed Time: {} seconds\n'.format(time.time() - time_start))

```



