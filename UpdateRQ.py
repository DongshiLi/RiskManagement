import pandas as pd
import numpy as np
from datetime import datetime , timedelta
from datetime import date, time
import time as t
import rqdatac as rq
from rqdatac import *
import pymysql
import logging
import re
import os
import math

rq.init()

Config = {
    #模式分为：backtest ; simulation
    'mode' : 'simulation',
    'Start_date' : '20200812',
    'End_date' : '20200812'
}

name_list = ['book_to_price', 'comovement', 'liquidity', 'growth', 'non_linear_size', 'beta', 'leverage', 'residual_volatility', 'momentum', 'earnings_yield', 'size',
             '农林牧渔', '采掘', '化工', '钢铁', '有色金属', '电子', '家用电器', '食品饮料', '纺织服装', '轻工制造', '医药生物', '公用事业', '交通运输', '房地产', '商业贸易', '休闲服务', 
             '综合', '建筑材料', '建筑装饰', '电气设备', '国防军工', '计算机', '传媒', '通信', '银行', '非银金融', '汽车', '机械设备']

time_list = ['daily', 'monthly', 'quarterly']

universe_list = ['whole_market', '000300.XSHG', '000905.XSHG', '000906.XSHG']

universe_name = {
    'whole_market': 'whole_market',
    '000300.XSHG': 'csi_300', 
    '000905.XSHG': 'csi_500',
    '000906.XSHG': 'csi_800'
}

beta_universe = ['000300.XSHG', '000016.XSHG', '000905.XSHG', '000906.XSHG', '000985.XSHG']

method_list = ['implicit', 'explicit']

#读库
DB_INFO=\
    dict()         
conn=\
    pymysql.connect(** DB_INFO,charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

#写库
WRITE_DB_INFO=\
    dict()         
write_conn=\
    pymysql.connect(** WRITE_DB_INFO,charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

def GetFactorCovariance(date, horizon):

    print("Get %s Factor Covariance on %s"%(str(horizon), str(date)))

    df = get_factor_covariance(date, horizon= str(horizon))

    for i in range(len(name_list)):
        for j in range(len(name_list)):
            write_rq_query = "UPDATE %s_factor_covariance SET %s = '%s' WHERE date = '%s' AND Type = '%s'"%(str(horizon), str(name_list[j]), str(float(df.loc[name_list[i], name_list[j]])), str(date), str(name_list[i]))

            rq_cursor = write_conn.cursor()

            check_rq_query = "SELECT * FROM %s_factor_covariance WHERE Type = '%s' AND date = '%s'"%(str(horizon), name_list[i], date)
            check_rq_df = pd.read_sql_query(check_rq_query, write_conn)
            if len(check_rq_df) == 0:
                build_rq_query = "INSERT INTO `RQ`.`%s_factor_covariance`(`Type`, `date`) VALUES ('%s', '%s')"%(str(horizon), name_list[i], date)
                rq_cursor = write_conn.cursor()
                rq_cursor.execute(build_rq_query)
                write_conn.commit()
                try:
                    rq_cursor.execute(write_rq_query)
                    write_conn.commit()
                    #print('update data successful')
                except Exception as e:
                    print("the failure of updating data is ：case%s"%e)
                    #发生错误是回滚
                    write_conn.rollback()
            else:
                try:
                    rq_cursor.execute(write_rq_query)
                    write_conn.commit()
                    #print('update data successful')
                except Exception as e:
                    print("the failure of updating data is ：case%s"%e)
                    #发生错误是回滚
                    write_conn.rollback()

    return df

def GetSpecficRisk(date, horizon):

    print("Get %s Specific Risk on %s"%(str(horizon), str(date)))

    stock_list = all_instruments(type='CS', market='cn', date=date)['order_book_id'].tolist()
    df = get_specific_risk(stock_list, date, date, horizon=horizon)


    for i in range(len(stock_list)):
        write_rq_query = "UPDATE %s_specific_risk SET specific_risk = '%s' WHERE date = '%s' AND order_book_id = '%s'"%(str(horizon), str(float(df[stock_list[i]])), str(date), str(stock_list[i]))
        

        rq_cursor = write_conn.cursor()

        check_rq_query = "SELECT * FROM %s_specific_risk WHERE order_book_id = '%s' AND date = '%s'"%(str(horizon), str(stock_list[i]), date)
        check_rq_df = pd.read_sql_query(check_rq_query, write_conn)
        if len(check_rq_df) == 0:
            build_rq_query = "INSERT INTO `RQ`.`%s_specific_risk`(`order_book_id`, `date`) VALUES ('%s', '%s')"%(str(horizon), str(stock_list[i]), date)
            rq_cursor = write_conn.cursor()
            rq_cursor.execute(build_rq_query)
            write_conn.commit()
            try:
                rq_cursor.execute(write_rq_query)
                write_conn.commit()
                #print('update data successful')
            except Exception as e:
                print("the failure of updating data is ：case%s"%e)
                #发生错误是回滚
                write_conn.rollback()
        else:
            try:
                rq_cursor.execute(write_rq_query)
                write_conn.commit()
                #print('update data successful')
            except Exception as e:
                print("the failure of updating data is ：case%s"%e)
                #发生错误是回滚
                write_conn.rollback()

    return df


def GetDescriptorFactor(date):

    print("Get Descriptor Factor on %s"%str(date))

    stock_list = all_instruments(type='CS', market='cn', date=date)['order_book_id'].tolist()
    df = get_descriptor_exposure(stock_list, date, date, descriptors=None)
    df = df.reset_index().set_index(['order_book_id'])

    for i in range(len(stock_list)):
        write_rq_query = "UPDATE descriptor_factor SET debt_to_assets = '" + str(df.loc[stock_list[i], 'debt_to_assets']) + \
                    "' , market_leverage = '" + str(df.loc[stock_list[i], 'market_leverage']) + \
                    "' , three_months_share_turnover = '" + str(df.loc[stock_list[i], 'three_months_share_turnover']) + \
                    "' , twelve_months_share_turnover = '" + str(df.loc[stock_list[i], 'twelve_months_share_turnover']) + \
                    "' , cash_earnings_to_price_ratio = '" + str(df.loc[stock_list[i], 'cash_earnings_to_price_ratio']) + \
                    "' , one_month_share_turnover = '" + str(df.loc[stock_list[i], 'one_month_share_turnover']) + \
                    "' , book_leverage = '" + str(df.loc[stock_list[i], 'book_leverage']) + \
                    "' , historical_sigma = '" + str(df.loc[stock_list[i], 'historical_sigma']) + \
                    "' , earnings_growth = '" + str(df.loc[stock_list[i], 'earnings_growth']) + \
                    "' , cumulative_range = '" + str(df.loc[stock_list[i], 'cumulative_range']) + \
                    "' , daily_standard_deviation = '" + str(df.loc[stock_list[i], 'daily_standard_deviation']) + \
                    "' , earnings_to_price_ratio = '" + str(df.loc[stock_list[i], 'earnings_to_price_ratio']) + \
                    "' , sales_growth = '" + str(df.loc[stock_list[i], 'sales_growth']) + \
                    "' WHERE date = '" + str(date) + \
                    "' AND order_book_id = '%s'"%str(stock_list[i])
        

        rq_cursor = write_conn.cursor()

        check_rq_query = "SELECT * FROM descriptor_factor WHERE order_book_id = '%s' AND date = '%s'"%(str(stock_list[i]), date)
        check_rq_df = pd.read_sql_query(check_rq_query, write_conn)
        if len(check_rq_df) == 0:
            build_rq_query = "INSERT INTO `RQ`.`descriptor_factor`(`order_book_id`, `date`) VALUES ('%s', '%s')"%(str(stock_list[i]), date)
            rq_cursor = write_conn.cursor()
            rq_cursor.execute(build_rq_query)
            write_conn.commit()
            try:
                rq_cursor.execute(write_rq_query)
                write_conn.commit()
                #print('update data successful')
            except Exception as e:
                print("the failure of updating data is ：case%s"%e)
                #发生错误是回滚
                write_conn.rollback()
        else:
            try:
                rq_cursor.execute(write_rq_query)
                write_conn.commit()
                #print('update data successful')
            except Exception as e:
                print("the failure of updating data is ：case%s"%e)
                #发生错误是回滚
                write_conn.rollback()

    return df

def GetFactorReturn(date, method):

    print("Get %s Factor Return on %s"%(str(method), str(date)))

    for i in range(len(universe_list)):
        df = get_factor_return(date, date,factors= None, universe=universe_list[i], method= method,industry_mapping=True)
        col_list = df.columns.values.tolist()
        for j in range(len(col_list)):
            write_rq_query = "UPDATE %s_factor_return SET %s = '%s' WHERE date = '%s' AND factor = '%s'"%(str(method), str(universe_name[universe_list[i]]), str(float(df[col_list[j]])), str(date), str(col_list[j]))

            rq_cursor = write_conn.cursor()

            check_rq_query = "SELECT * FROM %s_factor_return WHERE factor = '%s' AND date = '%s'"%(str(method), col_list[j], date)
            check_rq_df = pd.read_sql_query(check_rq_query, write_conn)
            if len(check_rq_df) == 0:
                build_rq_query = "INSERT INTO `RQ`.`%s_factor_return`(`factor`, `date`) VALUES ('%s', '%s')"%(str(method), col_list[j], date)
                rq_cursor = write_conn.cursor()
                rq_cursor.execute(build_rq_query)
                write_conn.commit()
                try:
                    rq_cursor.execute(write_rq_query)
                    write_conn.commit()
                    #print('update data successful')
                except Exception as e:
                    print("the failure of updating data is ：case%s"%e)
                    #发生错误是回滚
                    write_conn.rollback()
            else:
                try:
                    rq_cursor.execute(write_rq_query)
                    write_conn.commit()
                    #print('update data successful')
                except Exception as e:
                    print("the failure of updating data is ：case%s"%e)
                    #发生错误是回滚
                    write_conn.rollback()

    return df


def GetFactorExposure(date):

    print("Get Factor Exposure on %s"%str(date))


    stock_list = all_instruments(type='CS', market='cn', date=date)['order_book_id'].tolist()
    df = get_factor_exposure(stock_list, str(date), str(date),factors=None,industry_mapping=True)

    df = df.reset_index()

    industry = name_list[-28:]

    IndustryDict = dict()
    for i in range(len(industry)):
        List = df.loc[df[industry[i]] == 1, 'order_book_id'].tolist()
        for j in range(len(List)):
            df.loc[df['order_book_id'] == List[j], 'industry'] = industry[i]

    final = name_list[:11]
    final.insert(0, 'order_book_id')
    final.extend(['industry'])
    df = df[final]
    df = df.set_index(['order_book_id'])
    

    for i in range(len(stock_list)):
        write_rq_query = "UPDATE factor_exposure SET comovement = '" + str(df.loc[stock_list[i], 'comovement']) + \
                    "' , industry = '" + str(df.loc[stock_list[i], 'industry']) + \
                    "' , liquidity = '" + str(df.loc[stock_list[i], 'liquidity']) + \
                    "' , non_linear_size = '" + str(df.loc[stock_list[i], 'non_linear_size']) + \
                    "' , growth = '" + str(df.loc[stock_list[i], 'growth']) + \
                    "' , earnings_yield = '" + str(df.loc[stock_list[i], 'earnings_yield']) + \
                    "' , residual_volatility = '" + str(df.loc[stock_list[i], 'residual_volatility']) + \
                    "' , book_to_price = '" + str(df.loc[stock_list[i], 'book_to_price']) + \
                    "' , beta = '" + str(df.loc[stock_list[i], 'beta']) + \
                    "' , leverage = '" + str(df.loc[stock_list[i], 'leverage']) + \
                    "' , size = '" + str(df.loc[stock_list[i], 'size']) + \
                    "' , momentum = '" + str(df.loc[stock_list[i], 'momentum']) + \
                    "' WHERE date = '" + str(date) + \
                    "' AND order_book_id = '%s'"%str(stock_list[i])

        rq_cursor = write_conn.cursor()

        check_rq_query = "SELECT * FROM factor_exposure WHERE order_book_id = '%s' AND date = '%s'"%(stock_list[i], date)
        check_rq_df = pd.read_sql_query(check_rq_query, write_conn)
        if len(check_rq_df) == 0:
            build_rq_query = "INSERT INTO `RQ`.`factor_exposure`(`order_book_id`, `date`) VALUES ('%s', '%s')"%(stock_list[i], date)
            rq_cursor = write_conn.cursor()
            rq_cursor.execute(build_rq_query)
            write_conn.commit()
            try:
                rq_cursor.execute(write_rq_query)
                write_conn.commit()
                #print('update data successful')
            except Exception as e:
                print("the failure of updating data is ：case%s"%e)
                #发生错误是回滚
                write_conn.rollback()
        else:
            try:
                rq_cursor.execute(write_rq_query)
                write_conn.commit()
                #print('update data successful')
            except Exception as e:
                print("the failure of updating data is ：case%s"%e)
                #发生错误是回滚
                write_conn.rollback()

    return df


def GetSpecficReturn(date):

    print("Get Specific Return on %s"%str(date))

    stock_list = all_instruments(type='CS', market='cn', date=date)['order_book_id'].tolist()
    df = get_specific_return(stock_list, date, date)


    for i in range(len(stock_list)):
        write_rq_query = "UPDATE specific_return SET specific_return = '" + str(float(df[stock_list[i]])) + \
                    "' WHERE date = '" + str(date) + \
                    "' AND order_book_id = '%s'"%str(stock_list[i])
        

        rq_cursor = write_conn.cursor()

        check_rq_query = "SELECT * FROM specific_return WHERE order_book_id = '%s' AND date = '%s'"%(str(stock_list[i]), date)
        check_rq_df = pd.read_sql_query(check_rq_query, write_conn)
        if len(check_rq_df) == 0:
            build_rq_query = "INSERT INTO `RQ`.`specific_return`(`order_book_id`, `date`) VALUES ('%s', '%s')"%(str(stock_list[i]), date)
            rq_cursor = write_conn.cursor()
            rq_cursor.execute(build_rq_query)
            write_conn.commit()
            try:
                rq_cursor.execute(write_rq_query)
                write_conn.commit()
                #print('update data successful')
            except Exception as e:
                print("the failure of updating data is ：case%s"%e)
                #发生错误是回滚
                write_conn.rollback()
        else:
            try:
                rq_cursor.execute(write_rq_query)
                write_conn.commit()
                #print('update data successful')
            except Exception as e:
                print("the failure of updating data is ：case%s"%e)
                #发生错误是回滚
                write_conn.rollback()

    return df

def GetBeta(date):

    print("Get Beta on %s"%str(date))

    stock_list = all_instruments(type='CS', market='cn', date=date)['order_book_id'].tolist()
    for i in range(len(beta_universe)):
        df = get_stock_beta(stock_list, date, date, benchmark=beta_universe[i])
        for j in range(len(stock_list)):
            write_rq_query = "UPDATE stock_beta SET %s = '%s' WHERE date = '%s' AND order_book_id = '%s'"%(str(beta_universe[i][:6]) + '_' + str(beta_universe[i][-4:]), str(float(df[stock_list[j]])), date, str(stock_list[j]))

            rq_cursor = write_conn.cursor()

            check_rq_query = "SELECT * FROM stock_beta WHERE order_book_id = '%s' AND date = '%s'"%(str(stock_list[j]), date)
            check_rq_df = pd.read_sql_query(check_rq_query, write_conn)
            if len(check_rq_df) == 0:
                build_rq_query = "INSERT INTO `RQ`.`stock_beta`(`order_book_id`, `date`) VALUES ('%s', '%s')"%(str(stock_list[j]), date)
                rq_cursor = write_conn.cursor()
                rq_cursor.execute(build_rq_query)
                write_conn.commit()
                try:
                    rq_cursor.execute(write_rq_query)
                    write_conn.commit()
                    #print('update data successful')
                except Exception as e:
                    print("the failure of updating data is ：case%s"%e)
                    #发生错误是回滚
                    write_conn.rollback()
            else:
                try:
                    rq_cursor.execute(write_rq_query)
                    write_conn.commit()
                    #print('update data successful')
                except Exception as e:
                    print("the failure of updating data is ：case%s"%e)
                    #发生错误是回滚
                    write_conn.rollback()

    return df

    

def GetLastTradeDate(date):
    sql = "SELECT TRADE_DAYS FROM ASHARECALENDAR WHERE S_INFO_EXCHMARKET = 'SZSE' AND TRADE_DAYS <= '%s' ORDER BY TRADE_DAYS DESC LIMIT 0, 1000"%date
    td=pd.read_sql_query(sql, conn)
    if td.iloc[0]['TRADE_DAYS'] == str(date):
        return td.iloc[1]['TRADE_DAYS']
