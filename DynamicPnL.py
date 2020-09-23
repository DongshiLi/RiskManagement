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

risk_list = ['risk_size', 'risk_beta', 'risk_sizenl', 'risk_vol', 'risk_mom', 'risk_BookToPrice', 'risk_comove', 'risk_liquidity', 'risk_growth', 'risk_leverage', 'risk_EarningsYield']


Config = {
    #计算RMSE和Correlation的文件路径：分为实盘和模拟盘
    'CorrPath' : '',
    #RMSE和Correlation文件名，除读取数据以外，用于判别实盘和模拟盘
    #如果为模拟盘，文件名为，取当天日期文件，同时用前一个交易日的risk计算前N%减后N%的风险因子收益率
    #如果为实盘，文件名为，取前一个交易日日期的文件，用前一日交易日的risk计算前N%减后N%的风险因子收益率
    'CorrFileName' : '',
    #计算持仓组合因子收益率的文件路径：分为实盘和模拟盘
    'PortfolioPath' : '',
    #持仓权重文件名
    #如果为模拟盘，文件名为，取当天日期文件，同时取上一个交易日的风险暴露度和计算当日前N%减后N%风险收益率
    #如果为实盘，文件名为，取当天日期文件，同时取上一个交易日的风险暴露度和计算当日前N%减后N%风险收益率
    'PortfolioFileName' : '',
    #指数rebalance日期，需在当日使用新的指数权重
    'RebalanceDate': '20200615',
    #基准指数
    'Index' : '000905.SH',
    #sleep时间，控制计算的刷新时间间隔
    'sleep' : '300',
    #设置取TOP和LOW的股票比例，如前百分之50减去后百分之50的股票收益
    'Multiplier' : '50'
}

#读库
DB_INFO=\
    dict()         
conn=\
    pymysql.connect(** DB_INFO,charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

def GetLastTradeDate(date):
    sql = "SELECT TRADE_DAYS FROM ASHARECALENDAR WHERE S_INFO_EXCHMARKET = 'SZSE' AND TRADE_DAYS <= '%s' ORDER BY TRADE_DAYS DESC LIMIT 0, 1000"%date
    td=pd.read_sql_query(sql, conn)
    if td.iloc[0]['TRADE_DAYS'] == str(date):
        return td.iloc[1]['TRADE_DAYS']

def gen_weight(index, date):
    weight_sql = "SELECT S_CON_WINDCODE, I_WEIGHT/100" + \
                " FROM AINDEXHS300FREEWEIGHT" + \
                " WHERE S_INFO_WINDCODE = '" + str(index) + \
                "' AND TRADE_DT = '" + str(date) + \
                "' ORDER BY S_INFO_WINDCODE"
    WEIGHT = pd.read_sql_query(weight_sql, conn)
    return WEIGHT


if __name__ == '__main__':
    today = str((datetime.today() - timedelta(days = 0)).strftime('%Y%m%d'))
    LastDate = GetLastTradeDate(today)
    print(today)


    factor_return = get_factor_return(str(LastDate), str(LastDate), factors= None, universe=id_convert(Config['Index']),method='implicit',industry_mapping=True)
    factor_return.rename(columns={'size': 'risk_size', 'beta': 'risk_beta', 'non_linear_size': 'risk_sizenl', 'residual_volatility': 'risk_vol', 'momentum': 'risk_mom', 'book_to_price': 'risk_BookToPrice', 'comovement': 'risk_comove', 'liquidity': 'risk_liquidity', 'growth': 'risk_growth', 'leverage': 'risk_leverage', 'earnings_yield': 'risk_EarningsYield'}, inplace=True)
    
    
    write_rq_query = "UPDATE WEB_DYNAMICPNL SET risk_size_Pnl = '" + str(float(factor_return['risk_size'] * 100)) + \
                    "' , risk_beta_Pnl = '" + str(float(factor_return['risk_beta'] * 100)) + \
                    "' , risk_sizenl_Pnl = '" + str(float(factor_return['risk_sizenl'] * 100)) + \
                    "' , risk_vol_Pnl = '" + str(float(factor_return['risk_vol'] * 100)) + \
                    "' , risk_mom_Pnl = '" + str(float(factor_return['risk_mom'] * 100)) + \
                    "' , risk_BookToPrice_Pnl = '" + str(float(factor_return['risk_BookToPrice'] * 100)) + \
                    "' , risk_liquidity_Pnl = '" + str(float(factor_return['risk_liquidity'] * 100)) + \
                    "' , risk_growth_Pnl = '" + str(float(factor_return['risk_growth'] * 100)) + \
                    "' , risk_leverage_Pnl = '" + str(float(factor_return['risk_leverage'] * 100)) + \
                    "' , risk_earningsYield_Pnl = '" + str(float(factor_return['risk_EarningsYield'] * 100)) + \
                    "' WHERE TradeDay = '" + str(today) + \
                    "' AND Type = 'RQ'"
    
    rq_cursor = conn.cursor()

    check_rq_query = "SELECT * FROM WEB_DYNAMICPNL WHERE Type = 'RQ' AND TradeDay = '%s'"%today
    check_rq_df = pd.read_sql_query(check_rq_query, conn)
    if len(check_rq_df) == 0:
        build_rq_query = "INSERT INTO `WIND`.`WEB_DYNAMICPNL`(`Type`, `TradeDay`) VALUES ('RQ', '%s')"%today
        rq_cursor = conn.cursor()
        rq_cursor.execute(build_rq_query)
        conn.commit()
        try:
            rq_cursor.execute(write_rq_query)
            conn.commit()
            print('update data successful')
        except Exception as e:
            print("the failure of updating data is ：case%s"%e)
            #发生错误是回滚
            conn.rollback()
    else:
        try:
            rq_cursor.execute(write_rq_query)
            conn.commit()
            print('update data successful')
        except Exception as e:
            print("the failure of updating data is ：case%s"%e)
            #发生错误是回滚
            conn.rollback()

    stock_list = all_instruments(type='CS', market='cn', date=today)['order_book_id'].tolist()

    risk = get_factor_exposure(stock_list, str(LastDate), str(LastDate),factors=None,industry_mapping=True)
    risk = risk.reset_index()
    risk.rename(columns={'order_book_id': 'sid', 'size': 'risk_size', 'beta': 'risk_beta', 'non_linear_size': 'risk_sizenl', 'residual_volatility': 'risk_vol', 'momentum': 'risk_mom', 'book_to_price': 'risk_BookToPrice', 'comovement': 'risk_comove', 'liquidity': 'risk_liquidity', 'growth': 'risk_growth', 'leverage': 'risk_leverage', 'earnings_yield': 'risk_EarningsYield'}, inplace=True)
    #risk = risk[['sid', 'risk_size', 'risk_beta', 'risk_sizenl', 'risk_vol', 'risk_mom', 'risk_BookToPrice', 'risk_comove', 'risk_liquidity', 'risk_growth', 'risk_leverage', 'risk_EarningsYield']]
    
    

    length = int(round(len(risk) / 100,0))

    Top_List = dict()
    Low_List = dict()

    for i in range(len(risk_list)):
        Top_List[risk_list[i]] = risk.sort_values(by = [risk_list[i]])[-length * int(Config['Multiplier']):]['sid'].tolist()
        Low_List[risk_list[i]] = risk.sort_values(by = [risk_list[i]])[:length * int(Config['Multiplier'])]['sid'].tolist()

    for i in range(len(risk)):
        if ((risk.loc[i, 'sid'][:1] == '3') | (risk.loc[i, 'sid'][:1] == '0')):
            risk.loc[i, 'sid'] = risk.loc[i, 'sid'][0:6] + '.SZ'
        elif risk.loc[i, 'sid'][:1] == '6':
            risk.loc[i, 'sid'] = risk.loc[i, 'sid'][0:6] + '.SH'


    dt = datetime.strptime(str(today), '%Y%m%d')
    if dt == datetime.strptime(str(Config['RebalanceDate']), '%Y%m%d'):
        df_weight = gen_weight(Config['Index'], Config['RebalanceDate'])
    else:
        i = 1
        while(True):
            df_weight = gen_weight(Config['Index'], str((datetime.strptime("%s" %str(today), '%Y%m%d') - timedelta(days = i)).strftime('%Y%m%d')))
            if len(df_weight) == 0:
                i = i + 1
            else:
                break
    
    df_weight = df_weight.rename(columns={'S_CON_WINDCODE': 'sid', 'I_WEIGHT/100': 'bm_w'})
    df_weight = df_weight.set_index(['sid'])

    if Config['CorrFileName'] == str('alpha_500_'):
        print('Now Calculating alpha_500_ File RMSE & correlation')

        alpha = pd.read_csv(Config['CorrPath'] + Config['CorrFileName'] + today + '.csv')
        alpha.rename(columns={'Unnamed: 0': 'sid'}, inplace=True)

        portfolio_weight = pd.read_csv(Config['PortfolioPath'] + Config['PortfolioFileName'] + today + '.csv')
        portfolio_weight = portfolio_weight.set_index(['sid'])
        
        weight_merged = pd.merge(portfolio_weight, df_weight, how='outer', on=['sid'])
        weight_merged = weight_merged.fillna(0)
        weight_merged = weight_merged.sort_index()

        weight_merged['weight_diff'] = weight_merged['tar_w'] - weight_merged['bm_w']

        weight_merged = pd.merge(weight_merged, risk, how='inner', on=['sid'])

        while(datetime.now().time() <= time(15, 0, 0)):
        #while(True):

            t.sleep(int(Config['sleep']))
            print('Start to get snapshot tick date on ' + str(datetime.now().time()))

            alpha_Tick = current_snapshot(id_convert(alpha['sid'].tolist()))
            index_Tick = current_snapshot(id_convert(Config['Index']))

            for i in range(len(alpha_Tick)):
                if ((alpha_Tick[i]['order_book_id'][:1] == '3') | (alpha_Tick[i]['order_book_id'][:1] == '0')):
                    sid = str(alpha_Tick[i]['order_book_id'][:6]) + '.SZ'
                elif alpha_Tick[i]['order_book_id'][:1] == '6':
                    sid = str(alpha_Tick[i]['order_book_id'][:6]) + '.SH'
                try:
                    alpha.loc[alpha['sid'] == sid, 'Realalpha'] = float(alpha_Tick[i]['last'] / alpha_Tick[i]['prev_close'] - index_Tick['last'] / index_Tick['prev_close']) * 100
                except:
                    pass
                
            alpha = alpha.dropna()
            alpha['RSE'] = (alpha['alpha'] - alpha['Realalpha']) * (alpha['alpha'] - alpha['Realalpha'])
            RMSE = math.sqrt(alpha['RSE'].sum() / len(alpha))
            corr = alpha.corr().loc['alpha', 'Realalpha']
            print('RMSE now is : ' + str(RMSE))
            print('Correlation now is : ' + str(corr))

            print('Calculate risk dynamic PnL')

            df_Tick = pd.DataFrame()
            Current_Tick = current_snapshot(stock_list)
            start = datetime.now()
            for i in range(len(Current_Tick)):
                df_Tick.loc[i, 'sid'] = Current_Tick[i]['order_book_id']
                df_Tick.loc[i, 'return'] = (Current_Tick[i]['last'] / Current_Tick[i]['prev_close'] - 1) * 100
            df_Tick = df_Tick.set_index(['sid'])


            TotalDiff = dict()
            PortfolioExposure = dict()
            PortfolioPnL = dict()
            for i in range(len(risk_list)):
                TotalDiff[risk_list[i]] = (df_Tick.loc[Top_List[risk_list[i]]].sum() - df_Tick.loc[Low_List[risk_list[i]]].sum()) / (length * int(Config['Multiplier']))
                print(str(risk_list[i]) + ' PnL now is : ' + str(float(TotalDiff[risk_list[i]])))
                PortfolioExposure[risk_list[i]] = (weight_merged[risk_list[i]] * weight_merged['weight_diff']).sum()
                PortfolioPnL[risk_list[i]] = TotalDiff[risk_list[i]] * PortfolioExposure[risk_list[i]]
                print(str(risk_list[i]) + ' Portfolio PnL now is : ' + str(float(PortfolioPnL[risk_list[i]])))
            
            

            write_query = "UPDATE WEB_DYNAMICPNL SET risk_size_Pnl = '" + str(float(TotalDiff['risk_size'])) + \
                        "' , risk_beta_Pnl = '" + str(float(TotalDiff['risk_beta'])) + \
                        "' , risk_sizenl_Pnl = '" + str(float(TotalDiff['risk_sizenl'])) + \
                        "' , risk_vol_Pnl = '" + str(float(TotalDiff['risk_vol'])) + \
                        "' , risk_mom_Pnl = '" + str(float(TotalDiff['risk_mom'])) + \
                        "' , risk_BookToPrice_Pnl = '" + str(float(TotalDiff['risk_BookToPrice'])) + \
                        "' , risk_liquidity_Pnl = '" + str(float(TotalDiff['risk_liquidity'])) + \
                        "' , risk_growth_Pnl = '" + str(float(TotalDiff['risk_growth'])) + \
                        "' , risk_leverage_Pnl = '" + str(float(TotalDiff['risk_leverage'])) + \
                        "' , risk_earningsYield_Pnl = '" + str(float(TotalDiff['risk_EarningsYield'])) + \
                        "' , RMSE = '" + str(RMSE) + \
                        "' , Correlation = '" + str(corr) + \
                        "' WHERE TradeDay = '" + str(today) + \
                        "' AND Type = 'QH'"
            
            
            cursor = conn.cursor()

            check_query = "SELECT * FROM WEB_DYNAMICPNL WHERE Type = 'QH' AND TradeDay = '%s'"%today
            check_df = pd.read_sql_query(check_query, conn)
            if len(check_df) == 0:
                build_query = "INSERT INTO `WIND`.`WEB_DYNAMICPNL`(`Type`, `TradeDay`) VALUES ('QH', '%s')"%today
                cursor = conn.cursor()
                cursor.execute(build_query)
                conn.commit()
                try:
                    cursor.execute(write_query)
                    conn.commit()
                    print('update data successful')
                except Exception as e:
                    print("the failure of updating data is ：case%s"%e)
                    #发生错误是回滚
                    conn.rollback()
            else:
                try:
                    cursor.execute(write_query)
                    conn.commit()
                    print('update data successful')
                except Exception as e:
                    print("the failure of updating data is ：case%s"%e)
                    #发生错误是回滚
                    conn.rollback()

            write_portfolio_query = "UPDATE WEB_DYNAMICPNL SET risk_size_Pnl = '" + str(float(PortfolioPnL['risk_size'])) + \
                                    "' , risk_beta_Pnl = '" + str(float(PortfolioPnL['risk_beta'])) + \
                                    "' , risk_sizenl_Pnl = '" + str(float(PortfolioPnL['risk_sizenl'])) + \
                                    "' , risk_vol_Pnl = '" + str(float(PortfolioPnL['risk_vol'])) + \
                                    "' , risk_mom_Pnl = '" + str(float(PortfolioPnL['risk_mom'])) + \
                                    "' , risk_BookToPrice_Pnl = '" + str(float(PortfolioPnL['risk_BookToPrice'])) + \
                                    "' , risk_liquidity_Pnl = '" + str(float(PortfolioPnL['risk_liquidity'])) + \
                                    "' , risk_growth_Pnl = '" + str(float(PortfolioPnL['risk_growth'])) + \
                                    "' , risk_leverage_Pnl = '" + str(float(PortfolioPnL['risk_leverage'])) + \
                                    "' , risk_earningsYield_Pnl = '" + str(float(PortfolioPnL['risk_EarningsYield'])) + \
                                    "' , RMSE = '" + str(RMSE) + \
                                    "' , Correlation = '" + str(corr) + \
                                    "' WHERE TradeDay = '" + str(today) + \
                                    "' AND Type = 'QH_Portfolio'"
            
            
            portfolio_cursor = conn.cursor()

            check_portfolio_query = "SELECT * FROM WEB_DYNAMICPNL WHERE Type = 'QH_Portfolio' AND TradeDay = '%s'"%today
            check_portfolio_df = pd.read_sql_query(check_portfolio_query, conn)
            if len(check_portfolio_df) == 0:
                build_portfolio_query = "INSERT INTO `WIND`.`WEB_DYNAMICPNL`(`Type`, `TradeDay`) VALUES ('QH_Portfolio', '%s')"%today
                portfolio_cursor = conn.cursor()
                portfolio_cursor.execute(build_portfolio_query)
                conn.commit()
                try:
                    portfolio_cursor.execute(write_portfolio_query)
                    conn.commit()
                    print('update data successful')
                except Exception as e:
                    print("the failure of updating data is ：case%s"%e)
                    #发生错误是回滚
                    conn.rollback()
            else:
                try:
                    portfolio_cursor.execute(write_portfolio_query)
                    conn.commit()
                    print('update data successful')
                except Exception as e:
                    print("the failure of updating data is ：case%s"%e)
                    #发生错误是回滚
                    conn.rollback()

        print('Now is out of trade time range')

    elif Config['CorrFileName'] == str('trading_500_'):
        print('Now Calculating trading_500_ File RMSE & correlation')

        alpha = pd.read_csv(Config['CorrPath'] + Config['CorrFileName'] + LastDate + '.csv')

        portfolio_weight = pd.read_csv(Config['PortfolioPath'] + today + '/' + Config['PortfolioFileName'] + today + '.csv')
        portfolio_weight = portfolio_weight.set_index(['sid'])
        
        weight_merged = pd.merge(portfolio_weight, df_weight, how='outer', on=['sid'])
        weight_merged = weight_merged.fillna(0)
        weight_merged = weight_merged.sort_index()

        weight_merged['weight_diff'] = weight_merged['tar_w'] - weight_merged['bm_w']

        weight_merged = pd.merge(weight_merged, risk, how='inner', on=['sid'])


        while(datetime.now().time() <= time(15, 0, 0)):
        #while(True):

            t.sleep(int(Config['sleep']))
            print('Start to get snapshot tick date on ' + str(datetime.now().time()))

            alpha_Tick = current_snapshot(id_convert(alpha['sid'].tolist()))
            index_Tick = current_snapshot(id_convert(Config['Index']))

            
            for i in range(len(alpha_Tick)):
                if ((alpha_Tick[i]['order_book_id'][:1] == '3') | (alpha_Tick[i]['order_book_id'][:1] == '0')):
                    sid = str(alpha_Tick[i]['order_book_id'][:6]) + '.SZ'
                elif alpha_Tick[i]['order_book_id'][:1] == '6':
                    sid = str(alpha_Tick[i]['order_book_id'][:6]) + '.SH'
                try:
                    alpha.loc[alpha['sid'] == sid, 'Realalpha'] = float(alpha_Tick[i]['last'] / alpha_Tick[i]['prev_close'] - index_Tick['last'] / index_Tick['prev_close']) * 100
                except:
                    pass
                
            alpha = alpha.dropna()
            alpha['RSE'] = (alpha['alpha'] * 100 - alpha['Realalpha']) * (alpha['alpha'] * 100 - alpha['Realalpha'])
            RMSE = math.sqrt(alpha['RSE'].sum() / len(alpha))
            corr = alpha.corr().loc['alpha', 'Realalpha']
            print('RMSE now is : ' + str(RMSE))
            print('Correlation now is : ' + str(corr))

            print('Calculate risk dynamic PnL')

            df_Tick = pd.DataFrame()
            Current_Tick = current_snapshot(stock_list)

            for i in range(len(Current_Tick)):
                df_Tick.loc[i, 'sid'] = Current_Tick[i]['order_book_id']
                df_Tick.loc[i, 'return'] = (Current_Tick[i]['last'] / Current_Tick[i]['prev_close'] - 1) * 100
            df_Tick = df_Tick.set_index(['sid'])


            TotalDiff = dict()
            PortfolioExposure = dict()
            PortfolioPnL = dict()
            for i in range(len(risk_list)):
                TotalDiff[risk_list[i]] = (df_Tick.loc[Top_List[risk_list[i]]].sum() - df_Tick.loc[Low_List[risk_list[i]]].sum()) / (length * int(Config['Multiplier']))
                print(str(risk_list[i]) + ' PnL now is : ' + str(float(TotalDiff[risk_list[i]])))
                PortfolioExposure[risk_list[i]] = (weight_merged[risk_list[i]] * weight_merged['weight_diff']).sum()
                PortfolioPnL[risk_list[i]] = TotalDiff[risk_list[i]] * PortfolioExposure[risk_list[i]]
                print(str(risk_list[i]) + ' Portfolio PnL now is : ' + str(float(PortfolioPnL[risk_list[i]])))
            

            write_query = "UPDATE WEB_DYNAMICPNL SET risk_size_Pnl = '" + str(float(TotalDiff['risk_size'])) + \
                        "' , risk_beta_Pnl = '" + str(float(TotalDiff['risk_beta'])) + \
                        "' , risk_sizenl_Pnl = '" + str(float(TotalDiff['risk_sizenl'])) + \
                        "' , risk_vol_Pnl = '" + str(float(TotalDiff['risk_vol'])) + \
                        "' , risk_mom_Pnl = '" + str(float(TotalDiff['risk_mom'])) + \
                        "' , risk_BookToPrice_Pnl = '" + str(float(TotalDiff['risk_BookToPrice'])) + \
                        "' , risk_liquidity_Pnl = '" + str(float(TotalDiff['risk_liquidity'])) + \
                        "' , risk_growth_Pnl = '" + str(float(TotalDiff['risk_growth'])) + \
                        "' , risk_leverage_Pnl = '" + str(float(TotalDiff['risk_leverage'])) + \
                        "' , risk_earningsYield_Pnl = '" + str(float(TotalDiff['risk_EarningsYield'])) + \
                        "' , RMSE = '" + str(RMSE) + \
                        "' , Correlation = '" + str(corr) + \
                        "' WHERE TradeDay = '" + str(today) + \
                        "' AND Type = 'QH'"
            
            
            cursor = conn.cursor()

            check_query = "SELECT * FROM WEB_DYNAMICPNL WHERE Type = 'QH' AND TradeDay = '%s'"%today
            check_df = pd.read_sql_query(check_query, conn)
            if len(check_df) == 0:
                build_query = "INSERT INTO `WIND`.`WEB_DYNAMICPNL`(`Type`, `TradeDay`) VALUES ('QH', '%s')"%today
                cursor = conn.cursor()
                cursor.execute(build_query)
                conn.commit()
                try:
                    cursor.execute(write_query)
                    conn.commit()
                    print('update data successful')
                except Exception as e:
                    print("the failure of updating data is ：case%s"%e)
                    #发生错误是回滚
                    conn.rollback()
            else:
                try:
                    cursor.execute(write_query)
                    conn.commit()
                    print('update data successful')
                except Exception as e:
                    print("the failure of updating data is ：case%s"%e)
                    #发生错误是回滚
                    conn.rollback()

            write_portfolio_query = "UPDATE WEB_DYNAMICPNL SET risk_size_Pnl = '" + str(float(PortfolioPnL['risk_size'])) + \
                                    "' , risk_beta_Pnl = '" + str(float(PortfolioPnL['risk_beta'])) + \
                                    "' , risk_sizenl_Pnl = '" + str(float(PortfolioPnL['risk_sizenl'])) + \
                                    "' , risk_vol_Pnl = '" + str(float(PortfolioPnL['risk_vol'])) + \
                                    "' , risk_mom_Pnl = '" + str(float(PortfolioPnL['risk_mom'])) + \
                                    "' , risk_BookToPrice_Pnl = '" + str(float(PortfolioPnL['risk_BookToPrice'])) + \
                                    "' , risk_liquidity_Pnl = '" + str(float(PortfolioPnL['risk_liquidity'])) + \
                                    "' , risk_growth_Pnl = '" + str(float(PortfolioPnL['risk_growth'])) + \
                                    "' , risk_leverage_Pnl = '" + str(float(PortfolioPnL['risk_leverage'])) + \
                                    "' , risk_earningsYield_Pnl = '" + str(float(PortfolioPnL['risk_EarningsYield'])) + \
                                    "' , RMSE = '" + str(RMSE) + \
                                    "' , Correlation = '" + str(corr) + \
                                    "' WHERE TradeDay = '" + str(today) + \
                                    "' AND Type = 'QH_Portfolio'"
            
            
            portfolio_cursor = conn.cursor()

            check_portfolio_query = "SELECT * FROM WEB_DYNAMICPNL WHERE Type = 'QH_Portfolio' AND TradeDay = '%s'"%today
            check_portfolio_df = pd.read_sql_query(check_portfolio_query, conn)
            if len(check_portfolio_df) == 0:
                build_portfolio_query = "INSERT INTO `WIND`.`WEB_DYNAMICPNL`(`Type`, `TradeDay`) VALUES ('QH_Portfolio', '%s')"%today
                portfolio_cursor = conn.cursor()
                portfolio_cursor.execute(build_portfolio_query)
                conn.commit()
                try:
                    portfolio_cursor.execute(write_portfolio_query)
                    conn.commit()
                    print('update data successful')
                except Exception as e:
                    print("the failure of updating data is ：case%s"%e)
                    #发生错误是回滚
                    conn.rollback()
            else:
                try:
                    portfolio_cursor.execute(write_portfolio_query)
                    conn.commit()
                    print('update data successful')
                except Exception as e:
                    print("the failure of updating data is ：case%s"%e)
                    #发生错误是回滚
                    conn.rollback()
            

    
        print('Now is out of trade time range')

    else:
        print('The File Name is wrong, Plz check file name')
