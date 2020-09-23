import rqdatac as rq
from rqdatac import *
import pandas as pd
import numpy as np
from datetime import datetime , timedelta
from datetime import date, time
import pymysql
import logging
import re
import os


rq.init()


vars_name={
 'S_INFO_WINDCODE':'sid',
 'TRADE_DT':'DataDate',
 'S_DQ_PRECLOSE':'pre_close',
 'S_DQ_OPEN':'open',
 'S_DQ_HIGH':'high',
 'S_DQ_LOW':'low',
 'S_DQ_CLOSE':'close',
 'S_PCTCHANGE':'ret',
 'S_VOLUME':'volume',
 'S_AMOUNT':'amount',
 'S_DQ_ADJPRECLOSE':'adj_pre_close',
 'S_DQ_ADJOPEN':'adj_open',
 'S_DQ_ADJHIGH':'adj_high',
 'S_DQ_ADJLOW':'adj_low',
 'S_DQ_ADJCLOSE':'adj_close',
 'S_DQ_ADJFACTOR':'adj_factor',
 'S_DQ_AVGPRICE':'vwap',
 
 
 'S_VAL_MV':'mktcap',
 'S_DQ_MV':'fltcap',
 'S_VAL_PE':'pe',
 'S_VAL_PB_NEW':'pb',
 'S_VAL_PE_TTM':'pe_ttm',
 'S_VAL_PCF_OCF':'pcf_ocf',
 'S_VAL_PCF_OCFTTM':'pcf_ocf_ttm',
 'S_VAL_PCF_NCF':'pct_ncf',
 'S_VAL_PCF_NCFTTM':'pcf_ncf_ttm',
 'S_VAL_PS':'ps',
 'S_VAL_PS_TTM':'ps_ttm',
 'S_FREETURNOVER':'turnover',
 'S_PRICE_DIV_DPS':'dps',
 'NET_PROFIT_PARENT_COMP_TTM':'profit_ttm',
 'NET_PROFIT_PARENT_COMP_LYR':'profit_lyr',
 'NET_CASH_FLOWS_OPER_ACT_TTM':'cashflow_ttm',
 'NET_CASH_FLOWS_OPER_ACT_LYR':'cashflow_lyr',
 'OPER_REV_TTM':'rev_ttm',
 'OPER_REV_LYR':'rev_lyr',
 'NET_INCR_CASH_CASH_EQU_TTM':'incr_cash_ttm',
 'NET_INCR_CASH_CASH_EQU_LYR':'incr_cash_lyr',
 'UP_DOWN_LIMIT_STATUS':'LIMIT',
 
 
 'S_LI_INITIATIVEBUYRATE':'init_buy_r',
 'S_LI_INITIATIVESELLRATE':'init_sell_r',
 'S_LI_LARGEBUYRATE':'large_buy_r',
 'S_LI_LARGESELLRATE':'large_sell_r',
 
 
 'NET_INFLOW_RATE_VOLUME':'net_inflow_r',
 'CLOSE_NET_INFLOW_RATE_VOLUME':'net_close_inflow_r',
 'OPEN_NET_INFLOW_RATE_VOLUME_L':'net_open_infow_rl',
 'CLOSE_MONEYFLOW_PCT_VOLUME_L':'close_mf_pl',
 'OPEN_MONEYFLOW_PCT_VOLUME':'open_mf_p',
 
 
 
 'CITICS_IND_CODE':'INDUSTRY',
 'ST_FLAG':'ST',
 'SH50_I_WEIGHT':'w_sh50',
 'SH50_S_DQ_CLOSE':'sh50_close',
 'SH50_S_PCTCHANGE':'sh50_ret',
 'HS300_I_WEIGHT':'w_hs300',
 'HS300_S_DQ_CLOSE':'hs300_close',
 'HS300_S_PCTCHANGE':'hs300_ret',
 'CS500_I_WEIGHT':'w_cs500',
 'CS500_S_DQ_CLOSE':'cs500_close',
 'CS500_S_PCTCHANGE':'cs500_ret',
 'CS1000_I_WEIGHT':'w_cs1000',
 'CS1000_S_DQ_CLOSE':'cs1000_close',
 'CS1000_S_PCTCHANGE':'cs1000_ret',

 'MKT_I_WEIGHT':'w_mkt',
 'MKT_S_DQ_CLOSE':'mkt_close',
 'MKT_S_PCTCHANGE':'mkt_ret',
 'MKT_S_AMOUNT':'mkt_amount',
}

Config = {
    #读取文件夹路径，文件夹中需存储csv文件
    #如文件夹路径下还有文件夹，会读取嵌套文件夹里的文件致使数据错误
    'ReadPath' : '',
    #写文件存储的文件夹路径
    'WritePath' : '',
    #ban list中市值小于tar_v的公司，ban list筛选标准
    'tar_v': 350000,
    #指数rebalance日期，需在当日使用新的指数权重
    'RebalanceDate': 20200615,
    #取距今时间长度为SuperTableLength的Super Table
    'SuperTableLength' : 360,
    #筛选掉至今距离上市不到ListDateLength的公司，ban list筛选标准
    'ListDateLength' : 90,
    #基准指数
    'Index' : '000905.SH',
    #模式分为：backtest ; simulation
    'mode': 'backtest'
}

pct_list=['turnover','ret','sh50_ret','hs300_ret','cs500_ret','cs1000_ret','mkt_ret']

risk_name = ['sid', 'alpha', 'risk_size', 'risk_beta', 'risk_sizenl', 'risk_vol', 'risk_mom', 'risk_booktoprice', 
            'risk_comove', 'risk_liquidity', 'risk_growth', 'risk_leverage', 'risk_earningsyield', 'INDUSTRY', 
            'UNIVERSE', 'bm_w', 'banned']


#数据库信息
DB_INFO=\
    dict()         
conn=\
    pymysql.connect(** DB_INFO,charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)


def query_super_table(date):
    s=list(vars_name.keys())
    sql=f"SELECT {','.join(s)} FROM ASHARESUPERTABLE WHERE TRADE_DT>='%s'"%date
    data_df=pd.read_sql_query(sql, conn).rename(columns=vars_name)
    data_df['DataDate']=data_df['DataDate'].apply(int)
    data_df[pct_list]=data_df[pct_list]/100
    data_df.set_index(['sid','DataDate'],inplace=True)
    data_df.sort_index(level=['sid','DataDate'],ascending=True,inplace=True)
    return data_df

def query_industry(date):
    industry_sql = "SELECT S_INFO_WINDCODE, CITICS_IND_CODE" + \
                    " FROM ASHARESUPERTABLE" + \
                    " WHERE TRADE_DT = '" + str(date) + \
                    "' ORDER BY S_INFO_WINDCODE"
    INDUSTRY = pd.read_sql_query(industry_sql, conn)
    return INDUSTRY


def gen_universe(data_df,tail_q=.25,):
    ''' 
    after rename processing and tradable 
    bank,sh50,hs300,cs500,small,tail,new,subnew,ST,tech
    1   ,2   ,3    ,4    ,5    ,6   ,7  ,8     ,9 ,10
    '''
    _idx=data_df.index.names
    _df=data_df.sort_index(level=_idx,ascending=True)
    _trade_bars=_df.groupby(by=['sid'])['amount'].apply(lambda x:(x>0).cumsum()) 
    
    UNIVERSE=pd.Series(5,index=_df.index,name='UNIVERSE')

    con_tail=_df.groupby(by=['DataDate'])['fltcap'].apply(lambda x: x<x.quantile(tail_q))
    UNIVERSE.loc[con_tail]=6   
        
    UNIVERSE.loc[(_trade_bars<=60)]=7
    UNIVERSE.loc[(_trade_bars> 60)&(_trade_bars<=150)] = 8
    
    UNIVERSE.loc[(_df['w_cs500']>0)]=4
    UNIVERSE.loc[(_df['w_hs300']>0)]=3
    UNIVERSE.loc[(_df['w_sh50' ]>0)]=2   

    INDUSTRY=_df['INDUSTRY'].apply(int)
    UNIVERSE.loc[INDUSTRY==23]=1 
    
    UNIVERSE.loc[_df['ST']==1]=9 
    
    row=[num for num,i in enumerate(_df.index.get_level_values('sid')) if i[:3]=='688']
    UNIVERSE.iloc[row]=10
    
    return UNIVERSE

def gen_weight(index, date):
    weight_sql = "SELECT S_CON_WINDCODE, I_WEIGHT/100" + \
                " FROM AINDEXHS300FREEWEIGHT" + \
                " WHERE S_INFO_WINDCODE = '" + str(index) + \
                "' AND TRADE_DT = '" + str(date) + \
                "' ORDER BY S_INFO_WINDCODE"
    WEIGHT = pd.read_sql_query(weight_sql, conn)
    return WEIGHT


def gen_banlist(date,listdate,TargetVal):
    banlist_sql = "SELECT S_INFO_WINDCODE FROM ASHARESUPERTABLE WHERE ST_FLAG = 1 AND TRADE_DT = '" + str(date) + \
                "' UNION" + \
                " SELECT S_INFO_WINDCODE FROM ASHARESUPERTABLE WHERE SUSPEND_FLAG = 1 AND TRADE_DT ='" + str(date) + \
                "' UNION" + \
                " SELECT S_INFO_WINDCODE FROM ASHARESUPERTABLE WHERE S_VAL_MV < " + str(TargetVal) + " AND TRADE_DT = '" + str(date) + \
                "' UNION" + \
                " SELECT S_INFO_WINDCODE FROM ASHAREDESCRIPTION WHERE S_INFO_LISTDATE > '" + str(listdate) + \
                "' UNION" + \
                " SELECT S_INFO_WINDCODE FROM ASHAREEODPRICES WHERE S_INFO_WINDCODE LIKE '688%' ORDER BY S_INFO_WINDCODE"
    BAN = pd.read_sql_query(banlist_sql, conn)
    return BAN


def GetLastTradeDate(date):
    sql = "SELECT TRADE_DAYS FROM ASHARECALENDAR WHERE S_INFO_EXCHMARKET = 'SZSE' AND TRADE_DAYS <= '%s' ORDER BY TRADE_DAYS DESC LIMIT 0, 1000"%date
    td=pd.read_sql_query(sql, conn)
    if td.iloc[0]['TRADE_DAYS'] == str(date):
        return td.iloc[1]['TRADE_DAYS']

def GetSuspension(date):
    Suspension_sql = "SELECT S_INFO_WINDCODE" + \
                    " FROM ASHARETRADINGSUSPENSION" + \
                    " WHERE S_DQ_SUSPENDDATE = '" + str(date) + \
                    "' ORDER BY S_INFO_WINDCODE"
    SUSPENSION = pd.read_sql_query(Suspension_sql, conn)
    return SUSPENSION

def GetFullRisk(df, date):
    LastDate = GetLastTradeDate(date)
    List = []
    for i in range(len(df)):
        List.append(id_convert(df.loc[i, 'sid']))
    risk = get_factor_exposure(List, str(LastDate), str(LastDate),factors=None,industry_mapping=True)
    risk = risk.reset_index()
    risk.rename(columns={'order_book_id': 'sid', 'size': 'risk_size', 'beta': 'risk_beta', 'non_linear_size': 'risk_sizenl', 'residual_volatility': 'risk_vol', 'momentum': 'risk_mom', 'book_to_price': 'risk_booktoprice', 'comovement': 'risk_comove', 'liquidity': 'risk_liquidity', 'growth': 'risk_growth', 'leverage': 'risk_leverage', 'earnings_yield': 'risk_earningsyield'}, inplace=True)
    risk = risk[['sid', 'risk_size', 'risk_beta', 'risk_sizenl', 'risk_vol', 'risk_mom', 'risk_booktoprice', 'risk_comove', 'risk_liquidity', 'risk_growth', 'risk_leverage', 'risk_earningsyield']]
    for i in range(len(risk)):
        if ((risk.loc[i, 'sid'][:1] == '3') | (risk.loc[i, 'sid'][:1] == '0')):
            risk.loc[i, 'sid'] = risk.loc[i, 'sid'][0:6] + '.SZ'
        elif risk.loc[i, 'sid'][:1] == '6':
            risk.loc[i, 'sid'] = risk.loc[i, 'sid'][0:6] + '.SH'

    risk = pd.merge(risk, df, how='outer', on=['sid'])
    risk['alpha'] = risk['alpha'] / 100
    #for i in range(len(risk)):
    #    risk.loc[i, 'alpha'] = float(df.loc[df['sid'] == risk.loc[i, 'sid'], 'alpha'] / 100)
    risk = risk[['sid', 'alpha', 'risk_size', 'risk_beta', 'risk_sizenl', 'risk_vol', 'risk_mom', 'risk_booktoprice', 'risk_comove', 'risk_liquidity', 'risk_growth', 'risk_leverage', 'risk_earningsyield']]
    df_industry = query_industry(LastDate)
    df_universe = gen_universe(super_table)
    df_universe = df_universe.reset_index()
    df_universe = df_universe.loc[df_universe['DataDate'] == int(LastDate), ]
    dt = datetime.strptime(str(date), '%Y%m%d')

    
    if dt == datetime.strptime(str(Config['RebalanceDate']), '%Y%m%d'):
        df_500_weight = gen_weight(Config['Index'], Config['RebalanceDate'])
    else:
        i = 1
        while(True):
            df_500_weight = gen_weight(Config['Index'], str((datetime.strptime("%s" %str(date), '%Y%m%d') - timedelta(days = i)).strftime('%Y%m%d')))
            if len(df_500_weight) == 0:
                i = i + 1
            else:
                break
        

    ListDate = str((datetime.strptime("%s" %str(LastDate), '%Y%m%d') - timedelta(days = Config['ListDateLength'])).strftime('%Y%m%d'))
    TargetVal = Config['tar_v']
    df_banlist = gen_banlist(LastDate, ListDate, TargetVal)
    df_suspension = GetSuspension(LastDate)
    
    #risk = pd.merge(risk, df_industry, how='outer', on=['sid'])
    #risk = pd.merge(risk, df_universe, how='outer', on=['sid'])

    for i in range(len(risk)):
        risk.loc[i, 'INDUSTRY'] = int(df_industry.loc[df_industry['S_INFO_WINDCODE'] == risk.loc[i, 'sid'], 'CITICS_IND_CODE'])
        risk.loc[i, 'UNIVERSE'] = int(df_universe.loc[df_universe['sid'] == risk.loc[i, 'sid'], 'UNIVERSE'])
        
        if risk.loc[i, 'sid'] not in list(df_500_weight['S_CON_WINDCODE']):
            risk.loc[i, 'bm_w'] = 0
        else:
            risk.loc[i, 'bm_w'] = float(df_500_weight.loc[df_500_weight['S_CON_WINDCODE'] == risk.loc[i, 'sid'], 'I_WEIGHT/100'])
        if risk.loc[i, 'sid'] not in list(df_banlist['S_INFO_WINDCODE']):
            risk.loc[i, 'banned'] = 0
        else:
            risk.loc[i, 'banned'] = 1
        if risk.loc[i, 'sid'] in list(df_suspension['S_INFO_WINDCODE']):
            risk = risk.drop(i)
    
    risk = risk.reset_index()
    risk = risk[risk_name]
    risk = risk.set_index(['sid'])
    risk = risk.sort_index()
    return risk


if __name__ == '__main__':
    print('Start Reading Database')
    start = datetime.now()
    super_table = query_super_table(str((datetime.today() - timedelta(days = Config['SuperTableLength'])).strftime('%Y%m%d')))
    if Config['mode'] == 'simulation':
        print('Now mode is simulation')
        df = pd.read_csv(Config['ReadPath'] + 'alpha_500_' + str(datetime.today().strftime('%Y%m%d')) + '.csv',index_col=False, header=0)
        df.rename(columns={'Unnamed: 0': 'sid'}, inplace=True)
        date = str(datetime.today().strftime('%Y%m%d'))
        LastDate = GetLastTradeDate(date)
        
        risk = GetFullRisk(df, date)

        print("Update data using time： " + str(datetime.now() - start))
        print('Finsh ' + str(LastDate) + ', put csv file to write path')
        risk.to_csv(Config['WritePath'] + 'alpha_500_' + str(LastDate) + '.csv')

        
    elif Config['mode'] == 'backtest':
        print('Now mode is backtest')
        for root,dirs,files in os.walk(Config['ReadPath']):
            for file in files:
                df = pd.read_csv(os.path.join(root,file),index_col=False, header=0)
                df.rename(columns={'Unnamed: 0': 'sid'}, inplace=True)
                base = os.path.basename(file)
                date = base[-12:-4]
                LastDate = GetLastTradeDate(date)
                
                risk = GetFullRisk(df, date)

                print("Update data using time: " + str(datetime.now() - start))

                print('Finsh ' + str(LastDate) + ', put csv file to write path')
                risk.to_csv(Config['WritePath'] + 'alpha_500_' + str(LastDate) + '.csv')
    else:
        print('Mode is wrong, plz correct mode!')
