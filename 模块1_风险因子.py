# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 14:21:07 2019

@author: q02
"""
import pandas as pd
import numpy as np
import math
from matplotlib import pyplot as plt 
import os
    
#date_List = list(data_df_holdingvalue_weight.index)
#datelist_for_oralce = "('" + "','".join(date_List)) +"')"   


import sys 
sys.path.append('C:\\Users\\q02\\Desktop\\project\\python_global\\')
import my_oracle


str_startdate = '20180801' 



sqlstr_去掉新股后的月收盘价 = '''
SELECT S_INFO_WINDCODE,TRADE_DT,S_DQ_ADJCLOSE from 
AShareEODP_NoNS_M  where TRADE_DT >'%s'
'''%str_startdate
pd_去掉新股后的月收盘价 = pd.read_sql(sqlstr_去掉新股后的月收盘价,my_oracle.con_research) #这样写一般没问题,除了本例中,可能最后一天因子还没出来
pd_去掉新股后的月收益率 = pd_去掉新股后的月收盘价.groupby('S_INFO_WINDCODE').apply(lambda x :x.set_index('TRADE_DT').pct_change()).dropna().reset_index().rename(columns = {'S_DQ_ADJCLOSE':'S_MQ_PCTCHANGE'}) 


sqlstr_取风险因子值 = '''
select QIUSOCODE as S_INFO_WINDCODE,TRADEDATE as TRADE_DT, BETA,MOMENTUM,MKTSIZE,EARNYILD,RESVOL,GROWTH,BTOP,LEVERAGE,LIQUIDTY,SIZENL  from QS_DAILYRISKFACTOR t
     where TRADEDATE > '%s'
'''%str_startdate
pd_风险因子值 = pd.read_sql(sqlstr_取风险因子值,my_oracle.con_jrgc)
pd_风险因子值 = my_oracle.day_to_month_jrgc(pd_风险因子值)
pd_风险因子值 = my_oracle.lag1month(pd_风险因子值)

queryResult = pd.merge(pd_去掉新股后的月收益率,pd_风险因子值,how='inner',on=['S_INFO_WINDCODE','TRADE_DT'])

sql_取最新的中信行业 = "select a.s_info_windcode, b.INDUSTRIESNAME \
  from AShareIndustriesClassCITICS a, \
          AShareIndustriesCode  b \
 where substr(a.citics_ind_code, 1, 4) = substr(b.IndustriesCode, 1, 4) \
 and b.levelnum = '2' \
   and a.cur_sign = '1' \
 order by 1 " 
pd_indust = pd.read_sql(sql_取最新的中信行业,my_oracle.con_jrgc)

queryResult2 = pd.merge(queryResult,pd_indust,how='inner',on='S_INFO_WINDCODE')
 




'''
list_b = []
grp_分位数 = pd_rank_分位数.groupby(['TRADE_DT','INDUSTRIESNAME'])
for month_indust, item in grp_分位数:
    list_b.append([month_indust[0],month_indust[1],len(item)])
pd_b = pd.DataFrame(list_b)
'''
    
'''
pd_rank_2 = pd.DataFrame()
grp = queryResult.groupby('TRADE_DT')
for month,月度_因子和收益矩阵 in grp:
    print(month)
    print(len(月度_因子和收益矩阵))
#    for factor in 'S_MQ_PCTCHANGE,BETA,MOMENTUM,MKTSIZE,EARNYILD,RESVOL,GROWTH,BTOP,LEVERAGE,LIQUIDTY,SIZENL'.split(","):
    a = 月度_因子和收益矩阵.set_index(['S_INFO_WINDCODE','TRADE_DT']).rank()
    pd_rank_2 = pd.concat([pd_rank_2,a],axis=0)
pd_rank_2 = pd_rank_2.dropna()

for factor in 'S_MQ_PCTCHANGE,BETA,MOMENTUM,MKTSIZE,EARNYILD,RESVOL,GROWTH,BTOP,LEVERAGE,LIQUIDTY,SIZENL'.split(","):
    d = pd.DataFrame([pd_rank[factor],pd_rank_2[factor]]).T
    d.columns = ['a','b']
    d['dif'] = (d['a'] ==d['b'])
    e = d[d['dif']==False]
    print(len(e))
'''

#根据行业再做一次groupby

pd_factor_evaluate = pd.DataFrame()

pd_rank_分位数 = queryResult2.set_index(['S_INFO_WINDCODE','TRADE_DT','INDUSTRIESNAME']).groupby(['TRADE_DT','INDUSTRIESNAME']).apply(lambda x:x.rank()/len(x)-0.5).dropna()
pd_rank_分位数 = pd_rank_分位数[pd_rank_分位数['S_MQ_PCTCHANGE']>0.3].drop(columns=['S_MQ_PCTCHANGE'])
grp_1 = pd_rank_分位数.groupby('TRADE_DT')
for month,月度因子矩阵 in grp_1:
    a = 月度因子矩阵.mean(axis=0).rename((month,'全行业'))
    a['TRADE_DT'] = month
    a['INDUSTRIESNAME'] = '全行业'
    a['STOCK_NUM'] = len(月度因子矩阵)*5
    pd_factor_evaluate = pd.concat([pd_factor_evaluate,a],axis=1,sort=False)
    
    
pd_rank = queryResult2.set_index(['S_INFO_WINDCODE','TRADE_DT','INDUSTRIESNAME']).groupby(['TRADE_DT','INDUSTRIESNAME']).apply(lambda x:x.rank()).dropna()
grp = pd_rank.groupby(['TRADE_DT','INDUSTRIESNAME'])
for month_indust,月度_因子和收益矩阵 in grp:
    a = 月度_因子和收益矩阵.sort_values(by='S_MQ_PCTCHANGE',ascending=False) 
    num = int(len(月度_因子和收益矩阵)/5)
    #注意这里rank值是从小到大排列的rank，所以因子越大rank越大
    b = (a[:num].drop(columns=['S_MQ_PCTCHANGE']).mean(axis=0)/len(月度_因子和收益矩阵)-0.5).rename(month_indust) 
    b['TRADE_DT'] = month_indust[0]
    b['INDUSTRIESNAME'] = month_indust[1]
    b['STOCK_NUM'] = len(月度_因子和收益矩阵)
    pd_factor_evaluate = pd.concat([pd_factor_evaluate,b],axis=1,sort=False)
    
pd_factor_evaluate = pd_factor_evaluate.T

'''
list_a = []
#求一个行业中性的，全行业的结果：相当于在每个行业中选最靠前的20%的股票，等权放在一起，看这个组合的特征
pd_factor_evaluate_ALLINDUST = pd.DataFrame()
for month_indust,月度_因子和收益矩阵 in grp:
    num = int(len(月度_因子和收益矩阵)/5)
    list_a.append([month_indust[0],month_indust[1],len(月度_因子和收益矩阵),num])   
    a = 月度_因子和收益矩阵.sort_values(by='S_MQ_PCTCHANGE',ascending=False)[:num].drop(columns=['S_MQ_PCTCHANGE'])
    pd_factor_evaluate_ALLINDUST = pd.concat([pd_factor_evaluate_ALLINDUST,a],axis=0,sort=False)
'''


my_oracle.con_sqlite.cursor().execute('delete from polls_riskfactor_evaluate')
my_oracle.con_sqlite.commit()

from pandas.io import sql
sql.to_sql(pd_factor_evaluate,
            name='polls_riskfactor_evaluate',
            con=my_oracle.con_sqlite,
            index=False,
            if_exists='append')



'''
plt.rcParams['font.sans-serif'] = ['SimHei'] # 步骤一（替换sans-serif字体）
plt.rcParams['axes.unicode_minus'] = False  # 步骤二（解决坐标轴负数的负号显示问题）
#看某一个行业的范式
pd_factor_evaluate[pd_factor_evaluate['INDUSTRIESNAME']=='银行'].set_index('TRADE_DT').plot()

#看某一截面上某一因子，各个行业的驱动因子情况的范式
pd_factor_evaluate[pd_factor_evaluate['TRADE_DT']=='20190531'][['INDUSTRIESNAME','BETA']].set_index('INDUSTRIESNAME').sort_values(by='BETA').plot(kind='bar')


#只看显著的那些因子
temp = pd_factor_evaluate.set_index(['INDUSTRIESNAME','TRADE_DT']).applymap(lambda x:x if (abs(x)>0.1) else np.nan).dropna(how='all')




#取数实例1
sql_str = 'select * from polls_question'
queryResult = pd.read_sql(sql_str,my_oracle.con_sqlite)

#取数实例2
cursor  = my_oracle.con_sqlite.cursor().execute('select * from polls_question')
for row in cursor:
   print (row[0])
'''