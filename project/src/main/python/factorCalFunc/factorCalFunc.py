from random import sample
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os

ROOT = "D:/02PHBS_G2/PHBS_m2/bigDataAnalysis/big-data-hft/project"
os.chdir(ROOT)



# 参考下面的写法写计算factor的函数
# 输入就是sample_data的样子，输出就是一个数字
# 下面这个函数就是计算这个股票当天分钟return的std
# 最后给我一个py文件 里面都是这种的函数就行


# by 汪子杰
def cal_Rvar(sample_data): 
    factor = np.sum(sample_data['Return']**2)
    return factor

def cal_Skew(sample_data):
    factor = np.sqrt(len(sample_data))*np.sum(sample_data['Return']**3)/cal_Rvar(sample_data)**(3/2)
    return factor

def cal_Kurtosis(sample_data):
    factor = np.sqrt(len(sample_data))*np.sum(sample_data['Return']**4)/cal_Rvar(sample_data)**(2)
    return factor

def cal_DownwardVol(sample_data): #Calculate the downward return's volatility
    df = sample_data.copy()
    df['Sign'] = df['Return']<=0
    df['Sign'] = df['Sign'].replace(True,1)
    factor = np.sum((df['Return']**2)*df['Sign'])/np.sum(df['Return']**2)
    return factor

def cal_ClosePriceRatio(sample_data): #Weight the close price according to the volume
    factor = np.sum(sample_data['Amount']*sample_data['Close'])/(np.sum(sample_data['Amount'])*np.mean(sample_data['Close']))

    return factor

def cal_TrendStrength(sample_data): #Calculate the price trend 
    factor = (sample_data['Close'].iloc[-1]-sample_data['Close'].iloc[0])/np.sum(abs(sample_data['Close'].diff()))

    return factor









# by 国欣然 
# debug by 叶梦婕
def AmtPerTrd(data):
    factor = (data['Amount'].sum())/(data['Qty'].sum())
    return factor


def AmtperTrd_inFlow(data):
    factor = (data.loc[data['Return'] > 0, 'Amount'].sum())/(data.loc[data['Return'] > 0, 'Qty'].sum())
    return factor


def AmtperTrd_outFlow(data):
    factor = (data.loc[data['Return'] < 0, 'Amount'].sum())/(data.loc[data['Return'] < 0, 'Qty'].sum())
    return factor


def ApT_netInFlow_ratio(data):
    ApT_inFlow_ratio = AmtperTrd_inFlow(data)/AmtPerTrd(data)
    ApT_outFlow_ratio = AmtperTrd_outFlow(data)/AmtPerTrd(data)
    factor = ApT_inFlow_ratio/ApT_outFlow_ratio
    return factor


# 选取0.2作为分位数
def Amt_netInFlow_bigOrder(data):
    amount_quantile = data.quantile(.2)['Amount']
    f1 = data.loc[data['Return'] > 0]
    f1 = f1.loc[f1['Amount'] > amount_quantile,'Amount'].sum()
    f2 = data.loc[data['Return'] < 0]
    f2 = f2.loc[f2['Amount'] > amount_quantile,'Amount'].sum()
    factor = f1-f2
    return factor


def Amt_netInFlow_bigOrder_ratio(data):
    factor = Amt_netInFlow_bigOrder(data)/data['Amount'].sum()
    return factor


def Mom_bigOrder(data):
    amount_quantile = data.quantile(.2)['Amount']
    factor = (1 + data.loc[data['Amount'] > amount_quantile,'Return']).cumprod()
    factor = list(factor)[-1]
    return factor


def UID(data):
    factor = data['Amount'].std()/data['Amount'].mean()
    return factor


def PV_corr1(data):
    column_1 = data["Close"]
    column_2 = data["Amount"]
    factor = column_1. corr(column_2)
    return factor


def PV_corr2(data):
    new_data = data.shift()
    column_1 = data["Close"]
    column_2 = new_data["Amount"]
    factor = column_1. corr(column_2)
    return factor


def dp_P_Corr(data):
    column_1 = data['Close'].diff()
    column_2 = data['Close'].shift(-1)
    factor = column_1.corr(column_2)
    return factor


def dppo_P_Corr(data):
    index = []
    column_1 = data['Close'].diff()
    column_2 = [i for i in column_1 if i >0]
    for i,j in enumerate(column_1):
        if j >0:
            index.append(i)
    index = (np.array(index)-1).tolist()
    column_3 = data['Close'].iloc[index].tolist()
    factor = pd.Series(column_2).corr(pd.Series(column_3))
    return factor


def dp_dp_corr(data):
    column_1 = data['Close'].diff()
    column_2 = column_1.shift()
    factor = column_1.corr(column_2)
    return factor


def dppo_dppo_corr(data):
    column_1 = data['Close'].diff()
    column_2 = [i for i in column_1 if i > 0]
    column_3 = pd.Series(column_2).shift()
    factor = pd.Series(column_2).corr(column_3)
    return factor


def dpne_dpne_corr(data):
    column_1 = data['Close'].diff()
    column_2 = [i for i in column_1 if i < 0]
    column_3 = pd.Series(column_2).shift()
    factor = pd.Series(column_2).corr(column_3)
    return factor


def dpne_dppo_corr(data):
    column_1 = data['Close'].diff()
    column_2 = [i for i in column_1 if i < 0]
    column_3 = [i for i in column_1 if i > 0]
    column_4 = pd.Series(column_2).shift()
    factor = pd.Series(column_3).corr(column_4)
    return factor


def dppo_dpne_corr(data):
    column_1 = data['Close'].diff()
    column_2 = [i for i in column_1 if i < 0]
    column_3 = [i for i in column_1 if i > 0]
    column_4 = pd.Series(column_3).shift()
    factor = pd.Series(column_2).corr(column_4)
    return factor

if __name__ == '__main__':
    sample_data = pd.read_csv(r"src\main\python\factorCalFunc\mins_data_1ticker.csv",index_col=0)
    print(cal_Rvar(sample_data))
    print(cal_Skew(sample_data))
    print(cal_ClosePriceRatio(sample_data))
    print(cal_DownwardVol(sample_data))
    print(cal_Kurtosis(sample_data))
    print(cal_TrendStrength(sample_data))
    print(AmtPerTrd(sample_data))
    print(AmtperTrd_inFlow(sample_data))
    print(AmtperTrd_outFlow(sample_data))
    print(ApT_netInFlow_ratio(sample_data))
    print(Amt_netInFlow_bigOrder(sample_data))
    print(Amt_netInFlow_bigOrder_ratio(sample_data))
    print(Mom_bigOrder(sample_data))
    print(UID(sample_data))
    print(PV_corr1(sample_data))
    print(PV_corr2(sample_data))
    print(dp_P_Corr(sample_data))
    print(dppo_P_Corr(sample_data))
    print(dp_dp_corr(sample_data))
    print(dppo_dppo_corr(sample_data))
    print(dpne_dpne_corr(sample_data))
    print(dpne_dppo_corr(sample_data))
    print(dppo_dpne_corr(sample_data))