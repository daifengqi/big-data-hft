#%%
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
from factorCalFunc import *
from tqdm import tqdm

ROOT = "D:/02PHBS_G2/PHBS_m2/bigDataAnalysis/big-data-hft/project"
os.chdir(ROOT)

cal_func_names = [
    'cal_Skew',
    'cal_Rvar',
    'cal_Kurtosis',
    'cal_DownwardVol',
    'cal_ClosePriceRatio',
    'cal_TrendStrength',
    'AmtPerTrd',
    'AmtperTrd_inFlow',
    'AmtperTrd_outFlow',
    'ApT_netInFlow_ratio',
    'Amt_netInFlow_bigOrder',
    'Amt_netInFlow_bigOrder_ratio',
    'Mom_bigOrder',
    'UID',
    'PV_corr1',
    'PV_corr2',
    'dp_P_Corr',
    'dppo_P_Corr',
    'dp_dp_corr',
    'dppo_dppo_corr',
    'dpne_dpne_corr',
    'dpne_dppo_corr',
    'dppo_dpne_corr',
]

all_date_csv = os.listdir("data/min_data_cleaned")


#%%
FACTOR = {}
for cal_func in tqdm(cal_func_names):
    factor_path = os.path.join(r"data/factor", cal_func.replace("cal_","")+".pkl")
    factor_list = []
    for date_csv in all_date_csv:
        data = pd.read_csv(os.path.join("data/min_data_cleaned", date_csv), index_col=[0,1])
        fac = eval(f"data.groupby(level=0).apply({cal_func})")
        fac.index = [str(x)+".SH" for x in fac.index]
        fac.name = int(date_csv.split(".")[0])
        factor_list.append(fac)
    factor_df = pd.concat(factor_list,axis=1)
    print(cal_func)
    print(factor_df)
    factor_df.to_pickle(factor_path)

DF = []
for cal_func in tqdm(cal_func_names):
    fac_name = cal_func.replace("cal_","")
    factor_path = os.path.join(r"data/factor", fac_name+".pkl")
    factor = pd.read_pickle(factor_path)
    DF.append(factor.T.stack().rename(fac_name))
FACTOR_df = pd.concat(DF,axis=1)
FACTOR_df.to_pickle(os.path.join(r"data/factor","FACTOR_df.pkl"))


for cal_func in tqdm(cal_func_names):
    factor_path = os.path.join(r"data/factor", cal_func.replace("cal_","")+".pkl")
    fac = pd.read_pickle(factor_path).T
    assert fac.index[0] == 20190107, cal_func.replace("cal_","")
    fac.to_pickle(factor_path)
