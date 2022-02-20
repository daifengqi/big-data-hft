# 根据plot的结果人工筛选出可行性高的因子
# 筛选主要根据
# 1，单调性，ICIR的情况，分组return的情况
# 2. 因子分布，尽量正态，coverage高

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
from factorCalFunc import *
from tqdm import tqdm

fac_names = [
    'DownwardVol',
    'TrendStrength',
    'dp_P_Corr',
    'PV_corr1',
    'PV_corr2',
    'AmtPerTrd',
    'AmtperTrd_outFlow',
    'AmtperTrd_inFlow',
    'dppo_dppo_corr',
    'ClosePriceRatio',
    'Kurtosis',
    'dp_dp_corr',
    'Skew',
]
fac_directions = [1] + [-1]*(len(fac_names)-1)

fac_path = r"D:\02PHBS_G2\PHBS_m2\bigDataAnalysis\big-data-hft\project\data\factor"
final_df = None

def zscore(df):
    mu = df.mean(axis=1)
    std = df.std(axis=1)
    return (df.sub(mu,axis=0)).divide(std,axis=0)

def set_group_oneday(pred_label_oneday, group_num = 10, reverse = False):
    """set group id of each stock_id

    Args:
        pred_label_oneday (pd.Series): [description]
        group_num (int, optional): [description]. Defaults to 10.
        reverse (bool, optional): keep group_1 as short group, group_i+1 > group_i. Defaults to False.
    """
    if reverse:
        pred_label_oneday["signal"] *= -1
    pred_label_oneday = pred_label_oneday.sort_values(by="signal")
    pred_label_oneday['group'] = np.arange(len(pred_label_oneday))//((len(pred_label_oneday)//group_num)+1) + 1 
    pred_label_oneday.index = pred_label_oneday.index.droplevel(0)
    pred_label_oneday['weight'] = 0
    pred_label_oneday.loc[pred_label_oneday['group']==10,'weight'] = (pred_label_oneday['group']==10).mean()
    return(pred_label_oneday)

for i in range(len(fac_names)):
    fac_name = fac_names[i]
    fac_direction = fac_directions[i]
    if final_df is None:
        final_df = zscore(pd.read_pickle(os.path.join(fac_path,fac_name+".pkl")))*fac_direction
    else:
        final_df += zscore(pd.read_pickle(os.path.join(fac_path,fac_name+".pkl")))*fac_direction

final_df.to_pickle(os.path.join(fac_path,"final_signal.pkl"))

signal = pd.DataFrame(final_df.stack(),columns=['signal'])
signal_res = signal.groupby(level=0).apply(set_group_oneday)
signal_res.to_csv(r"D:\02PHBS_G2\PHBS_m2\bigDataAnalysis\big-data-hft\project\res\signal\signal_20190107_20190226.csv")


