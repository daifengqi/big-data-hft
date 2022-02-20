#%%
import os
import pandas as pd
import factorAnalysisIOTools as IOTools
from factorAnalysisCalTools import prepare_RET_dict, time_horizon_dict

#%%
def update_benchmark_return(stock_pool=None,stock_pool_path=None,stock_pool_name=None,benchmark_ret_save_dir=None):
    if stock_pool is None:
        stock_pool = pd.read_pickle(stock_pool_path)
    stock_pool_adjvwap = stock_pool.pivot(index='date',columns='code',values='ADJVWAP')
    stock_pool_adjclose = stock_pool.pivot(index='date',columns='code',values='ADJCLOSE')
    RET_dict = prepare_RET_dict(stock_pool_adjvwap, stock_pool_adjclose)
    benchmark_ret_list = []
    for RET_key, RET_tn in RET_dict.items():
        RET_tn = RET_tn/time_horizon_dict[RET_key]
        benchmark_ret_list.append(pd.DataFrame(RET_tn.mean(axis=1),columns=[RET_key]))

    IOTools.save_this(pd.concat(benchmark_ret_list,axis=1), benchmark_ret_save_dir,f'BENCHMARK_RETURN_{stock_pool_name.replace("STOCK_POOL_","")}')  
    

# %%
stock_pool_name = 'STOCK_POOL_basic'
stock_pool_path = f"E:\\data\\interday\\{stock_pool_name}.pkl"
benchmark_ret_save_dir = r"E:\data\interday"
stock_pool = pd.read_pickle(stock_pool_path)
stock_pool_adjvwap = stock_pool.pivot(index='date',columns='code',values='ADJVWAP')
stock_pool_adjclose = stock_pool.pivot(index='date',columns='code',values='ADJCLOSE')
RET_dict = prepare_RET_dict(stock_pool_adjvwap, stock_pool_adjclose)

#%%
for RET_key, RET_tn in RET_dict.items():
    RET_tn = RET_tn/time_horizon_dict[RET_key]
    RET_tn_mean = pd.DataFrame(RET_tn.mean(axis=1),columns=[RET_key+"_mean"])
    IOTools.save_this(RET_tn_mean, benchmark_ret_save_dir,f'BENCHMARK_RETURN_{stock_pool_name.replace("STOCK_POOL_","")}_{RET_tn}')  
    
