import os
import pathlib
import pickle
import gzip
import numpy as np
import pandas as pd
import scipy.io as scio

def load_hx_feature(path):
    if path.endswith(".pkl"):
        return loadpkl2df_factor(path)
    elif path.endswith(".mat"):
        return loadmat2df_factor(path)
    elif path.endswith(".ftr"):
        return loadftr2df_factor(path)
    elif path.endswith(".parquet"):
        return loadparquet2df_factor(path)
    elif path.endswith(".csv"):
        try:
            return loadcsv2df_factor(path)
        except:
            return loadcsv2df_factor2(path)


def save_obj_pkl(obj, path):
    with open(path, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj_pkl(path,to_datetime=False):
    with open(path, 'rb') as f:
        res = pickle.load(f)
    if to_datetime:
        if isinstance(res, pd.DataFrame):
            if not res.empty:
                res = dfindex_dateint2time(res)
        if isinstance(res, dict):
            for k,v in res.items():
                res[k] = dfindex_dateint2time(v)
    return res

def dfindex_dateint2time(df):
    if isinstance(df.index,pd.Int64Index):
        df.index = pd.to_datetime([str(x) for x in df.index])
    elif isinstance(df.index,pd.DatetimeIndex):
        pass
    else:
        raise TypeError("plz check the type of df.index")
    return df

def loadmat2df_interday(path):
    temp = scio.loadmat(path)
    this = pd.DataFrame(temp['SCORE'],
                        index=temp['TRADE_DT'].squeeze().tolist(),
                        columns=[x[0] for x in temp['STOCK_CODE'].squeeze()])
    # this_dtype = this.dtypes
    # this_dtype[this_dtype=='float64'] = 'float32'
    # this = this.astype(this_dtype)
    return this

def loadmat2df_factor(path):
    temp = scio.loadmat(path)
    if 'FACTOR_SCORE' in temp:
        factor_score = 'FACTOR_SCORE'
    else:
        factor_score = 'SCORE'
    if 'WIND_CODE' in temp:
        stock_code = 'WIND_CODE'
    else:
        stock_code = 'STOCK_CODE'
    factor = pd.DataFrame(temp[factor_score],
                        index=temp['TRADE_DT'].squeeze().tolist(),
                        columns=[x[0] for x in temp[stock_code].squeeze()])
    return factor

def loadpkl2df_factor(path):
    try:
        return pickle.load(open(path,'rb'))
    except:
        return pd.read_pickle(path,compression='gzip')
    # except:
    #     return pd.read_pickle(path)

def loadftr2df_factor(path):
    return pd.read_feather(path)

def loadparquet2df_factor(path):
    return pd.read_parquet(path)

def loadcsv2df_factor(path):
    # for qta csv loading~
    data = pd.read_csv(path,index_col=0,header=0)
    try:
        data.index = data.index.astype("int")
    except:
        data.index = [int(x.replace("-","")) for x in data.index]
    return data
def loadcsv2df_factor2(path):
    # for qta csv loading~
    data = pd.read_csv(path,index_col=0,header=None)
    try:
        data.index = data.index.astype("int")
    except:
        data.index = [int(x.replace("-","")) for x in data.index]
    return data

def style_analysis_metric(exposure):
    mean_ = np.nanmean(exposure)
    return np.nansum(abs(exposure-mean_))

def save_this(this,save_dir,this_name):
    pathlib.Path(save_dir).mkdir(parents=True,exist_ok=True)
    save_path = os.path.join(save_dir,this_name+".pkl")
    if os.path.exists(save_path):
        that = load_obj_pkl(save_path)
        if isinstance(this,pd.DataFrame):
            that_append = that.append(this)
            this = that_append[~that_append.index.duplicated(keep='last')]
            this = this.sort_index(ascending=True)
        elif isinstance(this,dict):
            for k in this.keys():
                temp_this = this[k]
                temp_that = that[k]
                temp_that_append = temp_that.append(temp_this)
                temp_this = temp_that_append[~temp_that_append.index.duplicated(keep='last')]
                temp_this = temp_this.sort_index(ascending=True)
                this[k] = temp_this
    save_obj_pkl(this,save_path)


def load_stock_code_trade_date(ref_path,start_date,end_date):
    '''
    选择一个参考的数据进行读取，拿到他的STOCK_CODE，TRADE_DT
    只提取在[start_date,end_date]有数据的stock_code,trade_date
    '''
    temp = loadmat2df_interday(ref_path).loc[start_date:end_date]
    temp = temp.dropna(axis=1,how='all')
    stock_code_cols = temp.columns
    trade_date_list = temp.index
    return stock_code_cols,trade_date_list

def load_stock_code_cols_from_csv(STOCK_CODE_path):
    '''
    直接从STOCK_CODE.csv文件里面把STOCK_CODE读出来
    '''
    STOCK_CODE = pd.read_csv(STOCK_CODE_path,header=None).set_axis(['code'], axis=1)
    STOCK_CODE = STOCK_CODE.sort_values(by='code').reset_index(drop=True)
    stock_code_cols = STOCK_CODE.values.squeeze().tolist()
    return stock_code_cols



def load_stock_pool(STOCK_POOL_path,start_date,end_date):
    temp = load_obj_pkl(STOCK_POOL_path)
    stk_pool = temp[(temp.date>=start_date)&(temp.date<=end_date)]
    return stk_pool


def load_res(res_save_dir,res_file_name,to_datetime=False):
    res = load_obj_pkl(os.path.join(res_save_dir,res_file_name+".pkl"),to_datetime=to_datetime)
    return res



def get_feature_name_list(feature_dir,res_save_dir):
    with open(os.path.join(res_save_dir,"feature_names.txt"),"w") as f:
        for file_name in os.listdir(feature_dir):
            f_n = file_name[:-4]
            f.write(f_n+'\n')