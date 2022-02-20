import os

# bottleneck numpy的mean都是要指定axis的
# import bottleneck as bn
import numpy as np
import pandas as pd
import statsmodels.api as sm

# from featureCalculationFunctions import statsCalculationNan as statsCalNan

def prepare_RET_dict(ADJVWAP, ADJCLOSE):
    RET_t1 = ADJVWAP.shift(-1)/ADJCLOSE-1
    RET_t2 = ADJVWAP.shift(-2)/ADJVWAP.shift(-1)-1
    RET_t5 = ADJVWAP.shift(-6)/ADJVWAP.shift(-1)-1
    RET_t10 = ADJVWAP.shift(-11)/ADJVWAP.shift(-1)-1
    RET_t15 = ADJVWAP.shift(-16)/ADJVWAP.shift(-1)-1
    RET_t20 = ADJVWAP.shift(-21)/ADJVWAP.shift(-1)-1
    RET_dict = {
        'RET_t1':RET_t1,
        'RET_t2':RET_t2,
        'RET_t5':RET_t5,
        'RET_t10':RET_t10,
        'RET_t15':RET_t15,
        'RET_t20':RET_t20
        }
    return RET_dict

time_horizon_dict = {
    'RET_t1':1,
    'RET_t2':1,
    'RET_t5':5,
    'RET_t10':10,
    'RET_t15':15,
    'RET_t20':20,
    }



# Preprocessing
def deExtreme_three_sigma(arr):
    mean_ = np.nanmean(arr)
    std_ = np.nanstd(arr)
    arr[arr>mean_+3*std_] = mean_+3*std_
    arr[arr<mean_-3*std_] = mean_-3*std_
    return arr

def neutralize_regression(arr,arr_control):
    X = sm.add_constant(arr_control)
    arr_c = arr.copy()
    try:
        res = sm.OLS(arr,X,missing='drop').fit()
        arr_c[~np.isnan(arr)] = res.resid
    except:
        pass
    return arr_c

def standardize_Zscore(arr):
    mean_ = np.nanmean(arr)
    std_ = np.nanstd(arr)
    return (arr-mean_)/std_



# IC calculation
def calculateIC_pearson(factor,ret):
    temp_array = np.vstack((factor, ret))
    temp_array[np.isinf(temp_array)] = np.nan
    temp_array = temp_array[:,~np.isnan(temp_array).any(axis=0)]
    return np.corrcoef(temp_array)[0, 1]

def calculateIC_spearman(factor,ret):
    return (pd.Series(factor).rank()).corr(pd.Series(ret).rank())
    # vstack --> (2,n)
    # temp_array = np.vstack((factor, ret))
    # temp_array[np.isinf(temp_array)] = np.nan
    # temp_array = temp_array[:,~np.isnan(temp_array).any(axis=0)]
    # # temp_rank = bn.nanrankdata(temp_array, axis=1)
    # temp_rank = temp_array.argsort().argsort()
    # #FIXME: 
    # temp_rank[temp_array==np.nan] = np.nan

    # temp_rank = temp_rank[:, ~np.isnan(temp_rank).any(axis=0)]
    # return np.corrcoef(temp_rank)[0, 1]


# get group weight
def group_weight_eqW(arr,lmt_filter_arr,group=1,group_num = 5,ff_size=None):
    '''
    按照等权的方式直接进行加权
    group:在第几个group中
    group_num:一共多少个group
    '''
    group_percent = 1/group_num
    arr_c = np.full_like(arr,np.nan)
    arr_down = np.nanquantile(arr,q=group_percent*(group-1))
    arr_up = np.nanquantile(arr,q=group_percent*group)
    arr_c[(arr>arr_down)&(arr<arr_up)] = 1
    if lmt_filter_arr is not None:
        arr_c = arr_c*lmt_filter_arr
    return arr_c/np.nansum(arr_c)


def group_weight_ffSize(arr,lmt_filter_arr,group=1,group_num = 5,ff_size=None):
    '''
    按照自由流通市值的方式直接进行加权
    group:在第几个group中
    group_num:一共多少个group
    ff_size和arr_c的shape是一样的，用点乘
    '''
    if ff_size is None:
        raise Exception("please input ff_size")
    group_percent = 1/group_num
    arr_c = np.full_like(arr,np.nan)
    arr_down = np.nanquantile(arr,q=group_percent*(group-1))
    arr_up = np.nanquantile(arr,q=group_percent*group)
    arr_c[(arr>arr_down)&(arr<arr_up)] = 1
    arr_c = arr_c*lmt_filter_arr
    if lmt_filter_arr is not None:
        arr_c = arr_c*ff_size
    return arr_c/np.nansum(arr_c)




# Style Analysis
# 目前是根据gw来计算的--20210810
# 默认用这个--20210810
def style_analysis_group_exposure_mean_gw(group_weight_oneday,factor_s):
    '''
    factor_s: 1D arr
    已经有group_weight_oneday的情况下
    '''
    exposure_list = []
    for i in range(len(group_weight_oneday)):
        exposure_list.append(np.nansum(group_weight_oneday[i]*factor_s))

    return np.array(exposure_list)

def style_analysis_group_percentile_gw(group_weight_oneday,factor_s):
    '''
    factor_s: 1D arr
    已经有group_weight_oneday的情况下
    '''
    factor_s_rank = factor_s.argsort().argsort()
    factor_s_rank[factor_s==np.nan] = np.nan
    factor_s_percentile = factor_s_rank/len(factor_s)
    exposure_list = []
    for i in range(len(group_weight_oneday)):
        exposure_list.append(np.nansum(group_weight_oneday[i]*factor_s_percentile))

    return np.array(exposure_list)

# 目前没有用这种
def style_analysis_group_percentile(factor,factor_s,group_num=5):
    '''
    factor: 1D arr
    factor_s: 1D arr
    v1：先对待测因子分组，看每组的股票在风格上的暴露的百分位数均值
    '''
    temp_array = np.vstack((factor, factor_s))
    temp_array[np.isinf(temp_array)] = np.nan
    temp_array = temp_array[:,~np.isnan(temp_array).any(axis=0)]
    factor = temp_array[0]
    factor_s = temp_array[1]
    factor_s_rank = factor_s.argsort().argsort()
    factor_s_rank[factor_s==np.nan] = np.nan
    factor_s_percentile = factor_s_rank/len(factor_s)
    quantile_list = [np.quantile(factor,i/group_num) for i in range(0,group_num+1)]
    exposure_list = [] # has #group_num entries
    for q0,q1 in zip(quantile_list[:-1],quantile_list[1:]):
        filter_ = (factor>q0)&(factor<q1)
        exposure_list.append(np.nanmean(factor_s_percentile[filter_]))

    return np.array(exposure_list)

# 目前没有用这种
def style_analysis_group_exposure_mean(factor,factor_s,group_num=5):
    '''
    factor: 1D arr
    factor_s: 1D arr
    v2：先对待测因子分组，看每组的股票在风格上的暴露的均值(直接是风格因子的值，有正有负)
    '''
    temp_array = np.vstack((factor, factor_s))
    temp_array[np.isinf(temp_array)] = np.nan
    temp_array = temp_array[:,~np.isnan(temp_array).any(axis=0)]
    factor = temp_array[0]
    factor_s = temp_array[1]
    quantile_list = [np.quantile(factor,i/group_num) for i in range(0,group_num+1)]
    exposure_list = [] # has #group_num entries
    for q0,q1 in zip(quantile_list[:-1],quantile_list[1:]):
        filter_ = (factor>q0)&(factor<q1)
        exposure_list.append(np.nanmean(factor_s[filter_]))

    return np.array(exposure_list)


'''
以下函数计算时：忽略nan

'''
class statsCalculationNan():

    @staticmethod    
    def central_moment_n(var, n=1, axis=0):
        return np.nanmean((var-np.nanmean(var, axis=axis))**n, axis=axis)


    @staticmethod
    def mean(var, axis=0):
        return np.nanmean(var, axis=axis)



    @staticmethod
    def variance(var, axis=0):
        try:
            return statsCalculationNan.central_moment_n(var, n=2)
        except:
            return np.nan

    @staticmethod
    def std(var,axis=0):
        try:
            return np.sqrt(statsCalculationNan.variance(var, axis=axis))
        except:
            return np.nan

    @staticmethod
    def var_coef(var, axis=0):
        try:
            return np.sqrt(statsCalculationNan.std(var,axis=axis))/statsCalculationNan.mean(var,axis=axis)
        except:
            return np.nan

    @staticmethod
    def skew(var, axis=0):
        try:
            return statsCalculationNan.central_moment_n(var, n=3)/statsCalculationNan.central_moment_n(var, n=2)**(1.5)
        except:
            return np.nan


    @staticmethod
    def kurtosis(var, axis=0):
        try:
            return statsCalculationNan.central_moment_n(var, n=4)/statsCalculationNan.central_moment_n(var, n=2)**2
        except:
            return np.nan


    @staticmethod
    def weighted_mean(var, wts, axis=0):
        """Calculates the weighted mean"""
        var_not_nan = ~np.isnan(var)
        try:
            return np.nansum(var*wts, axis=axis)/np.nansum(wts*var_not_nan, axis=axis)
        except:
            return np.nan


    @staticmethod
    def weighted_variance(var, wts, axis=0):
        """Calculates the weighted variance"""
        try:
            return statsCalculationNan.weighted_mean((var - statsCalculationNan.weighted_mean(var, wts, axis=axis))**2, wts, axis=axis)
        except:
            return np.nan

    @staticmethod
    def weighted_std(var, wts, axis=0):
        """Calculates the weighted std"""
        try:
            return np.sqrt(statsCalculationNan.weighted_variance(var, wts, axis=axis))
        except:
            return np.nan

    @staticmethod
    def weighted_skew(var, wts, axis=0):
        """Calculates the weighted skewness"""
        try:
            return (statsCalculationNan.weighted_mean((var - statsCalculationNan.weighted_mean(var, wts, axis=axis))**3, wts, axis=axis) /
                    statsCalculationNan.weighted_variance(var, wts, axis=axis)**(1.5))
        except:
            return np.nan


    @staticmethod
    def weighted_kurtosis(var, wts, axis=0):
        """Calculates the weighted skewness"""
        try:
            return (statsCalculationNan.weighted_mean((var - statsCalculationNan.weighted_mean(var, wts, axis=axis))**4, wts, axis=axis) /
                    statsCalculationNan.weighted_variance(var, wts, axis=axis)**(2))
        except:
            return np.nan


    @staticmethod
    def entropy(var, axis=0):
        try:
            return -np.nansum(var*np.log(var),axis=axis)
        except:
            return np.nan
        

# factor description
class FactorDescribe():
    def __init__(self,arr):
        self.factor = arr

    def get_nans_ratio(self):
        self.nans_ratio = np.sum(np.isnan(self.factor))/len(self.factor)

    def get_infs_ratio(self):
        self.infs_ratio = np.sum(np.isinf(self.factor))/len(self.factor)

    def get_zeros_ratio(self):
        self.zeros_ratio = np.sum(self.factor==0)/len(self.factor)

    def get_mean(self):
        self.factor_mean = statsCalculationNan.mean(self.factor)
    
    def get_median(self):
        self.factor_median = np.nanmedian(self.factor)

    def get_std(self):
        self.factor_std = statsCalculationNan.std(self.factor)

    def get_skew(self):
        self.factor_skew = statsCalculationNan.skew(self.factor)

    def get_kurtosis(self):
        self.factor_kurtosis = statsCalculationNan.kurtosis(self.factor)

    def run(self):
        self.get_zeros_ratio()
        self.get_infs_ratio()
        self.get_nans_ratio()
        self.get_skew()
        self.get_mean()
        self.get_median()
        self.get_std()
        self.get_kurtosis()


deExtreme_method_dict = {
    'three_sigma':deExtreme_three_sigma
}
neutralize_method_dict = {
    'regression':neutralize_regression
} 
standardize_method_dict = {
    'Zscore':standardize_Zscore
} 
calculateIC_method_dict = {
    'pearson':calculateIC_pearson,
    'spearman':calculateIC_spearman
} 
style_analysis_method_dict = {
    'percentile':style_analysis_group_percentile,
    'exposure_mean':style_analysis_group_exposure_mean,
    'exposure_mean_gw':style_analysis_group_exposure_mean_gw,
    'percentile_gw':style_analysis_group_percentile_gw
}
group_weight_method_dict = {
    'eq_weight':group_weight_eqW,
    'ff_size':group_weight_ffSize
}

BARRA_FACTOR_NAME_LIST = [
    'SIZE',
    'LIQUIDTY',
    'MOMENTUM',
    'RESVOL',
    'SIZENL',
    'SRISK',
    'BETA',
    'BTOP',
    'EARNYILD',
    'GROWTH',
    'LEVERAGE'
]

