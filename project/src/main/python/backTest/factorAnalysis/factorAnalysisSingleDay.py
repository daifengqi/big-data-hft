
import numpy as np
import pandas as pd
import statsmodels.api as sm
from copy import deepcopy
from .factorAnalysisCalTools import (calculateIC_method_dict,
                                    deExtreme_method_dict,
                                    group_weight_method_dict,
                                    neutralize_method_dict,
                                    standardize_method_dict,
                                    style_analysis_method_dict)
import warnings
warnings.filterwarnings("ignore")

class FactorAnalysisSingleDay():
    def __init__(self, factor, lmt_filter_arr=None, ret=None):
        self.factor = factor.values.squeeze().copy()
        self.sec_code = list(factor.index)
        self.lmt_filter_arr = lmt_filter_arr
        # self.date = date

    def deExtreme(self, method='three_sigma'):
        deExtreme_f = deExtreme_method_dict[method]
        self.factor = deExtreme_f(self.factor)

    def neutralize(self, factor_control, method='regression'):
        neutralize_f = neutralize_method_dict[method]
        factor_c = factor_control.loc[self.sec_code].values
        self.factor = neutralize_f(self.factor, factor_c)

    def standardize(self, method='Zscore'):
        standardize_f = standardize_method_dict[method]
        self.factor = standardize_f(self.factor)

    def fill_nan_inf(self,fill_value=0):
        self.factor[np.isnan(self.factor)] = fill_value
        self.factor[np.isinf(self.factor)] = fill_value
        
    def get_factor_rank(self):
        factor = self.factor.copy()
        self.factor_rank = np.argsort(np.argsort(factor))
        return self.factor_rank


    def calculate_IC(self, ret, method='pearson',filter_arr=None):
        calculateIC_f = calculateIC_method_dict[method]
        factor = self.factor.copy()
        if filter_arr is not None:
            factor = factor*filter_arr
        return calculateIC_f(factor, ret)

    def calculate_group_IC_rank(self, ret_rank, group_weight_oneday=None, factor_rank=None, method='pearson', filter_arr=None):
        calculateIC_f = calculateIC_method_dict[method]
        group_IC_oneday = []
        if factor_rank is None:
            factor_rank = self.factor_rank
        if group_weight_oneday is None:
            group_weight_oneday = self.group_backtest_weight(group_num=group_num)
        factor_rank_c = deepcopy(factor_rank)
        group_weight_oneday_c = deepcopy(group_weight_oneday)
        ret_rank_c = deepcopy(ret_rank)
        if filter_arr is not None:
            factor_rank_c = factor_rank_c*filter_arr
        group_weight_oneday_c = deepcopy(group_weight_oneday)
        for i in range(len(group_weight_oneday_c)):
            group_weight_filter = group_weight_oneday_c[i]
            group_weight_filter[np.isfinite(group_weight_filter)]=1
            factor_rank_i = factor_rank_c*group_weight_filter
            # assert np.nansum(np.isfinite(factor_i)) > 200, f'check num of finite factor in group{i}'
            group_IC_oneday.append(calculateIC_f(factor_rank_i, ret_rank_c))
        return group_IC_oneday


    def calculate_group_IC(self, ret, group_weight_oneday=None, method='pearson', filter_arr=None):
        ret = np.array(ret)
        group_IC_oneday = []
        calculateIC_f = calculateIC_method_dict[method]
        factor_c = self.factor.copy()
        if group_weight_oneday is None:
            group_weight_oneday = self.group_backtest_weight(group_num=group_num)
        if filter_arr is not None:
            factor_c = factor_c*filter_arr
        group_weight_oneday_c = deepcopy(group_weight_oneday)
        for i in range(len(group_weight_oneday_c)):
            group_weight_filter = group_weight_oneday_c[i]
            group_weight_filter[np.isfinite(group_weight_filter)]=1
            factor_i = factor_c*group_weight_filter
            # assert np.nansum(np.isfinite(factor_i)) > 200, f'check num of finite factor in group{i}'
            group_IC_oneday.append(calculateIC_f(factor_i, ret))
        return group_IC_oneday


    # 把计算每组的weight和计算每组的return分开
    # 在计算weight的时候，可以选择method
    '''log--20210810'''
    # 'eq_weight'：直接等权,输入的params不包括ff_size
    # TODO:‘ff_size'：根据自由流通市值进行加权，需要输入ff_size
    # 设置一个keep_self，是否把这次的group_weight存到self里面（会影响到后面style_analysis的）
    def group_backtest_weight(self, group_num,method='eq_weight',ff_size=None,keep_self=True):
        group_weight_oneday = []
        group_weight_method = group_weight_method_dict[method]
        for group in range(1, group_num+1):
            group_weight_oneday.append(group_weight_method(self.factor, self.lmt_filter_arr, group=group, group_num=group_num,ff_size=ff_size))
        if keep_self:
            self.group_weight_oneday = group_weight_oneday.copy()
        return group_weight_oneday

    def group_backtest_return(self,ret,group_weight_oneday=None,group_num=None):
        ret = np.array(ret)
        group_return_oneday = []
        if group_weight_oneday is None:
            group_weight_oneday = self.group_backtest_weight(group_num=group_num)
        for i in range(len(group_weight_oneday)):
            group_return_oneday.append(np.nansum(group_weight_oneday[i]*ret))
        return group_return_oneday

    def style_analysis(self, factor_style: dict, method='exposure_mean_gw',group_weight_oneday=None):
        style_analysis_f = style_analysis_method_dict[method]
        self.style_analysis_res = {}
        if group_weight_oneday is None:
            group_weight_oneday = self.group_weight_oneday
        for factor_s_name, factor_s in factor_style.items():
            factor_s = factor_s.loc[self.sec_code].values.squeeze()
            self.style_analysis_res[factor_s_name] = style_analysis_f(group_weight_oneday=group_weight_oneday,factor_s=factor_s)

    # TODO: 这里的计算方式不大合理，需要修改成 每日的i行业的个数占股票池中i行业的个数的ratio
    def industry_analysis(self, industry_arr):
        self.industry_analysis_res = {}
        group_name_list = [f'group{n}' for n in range(1,len(self.group_weight_oneday)+1)]
        for i in range(len(group_name_list)):
            group_i = group_name_list[i]
            group_weight_i = self.group_weight_oneday[i]
            self.industry_analysis_res[group_i] = np.nansum(industry_arr * (group_weight_i.reshape(-1,1)),axis=0)

    def calculate_factor_return(self,factor_control,ret):
        '''

        OLS回归求系数
        有的时候ret会是NaN，比如RET_t20，倒数19天的时候其实就算不出来了
        '''
        factor = self.factor
        ret = np.array(ret)
        if factor_control is not None:
            factor_c = factor_control.loc[self.sec_code].values
            X = np.hstack((factor.reshape(-1,1),factor_c))
        else:
            X = factor.reshape(-1,1)
        X = sm.add_constant(X)
        try:
            res = sm.OLS(ret,X,missing='drop').fit()
            return res.params[1]
        except:
            return np.nan
        

        

