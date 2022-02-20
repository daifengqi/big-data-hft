import copy
import os
import numpy as np
import pandas as pd
import yaml
from tqdm import tqdm
import time

from factorAnalysisCalTools import (FactorDescribe, neutralize_method_dict,
                                    standardize_method_dict,time_horizon_dict,
                                    BARRA_FACTOR_NAME_LIST)
from factorAnalysisIOTools import (load_stock_code_trade_date, load_stock_pool,
                                   loadmat2df_factor, loadmat2df_interday,
                                   save_this,loadpkl2df_factor,load_obj_pkl,load_hx_feature)
from factorAnalysisSingleDay import FactorAnalysisSingleDay
from joblib import Parallel, delayed


Zscore = standardize_method_dict['Zscore']
neutralize_f = neutralize_method_dict['regression']

class FactorAnalysisDaysTimeHorizon():
    """ 
    输入factor，ret_dict
    不同的TH，都输出每天的情况（这样TH=5的ret应该会是TH=1的五倍左右）
    - 等最后画图的时候，根据TH，取出对应的日期，然后进行计算年化收益
    - 直接除以TH对应的日期然后求和 差别不大？
    这个class的属性只到timeSeries的结果为止
    储存的格式index都是dateint
    等画图的时候再改成datetime
    """

    def __init__(self,factor,factor_style_dict,stock_pool,ret_dict,benchmark_return_df,industry_name_dict=None,factor_pool_dict=None,corr_test_date_list=None,group_num=10,time_horizon_dict=time_horizon_dict,ret=None):
        self.factor = factor
        self.factor_neu = pd.DataFrame(index=factor.index,columns=factor.columns)
        self.group_weight_order1_eqW_df = pd.DataFrame(index=factor.index,columns=factor.columns)
        self.group_weight_order10_eqW_df = pd.DataFrame(index=factor.index,columns=factor.columns)
        self.ret = ret_dict['RET_t2']
        self.factor_style_dict = factor_style_dict
        

        self.stock_pool = stock_pool
        self.ret_dict = ret_dict
        # self.factor_name = factor_name
        self.group_num = group_num
        self.time_horizon_dict = time_horizon_dict
        self.time_horizon_keys = list(ret_dict.keys())
        self.industry_name_dict = industry_name_dict
        self.factor_pool_dict = factor_pool_dict # factor_name-->factor_df
        self.corr_test_date_list = corr_test_date_list

        # self.res_figs_dict = {} # store figs plotted

        # dateint_list = list(set(self.factor.index).intersection(set(self.ret.index)))
        # dateint_list.sort()
        # 默认的date_list是int类型
        temp_date_list = list(set(self.factor.index).intersection(set(pd.unique(self.stock_pool.date))))
        temp_date_list.sort()
        self.dateint_list = temp_date_list
        # self.datetime_list = [pd.to_datetime(str(x)) for x in self.dateint_list]
        self.benchmark_return_eqW_th_df = benchmark_return_df.loc[self.dateint_list]

        
    def run(self):
        factor_describe_keys = ['nans_ratio','infs_ratio','zeros_ratio','mean','median','std','skew','kurtosis']
        factor_describe_dict = {k:[] for k in factor_describe_keys}
        # 计算factor与pool中的factor_old的corr
        # 提前订好需要抽样的日期（每周的周中，利用all_trade_date来计算，防止之后不统一）
        factor_pool_corr_dict = {k:[] for k in self.factor_pool_dict.keys()}
        factor_pool_corr_date_list = []
        # for k in factor_describe_keys:
        #     factor_describe_dict[k] = []

        group_weight_order1_list = []
        group_weight_order10_list = []
        # 不同time Horizon记录下IC group_return factor_return bench_mark_return
        # group_return_eqW_th_list_dict的value是一个group_num维度的list，其他的都是一个值
        IC_th_dict = {k:[] for k in self.time_horizon_keys}
        IC_th_all_barra_dict = {k:[] for k in self.time_horizon_keys}
        group_return_eqW_th_list_dict = {k:[] for k in self.time_horizon_keys}
        group_IC_th_list_dict = {k:[] for k in self.time_horizon_keys}
        factor_return_th_dict = {k:[] for k in self.time_horizon_keys}
        # benchmark_return_eqW_th_dict = {k:[] for k in self.time_horizon_keys}
        

        # group_return_ffS_th_list_dict = {k:[] for k in self.time_horizon_keys}
        # benchmark_return_ffS_th_dict = {k:[] for k in self.time_horizon_keys}

        # 风格分析是一样的，每次选出来的股票是一样的，只是持仓时间不同
        factor_style_analysis_res = {k:[] for k in self.factor_style_dict.keys()}
        # 20210627 添加行业分析，观察每一组的各行业占比
        # factor_industry_analysis_res = {k:[] for k in [f'group{n}' for n in range(1,self.group_num+1)]}

        for adate in tqdm(self.dateint_list):
            stock_info = self.stock_pool[self.stock_pool.date==adate]
            #TODO:20210924!!!!! 
            # stock_info = stock_info[stock_info.LIMIT_UD_filter_t0_t1_1d==1.0]
            stock_info = stock_info[stock_info.LIMIT_UD_t0==0.0]
            # 20210922：选factor时把t0涨跌停的剔除（factor计算的时候可能会以为个股涨跌停导致value比较奇怪）
            sec_code = list(stock_info.code)
            # 作为factor的分组时剔除t1涨跌停的
            # 20210922：构造STOCK_POOL的时候多加了一列LIMIT_UD_filter_t0_t1_1d，是1则表示保留
            lmt_filter_adate_ratio = stock_info.LIMIT_UD_filter_t0_t1_1d.mean()
            lmt_filter_abnormal = True if lmt_filter_adate_ratio<0.9 else False
            lmt_filter_adate_arr = (stock_info.LIMIT_UD_filter_t0_t1_1d).replace(0,np.nan).values


            # sec_code_lmt_t1 = list(stock_info[stock_info.LIMIT_UD_t1==0.0].code)
            # lmt_filter_adate_arr = np.array([1 if x in sec_code_lmt_t1 else np.nan for x in sec_code])

            factor_adate = self.factor.loc[adate,sec_code]
            ff_size_adate = stock_info.FF_SIZE.values
            # ret_adate = self.ret.loc[adate,sec_code]
            ret_dict = {k:v.loc[adate,sec_code] for k,v in self.ret_dict.items()}
            # ret_benchmark_dict = {k:v.loc[adate,sec_code_benchmark] for k,v in self.ret_dict.items()}

            factor_describe_adate = FactorDescribe(factor_adate.values)
            factor_describe_adate.run()
            factor_describe_dict['nans_ratio'].append(factor_describe_adate.nans_ratio)
            factor_describe_dict['infs_ratio'].append(factor_describe_adate.infs_ratio)
            factor_describe_dict['zeros_ratio'].append(factor_describe_adate.zeros_ratio)
            factor_describe_dict['mean'].append(factor_describe_adate.factor_mean)
            factor_describe_dict['median'].append(factor_describe_adate.factor_median)
            factor_describe_dict['std'].append(factor_describe_adate.factor_std)
            factor_describe_dict['skew'].append(factor_describe_adate.factor_skew)
            factor_describe_dict['kurtosis'].append(factor_describe_adate.factor_kurtosis)

            # TODO: set factor_control in config
            # ff_capital = Zscore((stock_info.FF_CAPITAL).set_axis(sec_code))
            size_barra = ((stock_info.SIZE).set_axis(sec_code))
            zx1 = pd.get_dummies((stock_info.ZX_1).set_axis(sec_code))
            
            factor_control = pd.concat([size_barra,zx1],axis=1)

            all_barra = (stock_info[BARRA_FACTOR_NAME_LIST]).set_axis(sec_code)
            factor_control_all_barra =  pd.concat([all_barra,zx1],axis=1)


            # TODO: set preprocessing method in config
            fasd = FactorAnalysisSingleDay(factor_adate, lmt_filter_adate_arr)
            fasd.deExtreme(method='three_sigma')
            fasd_all_barra = copy.deepcopy(fasd)
            fasd.neutralize(factor_control,method='regression')
            fasd_all_barra.neutralize(factor_control_all_barra,method='regression')
            fasd.standardize(method='Zscore')
            factor_rank_adate = fasd.get_factor_rank()
            # 20210917：在stock_pool中，但是因子值为np.nan的用0来代替（在预处理之后）
            fasd.fill_nan_inf(0)
            fasd_all_barra.standardize(method='Zscore')
            fasd_all_barra.fill_nan_inf(0)
            # 20210810： 默认还是等权的分析
            self.factor_neu.loc[adate,sec_code] = fasd.factor

            group_weight_oneday_eqW = fasd.group_backtest_weight(group_num=self.group_num,method='eq_weight')
            self.group_weight_order1_eqW_df.loc[adate,sec_code] = group_weight_oneday_eqW[0]
            self.group_weight_order10_eqW_df.loc[adate,sec_code] = group_weight_oneday_eqW[-1]

            # 测一下市值加权后的结果会不会有不同，不存在self中
            # group_weight_oneday_ffS = fasd.group_backtest_weight(group_num=self.group_num,method='ff_size',ff_size=ff_size_adate,keep_self=False)

            factor_style_adate = {k:(v.loc[adate,sec_code]) for k,v in self.factor_style_dict.items()}
            factor_style_adate['SIZE'] = factor_style_adate['SIZE'] - factor_style_adate['SIZE'].mean()
            factor_style_adate['AMOUNT_log'] = factor_style_adate['AMOUNT_log'] - factor_style_adate['AMOUNT_log'].mean()
            fasd.style_analysis(factor_style_adate,method='exposure_mean_gw')   # percentile_gw
            for k in factor_style_analysis_res.keys():
                factor_style_analysis_res[k].append(fasd.style_analysis_res[k]) 
            
            # Industry Analysis
            # fasd.industry_analysis(zx1.values)
            # for group_i in [f'group{n}' for n in range(1,self.group_num+1)]:
            #     factor_industry_analysis_res[group_i].append(pd.Series(fasd.industry_analysis_res[group_i],index=zx1.columns))




            for time_horizon,ret in ret_dict.items():
                ret_fasd = FactorAnalysisSingleDay(ret)
                ret_fasd.neutralize(factor_control,method='regression')
                ret_neutral = ret_fasd.factor
                ret_rank_adate = ret_fasd.get_factor_rank()
                time_horizon_n = self.time_horizon_dict[time_horizon]
                IC_th_dict[time_horizon].append(fasd.calculate_IC(ret=ret_neutral,method='pearson',filter_arr=lmt_filter_adate_arr))
                IC_th_all_barra_dict[time_horizon].append(fasd_all_barra.calculate_IC(ret=ret_neutral,method='pearson',filter_arr=lmt_filter_adate_arr))
                factor_return_th_dict[time_horizon].append(fasd.calculate_factor_return(factor_control=factor_control,ret=ret)/time_horizon_n)
                # log--20211019：增加多头 IC的记录
                group_IC_th_list_dict[time_horizon].append(fasd.calculate_group_IC_rank(factor_rank=factor_rank_adate,ret_rank=ret_rank_adate,group_weight_oneday=group_weight_oneday_eqW,method='pearson',filter_arr=lmt_filter_adate_arr))
                # group_IC_th_list_dict[time_horizon].append(fasd.calculate_group_IC(group_weight_oneday=group_weight_oneday_eqW,ret=ret_neutral,method='pearson',filter_arr=lmt_filter_adate_arr))
                # log--20211014：在全市场暴跌20200123，20200203的情况下，pool_filter_t1_t0里面的和bench里面的会差很多
                if lmt_filter_abnormal:
                    group_return_eqW_th_list_dict[time_horizon].append([self.benchmark_return_eqW_th_df.loc[adate,time_horizon]]*self.group_num)
                else:
                    group_return_eqW_th_list_dict[time_horizon].append([x/time_horizon_n for x in fasd.group_backtest_return(group_weight_oneday=group_weight_oneday_eqW,ret=ret)])

                # 全市场等权基准：只剔除当日涨跌停，ST，上市不满一年（单独使用ret_benchmark）
                # ret_benchmark = ret_benchmark_dict[time_horizon]
                # benchmark_return_eqW_th_dict[time_horizon].append(np.nanmean(ret)/time_horizon_n)
                # group_return_ffS_th_list_dict[time_horizon].append([x/time_horizon_n for x in fasd.group_backtest_return(group_weight_oneday=group_weight_oneday_ffS,ret=ret)])
                # benchmark_return_ffS_th_dict[time_horizon].append(np.nansum(ret*(ff_size_adate/np.nansum(ff_size_adate)))/time_horizon_n)

            if self.factor_pool_dict:
                if (adate in self.corr_test_date_list):
                    # ttt = time.time()
                    factor_pool_corr_date_list.append(adate)
                    for a_factor_old_name,a_factor_old_df in self.factor_pool_dict.items():
                        a_factor_old_adate = a_factor_old_df.loc[adate,sec_code]
                        factor_pool_corr_dict[a_factor_old_name].append(a_factor_old_adate.corr(factor_adate))
                    # print(adate,'  ',time.time()-ttt,'\n')



        # TODO: divide time horizon when save into self???
        self.factor_describe_df = pd.DataFrame(factor_describe_dict,index=self.dateint_list)
        # 20210922：benchmark单独计算直接load进来 __init__
        # self.benchmark_return_eqW_th_df = pd.DataFrame(benchmark_return_eqW_th_dict,index=self.dateint_list)
        # self.benchmark_return_ffS_th_df = pd.DataFrame(benchmark_return_ffS_th_dict,index=self.dateint_list)
        self.IC_th_df = pd.DataFrame(IC_th_dict,index=self.dateint_list)
        self.IC_th_all_barra_df = pd.DataFrame(IC_th_all_barra_dict,index=self.dateint_list)
        self.factor_return_th_df = pd.DataFrame(factor_return_th_dict,index=self.dateint_list)

        self.factor_pool_corr_df = pd.DataFrame(factor_pool_corr_dict,index=factor_pool_corr_date_list)

        # make sure we always long group 10
        group_cols_list = [f'group{n}' for n in range(1,self.group_num+1)]
        group_cols_list_order = [f'group{n}' for n in range(1,self.group_num+1)]
        if self.IC_th_df.mean().values[0] < 0:
            group_cols_list.reverse()
            self.long_group_weight_eqW_df = self.group_weight_order1_eqW_df
        else:
            self.long_group_weight_eqW_df = self.group_weight_order10_eqW_df



        self.group_return_eqW_th_dict = {k:pd.DataFrame(v,index=self.dateint_list,columns=group_cols_list)[group_cols_list_order] for k,v in group_return_eqW_th_list_dict.items()}
        self.style_analysis_res_dict = {k:pd.DataFrame(v,index=self.dateint_list,columns=group_cols_list)[group_cols_list_order] for k,v in factor_style_analysis_res.items()}
        self.group_excess_return_eqW_th_dict = {k:v.sub(self.benchmark_return_eqW_th_df[k],axis=0) for k,v in self.group_return_eqW_th_dict.items()}
        # self.group_return_ffS_th_dict = {k:pd.DataFrame(v,index=self.dateint_list,columns=group_cols_list)[group_cols_list_order] for k,v in group_return_ffS_th_list_dict.items()}
        # self.group_excess_return_ffS_th_dict = {k:v.sub(self.benchmark_return_ffS_th_df[k],axis=0) for k,v in self.group_return_ffS_th_dict.items()}
        # self.industry_analysis_res_dict = {group_i_order:pd.DataFrame(factor_industry_analysis_res[group_i],index=self.dateint_list).rename(columns=self.industry_name_dict) for group_i,group_i_order in zip(group_cols_list,group_cols_list_order)}
        self.group_IC_th_dict = {k:pd.DataFrame(v,index=self.dateint_list,columns=group_cols_list)[group_cols_list_order] for k,v in group_IC_th_list_dict.items()}
        self.long_group = group_cols_list_order[-1]
        self.short_group = group_cols_list_order[0]
        self.long_short_return_eqW_th_df = pd.DataFrame({k:v[self.long_group].sub(v[self.short_group]) for k,v in self.group_return_eqW_th_dict.items()})
        # self.long_short_return_ffS_th_df = pd.DataFrame({k:v[self.long_group].sub(v[self.short_group]) for k,v in self.group_return_ffS_th_dict.items()})
    
    def res_save(self,save_dir):
        time_horizon_dict = self.time_horizon_dict.copy()
        # factor log value latest 5 days
        factor_value_df = self.factor
        save_this(factor_value_df,save_dir,'00_factor_value_df')
        factor_value_neu_df = self.factor_neu
        save_this(factor_value_df,save_dir,'00_factor_value_neu_df')

        # factor describe
        factor_describe_df = self.factor_describe_df.copy()
        save_this(factor_describe_df,save_dir,'01_factor_describe_df')

        # IC
        IC_th_df = self.IC_th_df.copy()
        save_this(IC_th_df,save_dir,'02_IC_th_df')
        IC_th_all_barra_df = self.IC_th_all_barra_df.copy()
        save_this(IC_th_all_barra_df,save_dir,'02_IC_th_all_barra_df')
        group_IC_th_dict = self.group_IC_th_dict.copy()
        save_this(group_IC_th_dict,save_dir,'02_group_IC_th_dict')

        # group excess return
        group_excess_return_eqW_th_dict = self.group_excess_return_eqW_th_dict.copy()
        long_tt = pd.DataFrame(columns=list(self.time_horizon_keys))
        for time_horizon, th_n in time_horizon_dict.items():
            tt = group_excess_return_eqW_th_dict[time_horizon].copy()
            long_tt[time_horizon] = tt[self.long_group]
        save_this(long_tt,save_dir,'03_long_excess_return_eqW_th_df')
        save_this(group_excess_return_eqW_th_dict,save_dir,'04_group_excess_return_eqW_th_dict')

        # group_excess_return_ffS_th_dict = self.group_excess_return_ffS_th_dict.copy()
        # long_tt = pd.DataFrame(columns=list(self.time_horizon_keys))
        # for time_horizon, th_n in time_horizon_dict.items():
        #     tt = group_excess_return_ffS_th_dict[time_horizon].copy()
        #     long_tt[time_horizon] = tt[self.long_group]
        # save_this(long_tt,save_dir,'03_long_excess_return_ffS_th_df')
        # save_this(group_excess_return_ffS_th_dict,save_dir,'04_group_excess_return_ffS_th_dict')

        # style analysis
        style_analysis_res_dict = self.style_analysis_res_dict.copy()
        save_this(style_analysis_res_dict,save_dir,'05_style_analysis_res_dict')

        # long short return
        long_short_return_eqW_th_df = self.long_short_return_eqW_th_df.copy()
        save_this(long_short_return_eqW_th_df,save_dir,'06_long_short_return_eqW_th_df')
        # long_short_return_ffS_th_df = self.long_short_return_ffS_th_df.copy()
        # save_this(long_short_return_ffS_th_df,save_dir,'06_long_short_return_ffS_th_df')

        # factor return
        factor_return_th_df = self.factor_return_th_df.copy()
        save_this(factor_return_th_df,save_dir,'07_factor_return_th_df')

        # benchmark return
        benchmark_return_eqW_th_df = self.benchmark_return_eqW_th_df.copy()
        save_this(benchmark_return_eqW_th_df,save_dir,'08_benchmark_return_eqW_th_df')
        # benchmark_return_ffS_th_df = self.benchmark_return_ffS_th_df.copy()
        # save_this(benchmark_return_ffS_th_df,save_dir,'08_benchmark_return_ffS_th_df')

        # group return
        group_return_eqW_th_dict = self.group_return_eqW_th_dict.copy()
        save_this(group_return_eqW_th_dict,save_dir,'09_group_return_eqW_th_dict')

        # factor pool corr weekly record
        factor_pool_corr_df = self.factor_pool_corr_df.copy()
        save_this(factor_pool_corr_df,save_dir,'10_factor_pool_corr_df')

        # long_group_eqW_df
        long_group_weight_eqW_df = self.long_group_weight_eqW_df.copy()
        save_this(long_group_weight_eqW_df,save_dir,'11_long_group_weight_eqW_df')



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

def prepare_corr_date(date_int_list,how='weekly'):
    '''
    后续可以添加monthly的方式抽样
    '''
    corr_date_list = []
    if how == 'weekly':
        for a_date_int in date_int_list:
            a_day_of_week = pd.to_datetime(str(a_date_int)).dayofweek
            if a_day_of_week == 2:
                corr_date_list.append(a_date_int)
    return corr_date_list
        


def prepare_industry_name_dict(path,industry_level='zx_1'):
    industry_name = pd.read_csv(path,header=None,encoding='gbk')
    industry_name = industry_name[industry_name.iloc[:,-1]==industry_level]
    industry_name.iloc[:,2] = industry_name.iloc[:,2].astype('float')
    industry_name_se = pd.Series(industry_name.values[:,1],index=industry_name.values[:,2])
    return industry_name_se.to_dict()

def updateFactorAnalysisRes(config_path):
    with open(config_path) as f:
        config = yaml.load(f.read(),Loader=yaml.FullLoader)
        
    # log--20210811
    # 应该load stock code的时候，先读取ref里面的全部data，利用config里面start_date和end_date把这段时间里面ref里面不是nan的拿出来
    # 这样相当于在end_date之后上市的stock信息是完全不会被用到的！！
    # STOCK_POOL同理
    STOCK_CODE_DATE_ref_path = os.path.join(config['utility_data_dir'],config['STOCK_CODE_ref_file_name']+".mat")
    stock_code_cols,trade_date_list = load_stock_code_trade_date(STOCK_CODE_DATE_ref_path,config['start_date'],config['end_date'])

    STOCK_POOL_path = os.path.join(config['utility_data_dir'],config['stock_pool_file_name']+".pkl")
    STOCK_POOL = load_stock_pool(STOCK_POOL_path,config['start_date'],config['end_date'])

    
    # load Industry name for Industry analysis, 目前不用行业分析（低频一般不怎么看行业分布）
    # INDUSTRY_NAME_path = os.path.join(config['utility_data_dir'],config['industry_name_file_name']+".csv")
    # INDUSTRY_NAME_dict = prepare_industry_name_dict(INDUSTRY_NAME_path)

    # prepare RET_dict for analysis strategy result in different time horizon
    # log--20210804: 想把df都统一成stock_code_cols一样的格式 不要直接取[stock_code_cols]，使用reindex(columns=stock_code_cols)
    # ADJFACTOR = loadmat2df_interday(os.path.join(config['utility_data_dir'],config['adjfactor_file_name']+".mat")).reindex(columns=stock_code_cols)
    # AMOUNT = loadmat2df_interday(os.path.join(config['utility_data_dir'],config['amount_file_name']+".mat")).reindex(columns=stock_code_cols)
    # VWAP = loadmat2df_interday(os.path.join(config['utility_data_dir'],config['vwap_file_name']+".mat")).reindex(columns=stock_code_cols)
    # CLOSE = loadmat2df_interday(os.path.join(config['utility_data_dir'],config['close_file_name']+".mat")).reindex(columns=stock_code_cols)
    # # RETURN = loadmat2df_interday(os.path.join(config['utility_data_dir'],config['return_file_name']+".mat"))[stock_code_cols]
    # ADJVWAP = VWAP*ADJFACTOR
    # ADJCLOSE = CLOSE*ADJFACTOR # for style factor
    # log--20211011：实际上应该提供了STOCK_POOL的数据就可以直接backtest
    ADJVWAP = STOCK_POOL.pivot(index='date',columns='code',values='ADJVWAP').reindex(columns=stock_code_cols)
    ADJCLOSE = STOCK_POOL.pivot(index='date',columns='code',values='ADJCLOSE').reindex(columns=stock_code_cols)
    CLOSE = STOCK_POOL.pivot(index='date',columns='code',values='CLOSE').reindex(columns=stock_code_cols)
    AMOUNT = STOCK_POOL.pivot(index='date',columns='code',values='AMOUNT').reindex(columns=stock_code_cols)



    RETURN_t5 = ADJCLOSE.pct_change(5) # for style factor
    RETURN_t20 = ADJCLOSE.pct_change(20) # for style factor

    RET_dict = prepare_RET_dict(ADJVWAP,ADJCLOSE)
    BENCHMARK_RETURN_df = load_obj_pkl(os.path.join(config['utility_data_dir'],config['benchmark_return_file_name']+".pkl"))

    # prepare style factor
    FACTOR_STYLE={}
    for STYLE_NAME in config['style_factor_name_list']:
            STYLE_path = os.path.join(config['utility_data_dir'],STYLE_NAME+".mat")
            FACTOR_STYLE[STYLE_NAME] = loadmat2df_interday(STYLE_path).reindex(columns=stock_code_cols)
    FACTOR_STYLE['AMOUNT_log'] = (np.log(AMOUNT).replace([-np.inf,np.inf],np.nan))
    FACTOR_STYLE['CLOSE'] = (CLOSE.replace([-np.inf,np.inf],np.nan))
    FACTOR_STYLE['RETURN_t5'] = RETURN_t5.apply(Zscore,axis=1)
    FACTOR_STYLE['RETURN_t20'] = RETURN_t20.apply(Zscore,axis=1)

    # get factor pool corr date list
    factor_pool_corr_date_list = prepare_corr_date(trade_date_list,how='weekly')
    # get factor pool old feature
    FACTOR_POOL_dict = {}

    if config['corr_analysis']: 
        print("start to load factor pool")
        tt = time.time()
        for a_factor_old_name in os.listdir(config['factor_pool_dir']):
            a_factor_old_path = os.path.join(config['factor_pool_dir'],a_factor_old_name)
            if '.pkl' in a_factor_old_name:
                temp_df = loadpkl2df_factor(a_factor_old_path)
            elif '.mat' in a_factor_old_name:
                temp_df = loadmat2df_factor(a_factor_old_path)
            index_list = list(set(temp_df.index)&set(factor_pool_corr_date_list))
            FACTOR_POOL_dict[a_factor_old_name[:-4]] = (temp_df.loc[index_list]).astype('float32')
        print("IO Time for loading factor pool:",time.time()-tt)
    else:
        print("No corr analysis with old feature pool!")

    # factor_name_list_mat = config['factor_name_list_mat'] if config['factor_name_list_mat'] is not None else []
    # factor_path_list_mat = [os.path.join(config['factor_dir'],factor_name+".mat") for factor_name in factor_name_list_mat]
    # factor_name_list_pkl = config['factor_name_list_pkl'] if config['factor_name_list_pkl'] is not None else []
    # factor_path_list_pkl = [os.path.join(config['factor_dir'],factor_name+".pkl") for factor_name in factor_name_list_pkl]
    # factor_name_list_all = factor_name_list_mat + factor_name_list_pkl
    # factor_path_list_all = factor_path_list_mat + factor_path_list_pkl
    factor_name_list_all = [factor_basename.split(".")[0] for factor_basename in config['factor_basename_list']]
    factor_path_list_all = [os.path.join(config['factor_dir'],factor_basename) for factor_basename in config['factor_basename_list']]

    def analysis_parallel(factor_name,FACTOR_path):
        t0 = time.time()
        res_save_dir = os.path.join(config['factor_analysis_res_dir'],factor_name,config['stock_pool_file_name'].replace("STOCK_POOL_",""))
        FACTOR = load_hx_feature(FACTOR_path).reindex(columns=stock_code_cols).dropna(how='all').loc[config['start_date']:config['end_date']]
        faDays = FactorAnalysisDaysTimeHorizon(
            factor = FACTOR,
            factor_style_dict=FACTOR_STYLE,
            stock_pool = STOCK_POOL,
            ret_dict=RET_dict,
            benchmark_return_df=BENCHMARK_RETURN_df,
            # industry_name_dict=INDUSTRY_NAME_dict,
            factor_pool_dict=FACTOR_POOL_dict,
            corr_test_date_list=factor_pool_corr_date_list,
            group_num=config['group_num']
        )

        faDays.run()
        faDays.res_save(res_save_dir)
        print(f"=========== updateFactorAnalysisRes {factor_name} use {time.time()-t0} second ===========")

    if config['parallel']:
        Parallel(n_jobs=config['N_CPU'])(delayed(analysis_parallel)(factor_name, FACTOR_path) for factor_name,FACTOR_path in zip(factor_name_list_all,factor_path_list_all))
    else:
        for factor_name,FACTOR_path in tqdm(zip(factor_name_list_all,factor_path_list_all)):
            analysis_parallel(factor_name, FACTOR_path)

    # for factor_name,FACTOR_path in tqdm(zip(factor_name_list_all,factor_path_list_all)):
    #     res_save_dir = os.path.join(config['factor_analysis_res_dir'],factor_name,config['stock_pool_file_name'].replace("STOCK_POOL_",""))
    #     FACTOR = load_hx_feature(FACTOR_path).reindex(columns=stock_code_cols).dropna(how='all').loc[config['start_date']:config['end_date']]
    #     faDays = FactorAnalysisDaysTimeHorizon(
    #         factor = FACTOR,
    #         factor_style_dict=FACTOR_STYLE,
    #         stock_pool = STOCK_POOL,
    #         ret_dict=RET_dict,
    #         benchmark_return_df=BENCHMARK_RETURN_df,
    #         # industry_name_dict=INDUSTRY_NAME_dict,
    #         factor_pool_dict=FACTOR_POOL_dict,
    #         corr_test_date_list=factor_pool_corr_date_list,
    #         group_num=config['group_num']
    #     )

    #     faDays.run()
    #     faDays.res_save(res_save_dir)
    #     print(f"=================== updateFactorAnalysisRes {factor_name} successfully =================")

if __name__ == '__main__':
    config_path = r"D:\HX_proj\factorTest\factorAnalysisConfig.yaml"
    # config_path = "D:\\HX_proj\\factorTest\\factorAnalysisConfig.yaml"
    updateFactorAnalysisRes(config_path)
    # plotFactorAnalysisRes(config_path)
    print('end')
