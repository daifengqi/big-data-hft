import copy
import itertools
import os
import time

import numpy as np
import pandas as pd
import scipy.io as scio
import yaml
import factorAnalysisIOTools as IOTools
from factorAnalysisCalTools import prepare_RET_dict, time_horizon_dict


def save_df(this: pd.DataFrame,config: dict,file_name):
    file_path = os.path.join(config['utility_data_dir'],config[file_name]+".pkl")
    if os.path.exists(file_path):
        that = pd.read_pickle(file_path)
        this = that.append(this)
    this.drop_duplicates(subset=['date','code'],inplace=True,keep='last')
    this.to_pickle(file_path)
    
def loadmat2df_interday(path):
    temp = scio.loadmat(path)
    this = pd.DataFrame(temp['SCORE'],
                        index=temp['TRADE_DT'].squeeze().tolist(),
                        columns=[x[0] for x in temp['STOCK_CODE'].squeeze()])
    return this


class LoadStockInfo():
    def __init__(self,config):
        self.config = config
        temp = scio.loadmat(os.path.join(config['utility_data_dir'],config['SIZE_file_name']+".mat"))
        self.all_date_mat = temp['TRADE_DT'].squeeze()
        self.all_code_mat = [x[0] for x in temp['STOCK_CODE'].squeeze()]
        
        self.start_date = self.all_date_mat[self.all_date_mat>=config['start_date']][0]
        self.end_date = self.all_date_mat[self.all_date_mat<=config['end_date']][-1]
        self.start_indexer = np.where(self.all_date_mat==self.start_date)[0][0]
        self.end_indexer = np.where(self.all_date_mat==self.end_date)[0][0]
        self.date_list = self.all_date_mat[self.start_indexer:self.end_indexer+1]
        self.date_code_product = np.array(list(itertools.product(self.date_list,self.all_code_mat)))
        # self.date_code_product = np.array(list(itertools.product(self.date_list,self.all_code_mat)),dtype=[('date','int'),('code','object')])

    def load_arr(self,file_name,start_back_days=0,end_forward_days=0):
        path = os.path.join(
            self.config['utility_data_dir'],
            self.config[file_name]+".mat"
        )
        # log--20210804：对于ZX_1的数据，他们的STOCK_CODE的顺序和数量和其他的可能不一样
        # 查找self.all_code_mat的code在stkcd里面的位置，对应的score里面取出这些地方的元素，相当于reindex一下
        # update: 直接读取df，然后reindex，然后取value
        score = loadmat2df_interday(path).reindex(index=self.all_date_mat,columns=self.all_code_mat).values
        print(file_name,score.shape)
        start = max(self.start_indexer-start_back_days,0)
        end = min(self.end_indexer+end_forward_days+1,len(score))
        data = score[start:end]
        pad_width_axis0 = (
            max(start_back_days-self.start_indexer,0),
            max(self.end_indexer+end_forward_days+1-len(score),0)
        )
        if pad_width_axis0 != (0,0):
            data = np.pad(data, pad_width=(pad_width_axis0,(0,0)),mode='edge')
        return data

    def update_limit_300_688(self,start_back_days=0,end_forward_days=0):
        code_300 = np.array([x.startswith("300") for x in self.all_code_mat])
        code_688 = np.array([x.startswith("688") for x in self.all_code_mat])
        chg_date_300 = np.array([x>=20200824 for x in self.all_date_mat])
        code_300_score = np.tile(code_300,(len(chg_date_300),1))
        code_688_score = np.tile(code_688,(len(chg_date_300),1))
        chg_date_300_score = np.tile(chg_date_300.reshape(-1,1),(1,len(code_300)))
        assert code_300_score.shape == (len(self.all_date_mat),len(self.all_code_mat)), "code_300 shape wrong"

        start = max(self.start_indexer-start_back_days,0)
        end = min(self.end_indexer+end_forward_days+1,len(code_300_score))
        code_300_data = code_300_score[start:end]
        code_688_data = code_688_score[start:end]
        chg_date_data = chg_date_300_score[start:end]
        return code_300_data,code_688_data,chg_date_data




class UpdateStockPool():
    def __init__(self,config_path):
        with open(config_path) as f:
            self.config = yaml.load(f.read(),Loader=yaml.FullLoader)
        self.config['STOCK_INFO_file_path'] = os.path.join(self.config['utility_data_dir'],self.config['STOCK_INFO_file_name']+".pkl")

    def update_stock_info(self):
        config = copy.deepcopy(self.config)
        stock_info_loader = LoadStockInfo(config)

        t = time.time()
        # ADJFACTOR_2d_forward1 = stock_info_loader.load_arr('ADJFACTOR_file_name',end_forward_days=1)[1:]
        # OPEN_2d_forward1 = stock_info_loader.load_arr('ADJFACTOR_file_name',end_forward_days=1)[1:]
        # ADJOPEN_2d_forward1 = ADJFACTOR_2d_forward1*OPEN_2d_forward1
        FF_CAPITAL_2d = stock_info_loader.load_arr('FF_CAPITAL_file_name')
        PRECLOSE_2d_forward1 = stock_info_loader.load_arr('PRECLOSE_file_name',end_forward_days=1)[1:]
        # RETURN_TWAP_Q1_2d = stock_info_loader.load_arr('RETURN_TWAP_Q1_file_name',end_forward_days=1)
        FCT_HF_TWAP_H1_forward1 = stock_info_loader.load_arr('FCT_HF_TWAP_H1_file_name',end_forward_days=1)[1:]
        RETURN_2d = stock_info_loader.load_arr('RETURN_file_name',end_forward_days=1)

        # 20210917: 更新计算涨跌停的方法，算涨跌停的时候，20200824开始创业板涨幅停调整到了20%
        # 创业板通过stock_code前3位等于300识别； 还有科创板，前3位是688； 科创板自上市以来就是20%涨跌幅，创业板是20200824开始改20%的
        code_300_data,code_688_data,chg_date_data = stock_info_loader.update_limit_300_688()
        lmt1_t0 = (abs(RETURN_2d)>0.195)[:-1]
        lmt2_t0 = (abs(RETURN_2d)>0.095)[:-1] & (code_688_data==False) & (chg_date_data==False)
        lmt3_t0 = (abs(RETURN_2d)>0.095)[:-1] & (code_688_data==False) & (code_300_data==False)
        LIMIT_UD_t0_1d = (lmt1_t0 | lmt2_t0 | lmt3_t0).reshape(-1,)
        # 20211007: ADJOPEN*limit_ratio*5/6=LIMIT_PRICE_THRESHOLD_H1
        lmt1_t1 = (abs(FCT_HF_TWAP_H1_forward1/PRECLOSE_2d_forward1-1)>0.195*5/6)
        lmt2_t1 = (abs(FCT_HF_TWAP_H1_forward1/PRECLOSE_2d_forward1-1)>0.095*5/6) & (code_688_data==False) & (chg_date_data==False)
        lmt3_t1 = (abs(FCT_HF_TWAP_H1_forward1/PRECLOSE_2d_forward1-1)>0.095*5/6) & (code_688_data==False) & (code_300_data==False)
        LIMIT_UD_t1_1d = (lmt1_t1 | lmt2_t1 | lmt3_t1).reshape(-1,)
        LIMIT_UD_filter_t0_t1_1d = ((LIMIT_UD_t0_1d + LIMIT_UD_t1_1d) == 0)

        # LIMIT_UD_t0_1d = ((abs(RETURN_TWAP_Q1_2d)>0.095)[:-1]).reshape(-1,)
        # LIMIT_UD_t1_1d = ((abs(RETURN_TWAP_Q1_2d)>0.095)[1:]).reshape(-1,)
        # LIMIT_UD_filter_t0_t1_1d = (LIMIT_UD_t0_1d + LIMIT_UD_t1_1d) == 0

        ADJFACTOR_2d = stock_info_loader.load_arr('ADJFACTOR_file_name')
        VWAP_2d = stock_info_loader.load_arr('VWAP_file_name')
        CLOSE_2d = stock_info_loader.load_arr('CLOSE_file_name')
        AMOUNT_1d = stock_info_loader.load_arr('AMOUNT_file_name').reshape(-1,)
        ADJVWAP_1d = (ADJFACTOR_2d*VWAP_2d).reshape(-1,)
        ADJCLOSE_1d = (ADJFACTOR_2d*CLOSE_2d).reshape(-1,)
        CLOSE_1d = CLOSE_2d.reshape(-1,)
        FF_SIZE_1d = (FF_CAPITAL_2d*VWAP_2d).reshape(-1,)

        ADJFACTOR_2d_back19 = stock_info_loader.load_arr('ADJFACTOR_file_name',start_back_days=19)
        VOLUME_2d_back19 = stock_info_loader.load_arr('VOLUME_file_name',start_back_days=19)
        ADJVOLUME_2d_back19 = VOLUME_2d_back19/ADJFACTOR_2d_back19
        ADJVOLUME_ma20_2d = pd.DataFrame(ADJVOLUME_2d_back19).rolling(20).mean().values[19:]
        ADJVOLUME_ma20_1d = ADJVOLUME_ma20_2d.reshape(-1,)
        ADJVOLUME_ma20_q20_1d = np.nanquantile(ADJVOLUME_ma20_2d, q=0.2,axis=1,keepdims=False)
        ADJVOLUME_ma20_q20_1d = np.repeat(ADJVOLUME_ma20_q20_1d, repeats=ADJVOLUME_ma20_2d.shape[1])
        
        FF_CAPITAL_ma20_2d = stock_info_loader.load_arr('FF_CAPITAL_file_name',start_back_days=19)
        FF_CAPITAL_ma20_2d = pd.DataFrame(FF_CAPITAL_ma20_2d).rolling(20).mean().values[19:]
        FF_CAPITAL_ma20_1d = FF_CAPITAL_ma20_2d.reshape(-1,)
        FF_CAPITAL_ma20_q20_1d = np.nanquantile(FF_CAPITAL_ma20_2d, q=0.2,axis=1,keepdims=False)
        FF_CAPITAL_ma20_q20_1d = np.repeat(FF_CAPITAL_ma20_q20_1d, repeats=FF_CAPITAL_ma20_2d.shape[1])
        
        TOTAL_TRADEDAYS_1d = stock_info_loader.load_arr('TOTAL_TRADEDAYS_file_name').reshape(-1,)

        HS300_member_1d = stock_info_loader.load_arr('HS300_member_file_name').reshape(-1,)
        ZZ500_member_1d = stock_info_loader.load_arr('ZZ500_member_file_name').reshape(-1,)
        ISST_1d = stock_info_loader.load_arr('ISST_file_name').reshape(-1,)
        ISTRADEDAY_1d = stock_info_loader.load_arr('ISTRADEDAY_file_name').reshape(-1,)
        ZX_1_1d = stock_info_loader.load_arr('ZX_1_file_name').reshape(-1,)
        SIZE_1d = stock_info_loader.load_arr('SIZE_file_name').reshape(-1,)
        LIQUIDTY_1d = stock_info_loader.load_arr('LIQUIDTY_file_name').reshape(-1,)
        MOMENTUM_1d = stock_info_loader.load_arr('MOMENTUM_file_name').reshape(-1,)
        RESVOL_1d = stock_info_loader.load_arr('RESVOL_file_name').reshape(-1,)
        SIZENL_1d = stock_info_loader.load_arr('SIZENL_file_name').reshape(-1,)
        SRISK_1d = stock_info_loader.load_arr('SRISK_file_name').reshape(-1,)
        ADP_1d = stock_info_loader.load_arr('ADP_file_name').reshape(-1,)
        BETA_1d = stock_info_loader.load_arr('BETA_file_name').reshape(-1,)
        BTOP_1d = stock_info_loader.load_arr('BTOP_file_name').reshape(-1,)
        EARNYILD_1d = stock_info_loader.load_arr('EARNYILD_file_name').reshape(-1,)
        GROWTH_1d = stock_info_loader.load_arr('GROWTH_file_name').reshape(-1,)
        LEVERAGE_1d = stock_info_loader.load_arr('LEVERAGE_file_name').reshape(-1,)
        print("IO time",time.time()-t)
        t = time.time()
        STOCK_INFO_2d = np.stack(
            [
                SIZE_1d,ZX_1_1d,ADJVWAP_1d,ADJCLOSE_1d,CLOSE_1d,AMOUNT_1d,FF_SIZE_1d,
                HS300_member_1d,ZZ500_member_1d,ISST_1d,
                ISTRADEDAY_1d,TOTAL_TRADEDAYS_1d,LIMIT_UD_t0_1d,LIMIT_UD_t1_1d,LIMIT_UD_filter_t0_t1_1d,
                ADJVOLUME_ma20_1d,ADJVOLUME_ma20_q20_1d,FF_CAPITAL_ma20_1d,FF_CAPITAL_ma20_q20_1d,
                LIQUIDTY_1d,MOMENTUM_1d,RESVOL_1d,SIZENL_1d,SRISK_1d,
                ADP_1d,BETA_1d,BTOP_1d,EARNYILD_1d,GROWTH_1d,LEVERAGE_1d

            ],
            axis=1
        )
        print("concate time",time.time()-t)
        t = time.time()
        date_code_product_2d = stock_info_loader.date_code_product
        STOCK_INFO_cols_name = [
            'SIZE','ZX_1','ADJVWAP','ADJCLOSE','CLOSE','AMOUNT','FF_SIZE','HS300_member','ZZ500_member',
            'ISST','ISTRADEDAY','TOTAL_TRADEDAYS','LIMIT_UD_t0','LIMIT_UD_t1','LIMIT_UD_filter_t0_t1_1d',
            'ADJVOLUME_ma20','ADJVOLUME_ma20_q20','FF_CAPITAL_ma20','FF_CAPITAL_ma20_q20',
            'LIQUIDTY','MOMENTUM','RESVOL','SIZENL','SRISK',
            'ADP','BETA','BTOP','EARNYILD','GROWTH','LEVERAGE',
            'date','code'
        ]
        STOCK_INFO_df = pd.DataFrame(STOCK_INFO_2d.astype('float32'),columns=STOCK_INFO_cols_name[:-2])
        STOCK_INFO_df['date'] = date_code_product_2d[:,0].astype('int')
        STOCK_INFO_df['code'] = date_code_product_2d[:,1]

        print("form df time: ",time.time()-t)
        
        t = time.time()
        save_df(STOCK_INFO_df,self.config,'STOCK_INFO_file_name')
        self.STOCK_INFO = STOCK_INFO_df
        print("save time",time.time()-t)


    def update_stock_pool_basic(self,STOCK_INFO_file_path=None):
        '''
        T0当天结束计算的factor，这个SP用于分组，后续可能在真的计算分组return的时候，需要再筛选一个
        能否在第二天买入的SP_T1，（对于分析因子的影响？？？）
        SP_basic：
            - 全市场
            # - T0当天没有涨跌停（RETURN的abs大于0.095）
            # - T1当天没有涨跌停
            - 不是ST
            - 是TRADEDAY
            - 上市满一年（STOCK_INFO.datetime-STOCK_INFO.list_date > 365）

        '''
        if hasattr(self, 'STOCK_INFO'):
            STOCK_INFO = self.STOCK_INFO
        else:
            if STOCK_INFO_file_path is None:
                STOCK_INFO_file_path=self.config['STOCK_INFO_file_path']
            STOCK_INFO = pd.read_pickle(STOCK_INFO_file_path)
            STOCK_INFO = STOCK_INFO[(STOCK_INFO.date>=self.config['start_date'])&(STOCK_INFO.date<=self.config['end_date'])]
        filter_basic = (STOCK_INFO['ISTRADEDAY']==1.0) & (STOCK_INFO['TOTAL_TRADEDAYS']>250) & (STOCK_INFO['ISST']==0.0)
        STOCK_POOL_basic = STOCK_INFO.loc[filter_basic,self.config['STOCK_POOL_cols']]
        save_df(STOCK_POOL_basic,self.config,'STOCK_POOL_basic_file_name')




    def update_stock_pool_cap_vol_drop20(self,STOCK_INFO_file_path=None):
        '''
        T0当天结束计算的factor，这个SP用于分组，后续可能在真的计算分组return的时候，需要再筛选一个
        能否在第二天买入的SP_T1，（对于分析因子的影响？？？）
        SP_cap_vol_drop20：
            - 全市场
            - 去除流动性后20%（过去20天的voluma ma大于截面的q20）
            - 去除流通市值后20%
            - T0当天没有涨跌停（RETURN的abs大于0.095）
            - T1当天没有涨跌停
            - 不是ST
            - 是TRADEDAY
            - 上市满一年（STOCK_INFO.datetime-STOCK_INFO.list_date > 365）

        '''
        if hasattr(self, 'STOCK_INFO'):
            STOCK_INFO = self.STOCK_INFO
        else:
            if STOCK_INFO_file_path is None:
                STOCK_INFO_file_path=self.config['STOCK_INFO_file_path']
            STOCK_INFO = pd.read_pickle(STOCK_INFO_file_path)
            STOCK_INFO = STOCK_INFO[(STOCK_INFO.date>=self.config['start_date'])&(STOCK_INFO.date<=self.config['end_date'])]
        filter_basic = ( STOCK_INFO['ADJVOLUME_ma20']>STOCK_INFO['ADJVOLUME_ma20_q20'])&\
            (STOCK_INFO['FF_CAPITAL_ma20']>STOCK_INFO['FF_CAPITAL_ma20_q20'])&\
            (STOCK_INFO['ISTRADEDAY']==1.0) & (STOCK_INFO['TOTAL_TRADEDAYS']>250) & (STOCK_INFO['ISST']==0.0)
        STOCK_POOL_basic = STOCK_INFO.loc[filter_basic,self.config['STOCK_POOL_cols']]
        save_df(STOCK_POOL_basic,self.config,'STOCK_POOL_cap_vol_drop20_file_name')

    def update_stock_HS300(self,STOCK_INFO_file_path=None):
        '''
        满足basic的要求且是HS300里面的成分股
        '''
        if hasattr(self, 'STOCK_INFO'):
            STOCK_INFO = self.STOCK_INFO
        else:
            if STOCK_INFO_file_path is None:
                STOCK_INFO_file_path=self.config['STOCK_INFO_file_path']
            STOCK_INFO = pd.read_pickle(STOCK_INFO_file_path)
            STOCK_INFO = STOCK_INFO[(STOCK_INFO.date>=self.config['start_date'])&(STOCK_INFO.date<=self.config['end_date'])]
        filter_HS300 = (STOCK_INFO['HS300_member']==1.0) & (STOCK_INFO['LIMIT_UD_t0'] == 0.0) &\
            (STOCK_INFO['ISTRADEDAY']==1.0) & (STOCK_INFO['TOTAL_TRADEDAYS']>250) & (STOCK_INFO['ISST']==0.0)
        STOCK_POOL_HS300 = STOCK_INFO.loc[filter_HS300,self.config['STOCK_POOL_cols']]
        save_df(STOCK_POOL_HS300,self.config,'STOCK_POOL_HS300_file_name')


    def update_stock_ZZ500(self,STOCK_INFO_file_path=None):
        if hasattr(self, 'STOCK_INFO'):
            STOCK_INFO = self.STOCK_INFO
        else:
            if STOCK_INFO_file_path is None:
                STOCK_INFO_file_path=self.config['STOCK_INFO_file_path']
            STOCK_INFO = pd.read_pickle(STOCK_INFO_file_path)
            STOCK_INFO = STOCK_INFO[(STOCK_INFO.date>=self.config['start_date'])&(STOCK_INFO.date<=self.config['end_date'])]
        filter_ZZ500 = (STOCK_INFO['ZZ500_member']==1.0) &\
            (STOCK_INFO['ISTRADEDAY']==1.0) & (STOCK_INFO['TOTAL_TRADEDAYS']>250) & (STOCK_INFO['ISST']==0.0)
        STOCK_POOL_ZZ500 = STOCK_INFO.loc[filter_ZZ500,self.config['STOCK_POOL_cols']]
        save_df(STOCK_POOL_ZZ500,self.config,'STOCK_POOL_ZZ500_file_name')

    def update_stock_ZZ800(self,STOCK_INFO_file_path=None):
        if hasattr(self, 'STOCK_INFO'):
            STOCK_INFO = self.STOCK_INFO
        else:
            if STOCK_INFO_file_path is None:
                STOCK_INFO_file_path=self.config['STOCK_INFO_file_path']
            STOCK_INFO = pd.read_pickle(STOCK_INFO_file_path)
            STOCK_INFO = STOCK_INFO[(STOCK_INFO.date>=self.config['start_date'])&(STOCK_INFO.date<=self.config['end_date'])]
        filter_ZZ500 = ((STOCK_INFO['ZZ500_member']==1.0) | (STOCK_INFO['HS300_member']==1.0))&\
            (STOCK_INFO['ISTRADEDAY']==1.0) & (STOCK_INFO['TOTAL_TRADEDAYS']>250) & (STOCK_INFO['ISST']==0.0)
        STOCK_POOL_ZZ500 = STOCK_INFO.loc[filter_ZZ500,self.config['STOCK_POOL_cols']]
        save_df(STOCK_POOL_ZZ500,self.config,'STOCK_POOL_ZZ800_file_name')

    def update_stock_800(self,STOCK_INFO_file_path=None):
        if hasattr(self, 'STOCK_INFO'):
            STOCK_INFO = self.STOCK_INFO
        else:
            if STOCK_INFO_file_path is None:
                STOCK_INFO_file_path=self.config['STOCK_INFO_file_path']
            STOCK_INFO = pd.read_pickle(STOCK_INFO_file_path)
            STOCK_INFO = STOCK_INFO[(STOCK_INFO.date>=self.config['start_date'])&(STOCK_INFO.date<=self.config['end_date'])]
        filter_ZZ500 = ((STOCK_INFO['ZZ500_member']==1.0) | (STOCK_INFO['HS300_member']==1.0))&\
            (STOCK_INFO['ISTRADEDAY']==1.0) & (STOCK_INFO['TOTAL_TRADEDAYS']>250) & (STOCK_INFO['ISST']==0.0)
        STOCK_POOL_ZZ500 = STOCK_INFO.loc[filter_ZZ500,self.config['STOCK_POOL_cols']]
        save_df(STOCK_POOL_ZZ500,self.config,'STOCK_POOL_800_file_name')

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
    

if __name__ == '__main__':
    config_path = r"D:\HX_proj\factorAnalysis\updateStockPoolConfig.yaml"
    updateSP = UpdateStockPool(config_path)
    updateSP.update_stock_info()
    updateSP.update_stock_pool_basic()
    updateSP.update_stock_HS300()
    updateSP.update_stock_ZZ500()
    updateSP.update_stock_ZZ800()

    stock_pool_name = 'STOCK_POOL_basic'
    stock_pool_path = f"E:\\data\\interday\\{stock_pool_name}.pkl"
    update_benchmark_return(stock_pool_path=stock_pool_path,stock_pool_name=stock_pool_name,benchmark_ret_save_dir = r"E:\data\interday")