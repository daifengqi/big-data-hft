import copy
import os
import pathlib
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.gridspec import GridSpec
import seaborn

# 支持中文
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显中文符号
from .factorAnalysisIOTools import load_res

time_horizon_dict = {
    'RET_t1':1,
    'RET_t2':1,
    'RET_t5':5,
    'RET_t10':10,
    'RET_t15':15,
    'RET_t20':20,
    }



class FactorAnalysisResPlot():

    def __init__(self,config):
        self.config = config
        # with open(config_path) as f:
        #     self.config = yaml.load(f.read(),Loader=yaml.FullLoader) 
        res_save_dir = os.path.join(self.config['factor_analysis_res_dir'],self.config['factor_name'],config['stock_pool_file_name'].replace("STOCK_POOL_",""))
        self.factor_value = load_res(res_save_dir,'00_factor_value_df',to_datetime=True)
        self.factor_describe_df = load_res(res_save_dir,'01_factor_describe_df',to_datetime=True)
        self.IC_th_df = load_res(res_save_dir,'02_IC_th_df',to_datetime=True)
        self.IC_th_all_barra_df = load_res(res_save_dir,'02_IC_th_all_barra_df',to_datetime=True)
        self.group_IC_th_dict = load_res(res_save_dir,'02_group_IC_th_dict',to_datetime=True)
        self.long_excess_return_eqW_th_df = load_res(res_save_dir,'03_long_excess_return_eqW_th_df',to_datetime=True)
        self.group_excess_return_eqW_th_dict = load_res(res_save_dir,'04_group_excess_return_eqW_th_dict',to_datetime=True)
        self.style_analysis_res_dict = load_res(res_save_dir,'05_style_analysis_res_dict',to_datetime=True)
        self.long_short_return_eqW_th_df = load_res(res_save_dir,'06_long_short_return_eqW_th_df',to_datetime=True)
        # self.factor_pool_corr_df = load_res(res_save_dir,'10_factor_pool_corr_df',to_datetime=True)

        # self.long_excess_return_ffS_th_df = load_res(res_save_dir,'03_long_excess_return_ffS_th_df',to_datetime=True)
        # self.group_excess_return_ffS_th_dict = load_res(res_save_dir,'04_group_excess_return_ffS_th_dict',to_datetime=True)
        # self.long_short_return_ffS_th_df = load_res(res_save_dir,'06_long_short_return_ffS_th_df',to_datetime=True)

        # self.benchmark_return_th_df = load_res(res_save_dir,'08_benchmark_return_th_df',to_datetime=True)
        # self.industry_analysis_res_dict = load_res(res_save_dir,'10_industry_analysis_res_dict',to_datetime=True)
        # self.factor_return_th_df = load_res(res_save_dir,'07_factor_return_th_df',to_datetime=True)
        self.res_figs_dict = {}

    def run_plot_method(self):
        self.factor_describe_plot()
        self.IC_long_excess_return_plot()
        self.style_analysis_plot()
        self.IC_RETt2_year_cumsum_plot()
        self.long_excess_RETt2_year_cumsum_plot()
        # self.IC_th_cumsum_plot()
        self.group_IC_mean_RETt2_year_plot()
        # self.IC_th_all_barra_cumsum_plot()
        # self.IC_IR_year_th_plot()
        # self.IC_mean_year_th_plot()
        # self.long_short_return_th_plot()
        # self.group_annual_excess_return_year_th_plot()
        self.group_excess_return_plot()
        self.style_analysis_all_plot()
        # if not self.factor_pool_corr_df.empty:
        #     self.factor_pool_corr_plot()
        # self.factor_pool_corr_table_plot()
        # self.industry_analysis_plot()
        # self.factor_return_RETt2_year_cumsum_plot()
        # self.factor_return_th_cumsum_plot()
        # self.factor_return_IR_year_th_plot()
        # self.factor_return_mean_year_th_plot()

    def save_fig2pdf(self):
        pdf_dir = os.path.join(self.config['factor_analysis_res_plot_dir'],self.config['factor_name'])
        pathlib.Path(pdf_dir).mkdir(parents=True,exist_ok=True)
        pdf_path = os.path.join(pdf_dir,self.config['factor_name']+self.config['stock_pool_file_name'].replace("STOCK_POOL","")+"_"+datetime.now().date().strftime("%Y%m%d")+".pdf")
        with PdfPages(pdf_path) as pdf:
            for k,v in self.res_figs_dict.items():
                pdf.savefig(v)

    def factor_describe_plot(self,latest_days=5):
        factor = self.factor_value.iloc[-latest_days:].copy()
        factor_describe_df = self.factor_describe_df.iloc[-250:]
        fig, ax = plt.subplots(2,3,figsize=(18,10))
        for i in range(1,latest_days+1):
            factor_i = factor.iloc[-i]
            date = factor.index[-i]
            factor_i.plot(kind='hist',alpha=0.3,label=date,ax=ax[0,0],title = f"Latest {latest_days} days factor distribution (ylog)")
        ax[0,0].set_yscale('log')
        ax[0,0].legend()
        factor_describe_df['nans_ratio'].plot(ax=ax[0,1],title='nan inf ratio',label='nan ratio')
        factor_describe_df['infs_ratio'].plot(ax=ax[0,1],label='inf ratio')
        ax[0,1].legend()
        factor_describe_df['zeros_ratio'].plot(ax=ax[0,2],title='zero ratio')
        factor_describe_df['mean'].plot(ax=ax[1,0],label='mean')
        factor_describe_df['median'].plot(ax=ax[1,0],label='median')
        ax[1,0].legend()
        factor_describe_df['std'].plot(ax=ax[1,1],title='std')
        factor_describe_df['kurtosis'].plot(ax=ax[1,2],title='kurtosis')
        self.res_figs_dict['00_factor_describe_fig'] = fig
        plt.close()


    def IC_long_excess_return_plot(self):
        IC_all_time_mean = float(self.IC_th_df['RET_t2'].copy().mean())
        IC_th_df = self.IC_th_df.iloc[-250:].copy()
        IC_th_df_ma20 = self.IC_th_df.rolling(20).mean().iloc[-250:].copy()

        long_excess_return_t2_all_time_mean = float(self.long_excess_return_eqW_th_df['RET_t2'].copy().mean())
        long_excess_return_eqW_th_df = self.long_excess_return_eqW_th_df.iloc[-250:].copy()
        # long_excess_return_ffS_th_df = self.long_excess_return_ffS_th_df.iloc[-250:].copy()
        long_excess_return_eqW_th_df_ma20 = self.long_excess_return_eqW_th_df.rolling(20).mean().iloc[-250:].copy()

        # bench_mark_return_t2 = self.benchmark_return_th_df['RET_t2'].iloc[-250:].copy()

        fig,ax = plt.subplots(2,2,figsize=(18,9))

        ax1 = ax[0,0]
        ax2 = ax[1,0]
        ax3 = ax[0,1]
        ax4 = ax[1,1]
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,wspace=0.15,hspace=0.35)

        IC_th_df_t2 = IC_th_df['RET_t2'].copy()
        # IC_th_df_t2_pos = IC_th_df_t2.copy()
        # IC_th_df_t2_pos[IC_th_df_t2_pos < 0] = np.nan
        # IC_th_df_t2_neg = IC_th_df_t2.copy()
        # IC_th_df_t2_neg[IC_th_df_t2_neg > 0] = np.nan
        IC_th_df_t2.cumsum().plot(title='IC_RET_t2 cumsum',ax=ax1)
        IC_th_df_t2.plot(label='IC_th_df_t2',ax=ax2,title='IC_RET_t2 vs ma20')
        # IC_th_df_t2.plot(kind='bar',label='IC_th_df_t2',ax=ax2,title='IC_RET_t2 vs ma20')
        IC_th_df_ma20['RET_t2'].plot(kind='line',label='ma20',ax=ax2)
        # IC_th_df_t2_pos.plot(kind='bar',color='red',label='IC_pos',ax=ax2)
        # IC_th_df_t2_neg.plot(kind='bar',color='green',label='IC_neg',ax=ax2)
        ax2.axhline(y=IC_all_time_mean,alpha=0.5,ls='-',label=f'IC_mean all time:{round(IC_all_time_mean,4)}',color='purple')
        ax2.axhline(y=0,alpha=0.5,color='grey')
        ax2.text(IC_th_df.index[0],IC_all_time_mean,str(round(IC_all_time_mean,4)))
        ax2.legend()

        long_excess_return_eqW_th_df['RET_t2'].cumsum().plot(title='long_excess_return_RET_t2 cumsum',label='eqW',ax=ax3)
        # FF_SIZE
        # long_excess_return_ffS_th_df['RET_t2'].cumsum().plot(label='ffS',ax=ax3)
        
        ax3.legend()
        long_excess_return_eqW_th_df['RET_t2'].plot(title='long_excess_return_RET_t2 vs ma20',label='eqW',ax=ax4)
        # long_excess_return_ffS_th_df['RET_t2'].plot(label='long_excess_return_t2_ffS',ax=ax4)
        long_excess_return_eqW_th_df_ma20['RET_t2'].plot(label='ma20',ax=ax4)
        ax4.axhline(y=long_excess_return_t2_all_time_mean,alpha=0.5,ls='-',label=f'mean all time:{round(long_excess_return_t2_all_time_mean,4)}',color='purple')
        ax4.text(long_excess_return_eqW_th_df.index[0],long_excess_return_t2_all_time_mean,str(round(long_excess_return_t2_all_time_mean,4)))
        ax4.legend()


        self.res_figs_dict['01_IC_long_excess_return_fig'] = fig
        plt.close()

    # 之前的版本目前不用了
    def IC_long_excess_return_plot_oldversion(self):
        '''
        之前的版本目前不用了
        '''
        IC_all_time_mean = float(self.IC_th_df['RET_t2'].copy().mean())
        IC_th_df = self.IC_th_df.iloc[-250:].copy()
        IC_th_df_ma20 = self.IC_th_df.rolling(20).mean().iloc[-250:].copy()
        long_excess_return = self.long_excess_return_eqW_th_df.iloc[-250:].copy()
        bench_mark_return_t2 = self.benchmark_return_th_df['RET_t2'].iloc[-250:].copy()

        fig = plt.figure(constrained_layout=False,figsize=(18,9))
        
        gs = GridSpec(2, 2, figure=fig)

        ax1 = fig.add_subplot(gs[0, 0])
        ax2 = fig.add_subplot(gs[1, 0])
        ax3 = fig.add_subplot(gs[:, 1])
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,wspace=0.15,hspace=0.35)

        IC_th_df['RET_t2'].cumsum().plot(title='IC_RET_t2 cumsum',ax=ax1)
        IC_th_df['RET_t2'].plot(title='IC_RET_t2 vs ma20',label='IC',ax=ax2)
        IC_th_df_ma20['RET_t2'].plot(label='ma20',ax=ax2)
        ax2.axhline(y=IC_all_time_mean,alpha=0.5,ls='-',label=f'IC_mean all time:{round(IC_all_time_mean,4)}',color='purple')
        ax2.text(IC_th_df.index[0],IC_all_time_mean,str(round(IC_all_time_mean,4)))
        ax2.legend()
        long_excess_return.cumsum().plot(title = 'long_excess_return_cumsum vs time horizon \n always long group 10',ax=ax3)
        bench_mark_return_t2.cumsum().plot(ax=ax3,label='stock_pool_average_ret',linewidth=3,alpha=0.4,color='black')
        ax3.legend()
        self.res_figs_dict['01_IC_long_excess_return_fig'] = fig
        plt.close()

    def style_analysis_plot(self):
        style_analysis_res_dict = self.style_analysis_res_dict.copy()
        fig, ax = plt.subplots(2,3,figsize=(18,10))
        ax_list = [x for x0 in ax for x in x0]
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,wspace=0.15,hspace=0.35)
        for (style_name, style_res),ax_i in zip(style_analysis_res_dict.items(),ax_list):
            temp_res = style_res.iloc[-250:].copy()
            temp_res.mean().plot(kind='bar',ax=ax_i,title=f"exposure in style {style_name}") # ,ylim=0.3
            

        self.res_figs_dict['02_style_analysis_fig'] = fig
        plt.close()

    def IC_RETt2_year_cumsum_plot(self):
        IC_RET2 = self.IC_th_df.copy()[['RET_t2']]
        year_list = list(set([str(x)[:4] for x in list(IC_RET2.index)]))
        year_list.sort()
        if len(year_list) >= 6:
            year_list = year_list[-6:]

        fig, ax = plt.subplots(2,3,figsize=(18,10))
        ax_list = [x for x0 in ax for x in x0]
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,wspace=0.15,hspace=0.35)
        fig.suptitle('IC RET_t2 vs year')
        for year,ax_i in zip(year_list,ax_list):
            IC_RET2.loc[year].cumsum().plot(ax = ax_i,title=year)

        self.res_figs_dict['03_IC_RETt2_year_cumsum_fig'] = fig
        plt.close()

    def long_excess_RETt2_year_cumsum_plot(self):
        long_excess_return_t2 = self.long_excess_return_eqW_th_df['RET_t2'].copy()
        year_list = list(set([str(x)[:4] for x in list(long_excess_return_t2.index)]))
        year_list.sort()
        if len(year_list) >= 6:
            year_list = year_list[-6:]

        fig, ax = plt.subplots(2,3,figsize=(18,10))
        ax_list = [x for x0 in ax for x in x0]
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,wspace=0.15,hspace=0.35)
        fig.suptitle('long excess RET_t2 vs year')
        for year,ax_i in zip(year_list,ax_list):
            long_excess_return_t2.loc[year].cumsum().plot(ax = ax_i,title=year)

        self.res_figs_dict['04_long_excess_RETt2_year_cumsum_plot'] = fig
        plt.close()

    def IC_th_cumsum_plot(self):
        fig, ax = plt.subplots(2,1,figsize=(18,10))
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,hspace=0.55)
        IC_t2_all_time_mean = float(self.IC_th_df['RET_t2'].copy().mean())
        self.IC_th_df.cumsum().plot(title='IC_cumsum vs time horizon \n IC mean = '+str(round(IC_t2_all_time_mean,4)),ax=ax[0])
        IC_t2_250days_mean = float(self.IC_th_df['RET_t2'].iloc[-250:].copy().mean())
        self.IC_th_df.iloc[-250:].cumsum().plot(title='IC_cumsum latest 250 days vs time horizon \n IC mean = '+str(round(IC_t2_250days_mean,4)),ax=ax[1])
        self.res_figs_dict['05_0_IC_th_cumsum_fig'] = fig
        plt.close()

    def group_IC_mean_RETt2_year_plot(self):
        group_IC_RETt2_df = self.group_IC_th_dict['RET_t2'].copy()
        year_list = list(set([str(x)[:4] for x in list(group_IC_RETt2_df.index)]))
        year_list.sort()
        if len(year_list) >= 6:
            year_list = year_list[-6:]
        
        fig, ax = plt.subplots(2,3,figsize=(18,10))
        ax_list = [x for x0 in ax for x in x0]
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,wspace=0.15,hspace=0.35)
        fig.suptitle('rank_group_IC_mean_RET_t2 vs year')
        for year,ax_i in zip(year_list,ax_list):
            temp_IC = group_IC_RETt2_df.loc[year].mean()
            temp_IC.plot(kind='bar',ax=ax_i,title=year)

        self.res_figs_dict['05_1_group_IC_mean_RETt2_year_fig'] = fig
        plt.close()

    def IC_th_all_barra_cumsum_plot(self):
        fig, ax = plt.subplots(2,1,figsize=(18,10))
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,hspace=0.55)
        IC_t2_all_time_mean = float(self.IC_th_all_barra_df['RET_t2'].copy().mean())
        self.IC_th_all_barra_df.cumsum().plot(title='IC_cumsum vs time horizon (all barra neutral) \n IC mean = '+str(round(IC_t2_all_time_mean,4)),ax=ax[0])
        IC_t2_250days_mean = float(self.IC_th_all_barra_df['RET_t2'].iloc[-250:].copy().mean())
        self.IC_th_all_barra_df.iloc[-250:].cumsum().plot(title='IC_cumsum latest 250 days vs time horizon (all barra neutral) \n IC mean = '+str(round(IC_t2_250days_mean,4)),ax=ax[1])
        self.res_figs_dict['06_IC_th_all_barra_cumsum_fig'] = fig
        plt.close()

    def IC_IR_year_th_plot(self):
        IC_th_df = self.IC_th_df.copy()
        year_list = list(set([str(x)[:4] for x in list(IC_th_df.index)]))
        year_list.sort()
        if len(year_list) >= 6:
            year_list = year_list[-6:]
        
        fig, ax = plt.subplots(2,3,figsize=(18,10))
        ax_list = [x for x0 in ax for x in x0]
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,wspace=0.15,hspace=0.35)
        fig.suptitle('IC_IR_year vs time horizon')
        for year,ax_i in zip(year_list,ax_list):
            temp_IC = IC_th_df.loc[year]
            temp_IC_IR = pd.DataFrame(temp_IC.mean()/temp_IC.std(),columns=[year+'_IR']).T
            temp_IC_IR = temp_IC_IR*{k:np.sqrt(250/v) for k,v in time_horizon_dict.items()}
            temp_IC_IR.plot(kind='bar',ax=ax_i,title=year)

        self.res_figs_dict['07_IC_IR_year_th_fig'] = fig
        plt.close()

    def IC_mean_year_th_plot(self):
        IC_th_df = self.IC_th_df.copy()
        year_list = list(set([str(x)[:4] for x in list(IC_th_df.index)]))
        year_list.sort()
        if len(year_list) >= 6:
            year_list = year_list[-6:]
        
        fig, ax = plt.subplots(2,3,figsize=(18,10))
        ax_list = [x for x0 in ax for x in x0]
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,wspace=0.15,hspace=0.35)
        fig.suptitle('IC_mean_year vs time horizon')
        for year,ax_i in zip(year_list,ax_list):
            temp_IC = IC_th_df.loc[year].mean()
            temp_IC.plot(kind='bar',ax=ax_i,title=year)

        self.res_figs_dict['08_IC_mean_year_th_fig'] = fig
        plt.close()


    def long_short_return_th_plot(self):
        fig,ax = plt.subplots(figsize=(18,10))
        self.long_short_return_eqW_th_df.cumsum().plot(title='long short return VS time horizon',ax=ax)
        # FF_SIZE
        # self.long_short_return_ffS_th_df[['RET_t1','RET_t2']].set_axis(['RET_t1_ffS','RET_t2_ffS'],axis=1).cumsum().plot(ax=ax)
        
        self.res_figs_dict['09_long_short_return_fig'] = fig
        plt.close()

    def group_annual_excess_return_year_th_plot(self):
        group_excess_return_eqW_th_dict = self.group_excess_return_eqW_th_dict.copy()
        temp_df = group_excess_return_eqW_th_dict[list(group_excess_return_eqW_th_dict.keys())[0]]
        year_list = list(set([str(x)[:4] for x in list(temp_df.index)]))
        year_list.sort()
        if len(year_list) >= 6:
            year_list = year_list[-6:]
        
        fig, ax = plt.subplots(2,3,figsize=(18,10))
        ax_list = [x for x0 in ax for x in x0]
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,wspace=0.15,hspace=0.35)
        fig.suptitle('Group Annual Excess Return vs Year')

        for year,ax_i in zip(year_list,ax_list):
            temp_df=pd.DataFrame({k: v.loc[year].mean()*250 for k,v in group_excess_return_eqW_th_dict.items()})
            temp_df.plot(ax=ax_i,title=year)
        
        self.res_figs_dict['10_group_annual_excess_return_year_th_fig'] = fig
        plt.close()

        
    def group_excess_return_plot(self):
        group_excess_return_eqW_th_dict = self.group_excess_return_eqW_th_dict.copy()

        fig, ax = plt.subplots(2,3,figsize=(18,10))
        ax_list = [x for x0 in ax for x in x0]
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,wspace=0.15,hspace=0.35)
        fig.suptitle('Group Excess Return Cumsum vs time horizon')
        for (ret_th, group_excess_return_tn),ax_i in zip(group_excess_return_eqW_th_dict.items(),ax_list):
            group_excess_return_tn.cumsum().plot(ax=ax_i,title=ret_th)

        self.res_figs_dict['11_group_excess_return_th_fig'] = fig
        plt.close()


    def style_analysis_all_plot(self):
        style_analysis_res_dict = self.style_analysis_res_dict.copy()
        temp_df = style_analysis_res_dict[list(style_analysis_res_dict.keys())[0]]
        year_list = list(set([str(x)[:4] for x in list(temp_df.index)]))
        year_list.sort()
        if len(year_list) >= 6:
            year_list = year_list[-6:]
        n=0
        for style_name,style_res in style_analysis_res_dict.items():
            fig, ax = plt.subplots(2,3,figsize=(18,10))
            plt.subplots_adjust(left=None,bottom=None,right=None,top=None,wspace=0.15,hspace=0.35)
            fig.suptitle(f"exposure in style {style_name}")
            ax_list = [x for x0 in ax for x in x0]

            for year,ax_i in zip(year_list,ax_list):
                style_res.loc[year].mean().plot(kind='bar',title=year,ax=ax_i) # ,ylim=0.3


            self.res_figs_dict[f'12_0{n}_style_analysis_{style_name}_fig'] = fig
            plt.close()
            n += 1

    def factor_pool_corr_plot(self):
        abs_corr_mean_se = abs(self.factor_pool_corr_df.copy().mean())
        corr_pos_nag_TF = 2*(self.factor_pool_corr_df.mean()>0)-1
        factor_corr_mean_dir_df = pd.concat([abs_corr_mean_se,corr_pos_nag_TF],axis=1).set_axis(['corr_abs','corr_dir'],axis=1)
        factor_corr_mean_dir_df = factor_corr_mean_dir_df.sort_values(by='corr_abs',ascending=False)
        if factor_corr_mean_dir_df.shape[0]>20:
            # factor_corr_mean_dir_df = pd.concat([factor_corr_mean_dir_df.head(10),factor_corr_mean_dir_df.tail(10)])
            factor_corr_mean_dir_df = factor_corr_mean_dir_df.head(20)
        # seaborn.set(font_scale=2)
        fig, ax = plt.subplots(figsize=(18,20))
        plt.title("corr with factor pool head20")
        seaborn.heatmap(factor_corr_mean_dir_df,fmt='.4f',annot=True,cmap='vlag',yticklabels=True,cbar=False,annot_kws={"fontsize":18})
        plt.yticks(rotation=0,fontsize=20)
        ax.xaxis.tick_top() # x axis on top
        ax.xaxis.set_label_position('top')
        plt.tight_layout()
        # plt.savefig(path)
        self.res_figs_dict['13_factor_corr_mean_dir_fig'] = fig
        plt.close()

    def factor_pool_corr_table_plot(self):
        abs_corr_mean_se = abs(round(self.factor_pool_corr_df.copy().mean(),4))
        corr_pos_nag_TF = 2*(self.factor_pool_corr_df.mean()>0)-1
        factor_corr_mean_dir_df = pd.concat([abs_corr_mean_se,corr_pos_nag_TF],axis=1).set_axis(['corr_abs','corr_dir'],axis=1)
        factor_corr_mean_dir_df = factor_corr_mean_dir_df.sort_values(by='corr_abs',ascending=False)
        seaborn.set(font_scale=2)
        fig, ax = plt.subplots(figsize=(18,20))
        plt.title("corr with factor pool head10 & tail10")
        table_df = ax.table(
            cellText=factor_corr_mean_dir_df.values,
            rowLabels=factor_corr_mean_dir_df.index,
            colLabels=factor_corr_mean_dir_df.columns,
            cellLoc="center",
            colWidths=[0.04, 0.02, 0.02, 0.02, 0.02, 0.02],
            loc=(0, 0)
        )
        table_df.set_fontsize(55)
        table_df.scale(9.5, 4.2)
        plt.axis("off")
        plt.tight_layout()
        # plt.savefig(path)
        self.res_figs_dict['14_factor_corr_mean_dir_table_fig'] = fig
        plt.close()
        




    def factor_return_RETt2_year_cumsum_plot(self):
        factor_return_RET2 = self.factor_return_th_df.copy()[['RET_t2']]
        year_list = list(set([str(x)[:4] for x in list(factor_return_RET2.index)]))
        year_list.sort()
        if len(year_list) >= 6:
            year_list = year_list[-6:]

        fig, ax = plt.subplots(2,3,figsize=(18,10))
        ax_list = [x for x0 in ax for x in x0]
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,wspace=0.15,hspace=0.35)
        fig.suptitle('factor_return RET_t2 vs year')
        for year,ax_i in zip(year_list,ax_list):
            factor_return_RET2.loc[year].cumsum().plot(ax = ax_i,title=year)

        self.res_figs_dict['15_factor_return_RETt2_year_cumsum_fig'] = fig
        plt.close()

    def factor_return_th_cumsum_plot(self):
        factor_return_th_df = self.factor_return_th_df.copy()

        fig, ax = plt.subplots(2,1,figsize=(18,10))
        factor_return_th_df.cumsum().plot(title='factor_return_cumsum vs time horizon',ax=ax[0])
        factor_return_th_df.iloc[-250:].cumsum().plot(title='factor_return_cumsum latest 250 days vs time horizon',ax=ax[1])
        self.res_figs_dict['16_factor_return_th_cumsum_fig'] = fig
        plt.close()

    def factor_return_IR_year_th_plot(self):
        factor_return_th_df = self.factor_return_th_df.copy()
        year_list = list(set([str(x)[:4] for x in list(factor_return_th_df.index)]))
        year_list.sort()
        if len(year_list) >= 6:
            year_list = year_list[-6:]
        
        fig, ax = plt.subplots(2,3,figsize=(18,10))
        ax_list = [x for x0 in ax for x in x0]
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,wspace=0.15,hspace=0.35)
        fig.suptitle('factor_return_IR_year vs time horizon')
        for year,ax_i in zip(year_list,ax_list):
            temp_factor_return = factor_return_th_df.loc[year]
            temp_factor_return_IR = pd.DataFrame(temp_factor_return.mean()/temp_factor_return.std(),columns=[year+'_IR']).T
            temp_factor_return_IR = temp_factor_return_IR*{k:np.sqrt(250/v) for k,v in time_horizon_dict.items()}
            temp_factor_return_IR.plot(kind='bar',ax=ax_i,title=year)

        self.res_figs_dict['17_factor_return_IR_year_th_fig'] = fig
        plt.close()

    def factor_return_mean_year_th_plot(self):
        factor_return_th_df = self.factor_return_th_df.copy()
        year_list = list(set([str(x)[:4] for x in list(factor_return_th_df.index)]))
        year_list.sort()
        if len(year_list) >= 6:
            year_list = year_list[-6:]
        
        fig, ax = plt.subplots(2,3,figsize=(18,10))
        ax_list = [x for x0 in ax for x in x0]
        plt.subplots_adjust(left=None,bottom=None,right=None,top=None,wspace=0.15,hspace=0.35)
        fig.suptitle('factor_return_mean_year vs time horizon')
        for year,ax_i in zip(year_list,ax_list):
            temp_factor_return = factor_return_th_df.loc[year].mean()
            temp_factor_return.plot(kind='bar',ax=ax_i,title=year)

        self.res_figs_dict['18_factor_return_mean_year_th_fig'] = fig
        plt.close()

    def industry_analysis_plot(self):
        # only plot long and short group
        group_list = list(self.industry_analysis_res_dict.keys())
        # group_list.sort()
        industry_long_group = self.industry_analysis_res_dict[group_list[-1]].copy().tail(60)
        industry_short_group = self.industry_analysis_res_dict[group_list[0]].copy().tail(60)
        fig, ax =plt.subplots(1,2,figsize=(18,10))
        industry_long_group.mean().sort_values(ascending=True).plot(kind='barh',ax=ax[0],title='recent 60days long group')
        industry_short_group.mean().sort_values(ascending=False).plot(kind='barh',ax=ax[1],title='recent 60days short group')
        self.res_figs_dict['16_industry_analysis_fig'] = fig
        plt.close()

        industry_long_short_group = industry_long_group - industry_short_group
        fig, ax = plt.subplots(figsize=(18,10))
        industry_long_short_group.mean().sort_values(ascending=True).plot(kind='barh',ax=ax,title='recent 60days long short group')
        self.res_figs_dict['19_industry_long_short_analysis_fig'] = fig

        



def plotFactorAnalysisRes(config_path):
    with open(config_path) as f:
        config = yaml.load(f.read(),Loader=yaml.FullLoader)
    
    # if (config['factor_name_list_mat'] is None) and (config['factor_name_list_pkl'] is None):
    #     return ("factor_name_list is None, plz check")
    # elif config['factor_name_list_mat'] is None:
    #     factor_name_list_plot = config['factor_name_list_pkl']
    # elif config['factor_name_list_pkl'] is None:
    #     factor_name_list_plot = config['factor_name_list_mat']
    # else:
    #     factor_name_list_plot = config['factor_name_list_mat']+config['factor_name_list_pkl']
    if (config['factor_basename_list'] is None):
        return ("factor_basename_list is None, plz check")
    factor_name_list_plot = [x.split(".")[0] for x in config['factor_basename_list']]
    for factor_name in factor_name_list_plot:
        if factor_name is not None:
            config_c = copy.deepcopy(config)
            config_c['factor_name'] = factor_name
            fAnaResPlot = FactorAnalysisResPlot(config_c)
            fAnaResPlot.run_plot_method()
            fAnaResPlot.save_fig2pdf()
            print(f"========= plotFactorAnalysisRes {factor_name} successfully ==============")
