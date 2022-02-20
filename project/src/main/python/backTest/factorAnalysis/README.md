[toc]

# factorAnalysis使用说明（new）

## 环境

python 3.7 （建议`conda create -n env_name python=3.7 `）

pandas版本 1.1.5（不然可能打不开pkl文件）

## 需要的数据

- STOCK_POOL_basic.pkl
- SIZE.mat（其他的intraday.mat文件都可以，主要是为了code和date对齐）
- BENCHMARK_RETURN_basic.pkl

## 文档

### main.py

```python
factor_analysis_config_path = r"D:\HX_proj\factorTest\factorAnalysisConfig.yaml"
```

配置文件地址

### factorAnalysisConfig.yaml

```yaml
start_date: 20170101
end_date: 20210801
corr_analysis: False

# need config
factor_analysis_res_dir: D:\HX_proj\factorAnalysis\res\00_factor_analysis_res
# factor_analysis_res_dir: D:\HX_proj\factorTest\draft\076\factor_analysis_res_dir
factor_analysis_res_plot_dir: D:\HX_proj\factorAnalysis\res\01_factor_analysis_plot
utility_data_dir: E:\data\interday

# stock_pool_file_name: STOCK_POOL_ZZ500
stock_pool_file_name: STOCK_POOL_basic # .pkl
STOCK_CODE_ref_file_name: SIZE # .mat
benchmark_return_file_name: BENCHMARK_RETURN_basic # .pkl
```

start_date，end_date：设定需要回测的时间区间（闭区间）

- 第一次全量计算之后，下次只需要进行增量的部分即可

factor_analysis_res_dir和factor_analysis_res_plot_dir是计算结果存储的文件夹（前者存数据，后者存图片）

- 注意：如果想要之后都只计算增量的话，factor_analysis_res_dir不要变，每次增量计算结果会直接append到上次的结果上
- factor_analysis_res_plot_dir也不需要变，这个文件夹里面，会有以feature_name为名的子文件夹，每个feature_name文件夹下会有不同时间运行factorAnalysis的report（后缀是所选用的**股票池+日期**，如vol_bcvp_t_l1_**basic_20210619**.pdf）

utility_data_dir：存放着interday的那些数据的文件夹

stock_pool_file_name：目前会有四种股票池可供选择

- STOCK_POOL_basic：剔除当天ST，非交易日，上市未满一年，当天与第二天涨跌停的
- STOCK_POOL_cap_vol_drop20：在basic基础上，剔除FF_CAPITAL_ma20与ADJVOLUME_ma20后20%
- STOCK_POOL_HS300：在basic基础上，选取是沪深300的股票
- STOCK_POOL_ZZ500：在basic基础上，选取是中证500的股票

STOCK_CODE_ref_file_name：用于拿出stock_code和date_list的，把所有data都对齐，只要是HX提供的intraday的.mat文件就行

factor_dir：factor存储的文件夹

factor_pool_dir：factor pool储存的文件夹（文件是.pkl或者.mat，可以先不用，计算和原有factor的corr的，较为耗时间，corr_analysis为False）

factor_name_list：待测的factor的名字（目前支持HX的.pkl和.mat文件）

# factorTest使用说明（old）

拷贝压缩文件factorTest.rar

可以直接拷贝STOCK_POOL的数据，那样就只需要输入factor的pkl或者mat文件就好（此时需要py3.7，pd1.1.5）

## 需要修改的地方

### main.py

```yaml
update_stock_pool_config_path = r"D:\HX_proj\factorTest\updateStockPoolConfig.yaml"
factor_analysis_config_path = r"D:\HX_proj\factorTest\factorAnalysisConfig.yaml"
```

修改成两个配置文件的绝对地址

### updateStockPoolConfig.yaml

```yaml
start_date: 20150101
end_date: 20210530

utility_data_dir: E:\data\interday
```

start_date，end_date：设定需要更新的股票池的时间区间（闭区间）

- 第一次全量计算之后，下次只需要进行增量的部分即可

utility_data_dir：存放着interday的那些数据的文件夹，stock pool也会更新到这个文件夹里面（方便后面factorAnalysis使用）

### factorAnalysisConfig.yaml

```yaml
start_date: 20160104
end_date: 20210610

# need config
factor_analysis_res_dir: D:\HX_proj\factorTest\draft\0619\factor_analysis_res_dir
factor_analysis_res_plot_dir: D:\HX_proj\factorTest\draft\0619\factor_analysis_res_plot_dir
utility_data_dir: E:\data\interday
stock_pool_file_name: STOCK_POOL_basic

factor_dir: E:\data\feature
factor_pool_dir: E:\data\feature_old

factor_name_list: # .mat
 - vol_bcvp_t_l1
 - xxxx
```

start_date，end_date：设定需要回测的时间区间（闭区间）

- 第一次全量计算之后，下次只需要进行增量的部分即可

factor_analysis_res_dir和factor_analysis_res_plot_dir是计算结果存储的文件夹（前者存数据，后者存图片）

- 注意：如果想要之后都只计算增量的话，factor_analysis_res_dir不要变，每次增量计算结果会直接append到上次的结果上
- factor_analysis_res_plot_dir也不需要变，这个文件夹里面，会有以feature_name为名的子文件夹，每个feature_name文件夹下会有不同时间运行factorAnalysis的report（后缀是所选用的**股票池+日期**，如vol_bcvp_t_l1_**basic_20210619**.pdf）

utility_data_dir：存放着interday的那些数据的文件夹

stock_pool_file_name：目前会有四种股票池可供选择

- STOCK_POOL_basic：剔除当天ST，非交易日，上市未满一年，当天与第二天涨跌停的
- STOCK_POOL_cap_vol_drop20：在basic基础上，剔除FF_CAPITAL_ma20与ADJVOLUME_ma20后20%
- STOCK_POOL_HS300：在basic基础上，选取是沪深300的股票
- STOCK_POOL_ZZ500：在basic基础上，选取是中证500的股票

factor_dir：factor存储的文件夹

factor_pool_dir：factor pool储存的文件夹（文件是.pkl或者.mat）

factor_name_list：待测的factor的名字

# 更新20210804

- ZX_1.mat数据里面STOCK_CODE和其他的mat文件没有对齐，updateStockPool.LoadStockInfo里面使用SIZE.mat作为ref，按照他的date和stock_code对于之后load进来的df进行reindex后取values，再用numpy处理
- factorAnalysisTimeHorizon.load_stock_code_cols的输入改为STOCK_CODE_ref_path（和上面保持一致取SIZE.mat文件的STOCK_CODE作为参考）
- 对于读取进来的FACTOR，要和stock_code_cols对齐之前的操作是`FACTOR[stock_code_cols]`现在改为`FACTOR.reindex(stock_code_cols)`，保证Factor里面没有的stock自动填充成Nan

# 更新20210812

- 把`factorAnalysisTimeHorizon.py`里面的函数拆分到了`factorAnalysisIOTools.py`和`factorAnalysisCalTools.py 	`中，前者包含`load_sth`的method，后者是进行factor analysis的时候计算相关的method
- 在`updateStockPool.py`中添加了ZZ800的stockPool（HS300+ZZ500），stockPool数据里新增加流通市值FF_SIZE（FF_CAPITAL*VWAP）
- 在`factorAnalysisTimeHorizon.py`里增加了**分层回测的时候组内流通市值加权的方式**，目前可以计算但是已经注释掉了（搜索ffS把注释取消，记得`factorAnalysisResPlot.py`里面也看一下）
- 在`factorAnalysisTimeHorizon.py`里添加了功能**分析feature和factor pool中的feature_old的corr**，目前是每个周三抽样进行股票池内两个feature的corr的计算，在plot的时候选出corr_abs最大的前20个进行展示
- 在`factorAnalysisConfig.yaml`中`start_date`必须要**大于20160701**，因为factor Pool里面的feature_old是从20160701开始的

# 更新20210815

- 在preprocessing的时候init里面加一个copy（）（否则会改变input的factor导致后面计算corr的时候有些许不一样）

# 更新20210917

- **是否考虑次日涨跌停**

  - 之前是考虑的，计算icir的时候会有区别（会直接把涨跌停的剔除掉）
  - 对冲的时候也会有区别（实际上用来对冲的部分全市场涨跌停是需要考虑的）
- 算涨跌停的时候，20200824开始创业板涨幅停调整到了20%，可以照着这个逻辑改下 ，创业板通过stock_code前3位等于300识别； 还有科创板，前3位是688； 科创板自上市以来就是20%涨跌幅，创业板是20200824开始改20%的

  ![image-20210917111529024](README.assets/image-20210917111529024.png)
  ![image-20210917111534907](README.assets/image-20210917111534907.png)
- 基准和多头 空头计算参考这个原则
  ![image-20210917111555865](README.assets/image-20210917111555865.png)
- 全市场等权基准：

  - 当日涨跌停，ST，上市不满一年
  - 构造STOCK_POOL的时候不考虑t1的LIMIT_UD
  - 后续利用LIMIT_UD_t1计算出lmt_filter_arr（STOCK_POOL当天的sec_code里面t1涨跌停的设为nan）
- 多头选择原则：

  - 考虑t1的涨跌停不可交易（目前是t0预测，t1买入，t2卖出，所以要看t1能不能买入，t2的涨跌停不需要考虑，因为已经买入了）
  - 这里在group_weight_method里多增加一个参数lmt_filter_arr：假设stock_pool一共100个股票，group10根据之前选出来有10只股票，但是在实际情况下，t1可能只有8只可以买入，这时候利用lmt_filter_arr把权重从【10个股票每个1/10变成8个股票每个1/8】
  - 在stock_pool里面，如果因子标准化过后，有nan的数值，以0填充
- stock_pool 变化

  - 20210510
    - 300222.SZ：0511 涨11% 纳入新的stock_pool（现在不考虑次日涨停，且涨停按19.95%算）
    - 300086.SZ：0510跌10.多%，纳入新的stock_pool（跌停按19.95%算）
    - 300430.SZ：0510涨15%，纳入新的stock_pool（涨停按19.95%）
    - 600872.SZ：0511涨停9.99%，纳入新的stock_pool（现在不考虑次日涨停）
    - 688068.SH：0511跌9.99%（19.95%才算跌停，且不看次日），纳入新的stock_pool

# 更新20210922

- RETURN的更新方式
  - 用于计算回测收益的时候，按照之前写的`prepare_RET_dict`
  - 用于判断是否涨跌停的时候（构造STOCK_POOL的时候），利用下一期的前半小时VWAP和前一天的收盘价PRECLOSE` FCT_HF_TWAP_Q1/PRECLOSE-1`，只要判断前半小时是否涨跌停就好
    - 差的还是有点大的，感觉会放松很多，T1可以在开盘30min购买到
- 基准收益单独计算
  - 利用不同的STOCK_POOL的pkl文件直接写一个函数计算这段时间，某一个频率的return的情况
  - STOCK_POOL的t0和t1的涨跌停都不剔除，只有ST和上市未满250天剔除
  - 在STOCK_POOL的df上加入一列`LIMIT_UD_filter_T0_T1`：当T0和T1都不是涨跌停的时候取1，其他取0（逻辑表达式：这两列加起来的和==0）
- 计算RET_t5
  - 方法一：直接`RET_t5 = ADJVWAP.shift(-6)/ADJVWAP.shift(-1)-1`之后平均每日的拿`RET_t5/5`
  - 方法二：计算RET_t5之后，`RET_t5.rolling(5,min_periods=1).mean()/5`
  - 按结果的图像，基本没啥区别？？？
    方法一：![image-20210922105516100](README.assets/image-20210922105516100.png)
    方法二：![image-20210922105440979](README.assets/image-20210922105440979.png)

# 更新20210925

对比20210815的版本，以下为最新版本的更改情况

- 测试的benchmark return单独根据各种STOCK_POOL计算，目前使用的STOCK_POOL_basic是只剔除了ST和上市未满250个交易日的，没有考虑是否涨跌停（作为benchmark本身就不需要考虑）使用`factorAnalysis\updateStockPool.py\update_benchmark_return`在更新STOCK_POOL的时候同步更新
- STOCK_POOL的涨跌停信息：
  - LIMIT_UD_t0：利用RETURN.mat文件（close/preclose计算得来，preclose是考虑了复权因子的）
  - LIMIT_UD_t1：利用`FCT_HF_TWAP_Q1/PRECLOSE-1`得到的RETURN_Q1.mat文件，开盘半小时的TVWAP和preclose算的
  - LIMIT_UD_filter_t0_t1：t0和t1都不是涨跌停的情况为1
- 在回测单因子时：目前使用basic的STOCK_POOL
- 因子预处理时：ZScore之后，将inf和nan都填补成0
- 因子计算IC时：ret也相对应的进行预处理，并且剔除T0和T1涨跌停的股票，计算的时normalIC
- 因子分组时：分组前剔除T0涨跌停的（防止因为交易数据分布问题影响因子的计算结果），分组后，计算各组收益时，剔除T1涨跌停的
  - 例如：long_group中原本有10只股票，发现2只在T1是涨跌停了，则构建long_portfolio的时候只保留8只，等权重1/8

# 更新20211007

- STOCK_POOL的涨跌停信息：
  - LIMIT_UD_t0：利用RETURN.mat文件（close/preclose计算得来，preclose是考虑了复权因子的），对比10%或者20%
  - LIMIT_UD_t1：利用`FCT_HF_TWAP_H1/PRECLOSE-1`得到的RETURN_H1.mat文件，开盘半小时的TVWAP和preclose算的，但是他对比的是：**按照涨跌停规则（300开头的创业板20200824及以后20%，688开头的科创板一直20%，其余情况10%）计算`ADJOPEN*(limit_ratio/2+2*limit_ratio)/3=ADJOPEN*limit_ratio*5/6=LIMIT_PRICE_THRESHOLD_H1`**与`FCT_HF_TWAP_H1`，如果`LIMIT_PRICE_THRESHOLD_H1 > FCT_HF_TWAP_H1`则LIMIT_UD_t1=0，否则是1
    - FCT_HF_TWAP_H1/ADJOPEN > 5/6*limit_ratio
  - LIMIT_UD_filter_t0_t1：t0和t1都不是涨跌停的情况为1

# 更新20211011

- factorAnalysisTimeHorizon中，理应输入factor 的pkl和mat文件以及STOCK_POOL即可，所有需要的数据都从STOCK_POOL里面拿就行
  - AMOUNT
  - CLOSE
  - TODO：？？？sec_code和trade_list要不要直接拿？

# 更新20211014

- 对于全市场大幅下跌的情况（20200123和20200203）pool经过filter_t1_t0筛选后会只有400不到的股票（很不合理）这时候测试我们直接把group_ret设置为bench_ret，记他的超额为0
- 算是final version，后续可以增加多头IC等指标



