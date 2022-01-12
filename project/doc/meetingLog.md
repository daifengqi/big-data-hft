## 2022-01-12

整理数据的步骤：

1. 修改hook/Driver的`file`为对应的`文件夹/csv文件名`，比如`201901/20190102.csv`，注意要在HDFS上查看，是有这个文件的（即跳过非交易日）
2. run hook/Driver，成功后，再 run hook/Fetch，取到第一次mapreduce的结果
3. 将 `big-data-hft/tickKeyCount/20190102.csv/part-00000`，重命名为`20190102.csv`，放到`big-data-hft/project/src/main/resources/`文件夹里
4. 修改 main/Driver 里的 `file` 为同样的名字 `20190102.cs`
5. run main/Driver，成功后，再 run main/Fetch，取到第二次mapreduce的结果
6. 结果在`big-data-hft/tickDataOutput` 文件夹里，同样重命名为csv文件就可以使用了，这就final的文件。



## 2022-01-10

TODO:

1. `Java`，MapReduce 的逻辑修改，在Mapper里判断出open，close，high，low的价格
2. `Java`，main.Driver 的 setInputPaths 划分成文件的维度，预期输出也是多个文件
3. `Python`，Reduce 之后的数据预处理



## 2022-01-08

proj内容分工--T1的10：00~T2的10：00预测

Q：老师说可以每分钟都做，很奇怪？

- 数据预处理：java写，整合成1min的bar（7哥+wzj）--1月12日之前

  - res: 每一天每一只股票会有一个df, pickle h5 feather形式，实在不行就csv

    - **multiIndex (timestamp,stock_code)**

    | timestamp | open | high | low  | close | vwap | volume | amount | buy_count | sell_count | ...  |
    | --------- | ---- | ---- | ---- | ----- | ---- | ------ | ------ | --------- | ---------- | ---- |
    | 09:30:00  |      |      |      |       |      |        |        |           |            |      |
    | 09:31:00  |      |      |      |       |      |        |        |           |            |      |
    |           |      |      |      |       |      |        |        |           |            |      |

    

  - 初步的特征构建（**盘口压力差等特征计算**）

  - **买卖单数据融合**的逻辑

  - mapreduce的操作

  - 存到本地即可（60天1min的数据应该不会很大）

- filesync数据搜集（wzj）--1月12日之前

  - sql连接filesync
  - 每日收盘价、开盘价（adj的！！！）每个特征都是一个df
  - 市值、（barra风险因子的一些数据无所谓）一个df（date x stock）

  | datetime | 000001. |      |
  | -------- | ------- | ---- |
  | 20220104 |         |      |
  | 20220105 |         |      |
  | .。。    |         |      |

- 数据分析、因子构建（ymj+gxr）python--思路 1月15日之前把因子算好

  - 分钟k线进行日度因子的构造

- 回测框架、可视化展示（ymj）-15和16日整体的整合，ppt大致内容

- ppt制作、report与GitHub整理（pww+qy）16日-**并行！！！**