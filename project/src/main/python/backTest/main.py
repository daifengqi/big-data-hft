from BackTest import updateFactorAnalysisRes
from factorAnalysis.factorAnalysisResPlot import plotFactorAnalysisRes
# import sys
import time

config_path = r"D:\02PHBS_G2\PHBS_m2\bigDataAnalysis\big-data-hft\project\src\main\python\backTest\factorAnalysis\config_factorAnalysis.yaml"



# updateSP = UpdateStockPool(update_stock_pool_config_path)
# updateSP.update_stock_info()
# updateSP.update_stock_pool_basic()
# updateSP.update_stock_HS300()
# updateSP.update_stock_ZZ500()
# updateSP.update_stock_ZZ800()

# stock_pool_name = 'STOCK_POOL_basic'
# stock_pool_path = f"E:\\data\\interday\\{stock_pool_name}.pkl"
# update_benchmark_return(stock_pool_path=stock_pool_path,stock_pool_name=stock_pool_name,benchmark_ret_save_dir = r"E:\data\interday")


# # factor analysis
updateFactorAnalysisRes(config_path)
plotFactorAnalysisRes(config_path)

