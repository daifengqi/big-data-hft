# from updateStockPool import UpdateStockPool,update_benchmark_return
from factorAnalysisTimeHorizonSimple import updateFactorAnalysisResSimple
# from factorAnalysisResPlot import plotFactorAnalysisRes
# import sys
import time
import warnings
warnings.filterwarnings("ignore")
factor_analysis_config_path = r"D:\HX_proj\factorAnalysis\config_factorAnalysisSimple.yaml"

# # factor analysis
updateFactorAnalysisResSimple(factor_analysis_config_path)