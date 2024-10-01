path1=r"C:\Users\RA373432\Downloads\tmp\Output_demo-addr-add_L47D1qXIaSyZPaE1pu1lJo7XBetF5gIR2019-04-09 19-14-27.291000.xlsx"
path2=r"C:\Users\RA373432\Downloads\tmp\Output_demo-addr-add_gAL47D1qXIaSyZPaE1pu1lJo7XBetF5g2019-04-10 11-30-51.427000.xlsx"

import pandas as pd
from bin import utils

df1 = utils.renameColsTovalidNames(pd.read_excel(path1)).fillna("")
df2 = utils.renameColsTovalidNames(pd.read_excel(path2)).fillna("")
difference = df1==df2
set([val for arr in list(difference.values) for val in list(arr)])