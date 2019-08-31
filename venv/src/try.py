import datacompy, pandas as pd
import xlsxwriter
import sys
df1 = pd.read_csv(
    '/Users/omerorhan/Documents/EventDetection/regression_server/1000/all_event_local_new_1000.csv')
df1.drop(df1.columns[0], axis=1, inplace=True)
df2 = pd.read_csv('/Users/omerorhan/Documents/EventDetection/regression_server/1000/all_event_local_old_1000.csv')
df2.drop(df2.columns[0], axis=1, inplace=True)

compare = datacompy.Compare(
    df1,
    df2,
    join_columns='start_time',  # You can also specify a list of columns eg ['policyID','statecode']
    abs_tol=0,  # Optional, defaults to 0
    rel_tol=0,  # Optional, defaults to 0
    df1_name='Original',  # Optional, defaults to 'df1'
    df2_name='New'  # Optional, defaults to 'df2'
)
print((compare.report("/Users/omerorhan/Documents/EventDetection/regression_server/report.xlsx" ,10)))
