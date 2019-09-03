import datacompy, pandas as pd
import xlsxwriter
import sys

writer = pd.ExcelWriter("/Users/omerorhan/Documents/EventDetection/regression_server/report.xlsx", engine='xlsxwriter')

df1 = pd.read_csv(
    '/Users/omerorhan/Documents/EventDetection/regression_server/all_event_local_1000_new.csv')
df1.drop(df1.columns[0], axis=1, inplace=True)
df2 = pd.read_csv('/Users/omerorhan/Documents/EventDetection/regression_server/all_event_local_1000_old.csv')
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
((compare.report(writer, 10)))

writer.save()
