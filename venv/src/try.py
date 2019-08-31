import datacompy, pandas as pd
import xlsxwriter


df1 = pd.read_csv('/Users/omerorhan/Documents/EventDetection/regression_server/backup/115 examples2/all_event_local_0829_with_changes.csv')
df2 = pd.read_csv('/Users/omerorhan/Documents/EventDetection/regression_server/backup/115 examples2/all_event_local.csv')


compare = datacompy.Compare(
    df1,
    df2,
    join_columns='start_time',  #You can also specify a list of columns eg ['policyID','statecode']
    abs_tol=0, #Optional, defaults to 0
    rel_tol=0, #Optional, defaults to 0
    df1_name='Original', #Optional, defaults to 'df1'
    df2_name='New' #Optional, defaults to 'df2'
)

'''
workbook = xlsxwriter.Workbook('/Users/omerorhan/Documents/EventDetection/regression_server/report.xlsx')
worksheet = workbook.add_worksheet()
# Some data we want to write to the worksheet.
expenses = (
    ['Rent', 1000],
    ['Gas',   100],
    ['Food',  300],
    ['Gym',    50],
)
row = 0
col = 0

# Iterate over the data and write it out row by row.
for item, cost in (expenses):
    worksheet.write(row, col,     item)
    worksheet.write(row, col + 1, cost)
    row += 1

workbook.close()
'''
print((compare.report()))
