import datacompy, pandas as pd
import xlsxwriter


df1 = pd.read_csv('/Users/omerorhan/Documents/EventDetection/regression_server/1000/all_event_local_new_1000.csv')
df2 = pd.read_csv('/Users/omerorhan/Documents/EventDetection/regression_server/1000/all_event_local_old_1000.csv')


compare = datacompy.Compare(
    df1,
    df2,
    join_columns='phone_manipulation_count',  #You can also specify a list of columns eg ['policyID','statecode']
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

df = pd.DataFrame({'A': ['a','a'], 'B': ['b','b']}, index=[0,1])
print(df)