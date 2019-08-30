import datacompy, pandas as pd
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


print((compare.report()))