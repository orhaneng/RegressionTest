import datacompy, pandas as pd
import xlsxwriter
import sys

def compareTrips(path,):
    writer = pd.ExcelWriter(path+"reports/regression_report.xlsx", engine='xlsxwriter')
    df1 = pd.read_csv(path+"/tripresults/maintripresult/trip_results.csv")
    df1.drop(df1.columns[0], axis=1, inplace=True)
    df2 = pd.read_csv(path+"/tripresults/trip_results.csv")
    df2.drop(df2.columns[0], axis=1, inplace=True)
    compare = datacompy.Compare(
        df1,
        df2,
        join_columns='s3_key',  # You can also specify a list of columns eg ['policyID','statecode']
        abs_tol=0,  # Optional, defaults to 0
        rel_tol=0,  # Optional, defaults to 0
        df1_name='Original',  # Optional, defaults to 'df1'
        df2_name='New'  # Optional, defaults to 'df2'
    )
    compare.report(writer, sys.maxsize)
    writer.save()
