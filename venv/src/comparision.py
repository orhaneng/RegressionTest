import datacompy, pandas as pd
import xlsxwriter
import sys
from versioningfiles import VersionFile
import os


def compareTrips(path, poolsize, version):
    VersionFile(path + "reports/" + poolsize.value + "/", ".xlsx")
    filepath =path + "reports/" + poolsize.value + "/regression_report" + version + ".xlsx"
    writer = pd.ExcelWriter(filepath,
                            engine='xlsxwriter')
    df1 = pd.read_csv(path + "tripresults/maintripresult/" + poolsize.value + "/" + checkfolder(
        path + "tripresults/maintripresult/" + poolsize.value), index_col=False)
    df1.drop(df1.columns[1], axis=1, inplace=True)
    df2 = pd.read_csv(
        path + "tripresults/" + poolsize.value + "/" + checkfolder(path + "tripresults/" + poolsize.value),
        index_col=False)
    df2.drop(df2.columns[1], axis=1, inplace=True)
    compare = datacompy.Compare(
        df1,
        df2,
        join_columns='start_time',  # You can also specify a list of columns eg ['policyID','statecode']
        abs_tol=0,  # Optional, defaults to 0
        rel_tol=0,  # Optional, defaults to 0
        df1_name='Original',  # Optional, defaults to 'df1'
        df2_name='New'  # Optional, defaults to 'df2'
    )
    compare.report(writer, sys.maxsize)
    writer.save()
    return filepath


def checkfolder(path):
    filenames = os.listdir(path)
    count = 0
    fname = ""
    for filename in filenames:
        if filename.endswith(".csv"):
            count = count + 1
            fname = filename
    if count > 1:
        print("ERROR! be allowed only one csv file under " + path)
        exit()
    return fname
