import datacompy, pandas as pd
import xlsxwriter
import sys
from versioningfiles import VersionFile
import os
import numpy as np
import datetime

def compareTrips(path, poolsize, version):
    VersionFile(path + "reports/" + poolsize + "/", ".xlsx")


    filepath = path + "reports/" + poolsize + "/regression_report" + version + ".xlsx"
    writer = pd.ExcelWriter(filepath,
                            engine='xlsxwriter')
    df1 = pd.read_csv(path + "tripresults/maintripresult/" + poolsize + "/" + checkfolder(
        path + "tripresults/maintripresult/" + poolsize), index_col=False)
    df1.drop(df1.columns[1], axis=1, inplace=True)
    df2 = pd.read_csv(
        path + "tripresults/" + poolsize + "/" + checkfolder(path + "tripresults/" + poolsize),
        index_col=False)
    df2.drop(df2.columns[1], axis=1, inplace=True)
    compare = datacompy.Compare(
        df1,
        df2,
        join_columns='s3_key',  # You can also specify a list of columns eg ['policyID','statecode']
        abs_tol=0,  # Optional, defaults to 0
        rel_tol=0,  # Optional, defaults to 0
        df1_name='Original',  # Optional, defaults to 'df1'
        df2_name='New'  # Optional, defaults to 'df2'
    )

    versions = pd.DataFrame({"Version Type": ["Base Version", "New Version","Report Created Time"], "Version": [checkfolder(
        path + "tripresults/maintripresult/" + poolsize),checkfolder(path + "tripresults/" + poolsize),str(datetime.datetime.now())]})
    versions.to_excel(writer, sheet_name='Summary', startrow=1, startcol=1)

    compare.report(writer, sys.maxsize)
    driverScoreComparision(writer, df1, df2)
    writer.save()

    return filepath


def highlight_diff(data, color='yellow'):
    attr = 'background-color: {}'.format(color)
    other = data.xs('Old', axis='columns', level=-1)
    return pd.DataFrame(np.where(data.ne(other, level=0), attr, ''),
                        index=data.index, columns=data.columns)


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


def driverScoreComparision(writer, df1, df2):
    old_score = df1.groupby("driver_id", as_index=False)["score"].mean()
    new_score = df2.groupby("driver_id", as_index=False)["score"].mean()
    df_all = pd.concat([old_score.set_index('driver_id'), new_score.set_index('driver_id')],
                       axis='columns', keys=['Old', 'New'])
    df_final = df_all.swaplevel(axis='columns')[old_score.columns[1:]]
    df_final = df_final.style.apply(highlight_diff, axis=None)
    df_final.to_excel(writer, sheet_name='Driver Summary', startrow=11, startcol=1)


compareTrips('/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/', "1000", '3.2.1')
