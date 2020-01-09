from src.datacompy.core import *
from src.jsoncomparision import checktwoJSONfiles
from multiprocessing import Pool
import tqdm

import pandas as pd
import xlsxwriter
import sys
from src.versioningfiles import VersionFile
import os
import numpy as np
import datetime
from src.Enums import *


def compareTrips(path, poolsize, version, regressionType, threadsize):
    VersionFile(path + "reports/" + poolsize + "/", ".xlsx")

    filepath = path + "reports/" + poolsize + "/regression_report" + version + ".xlsx"
    writer = pd.ExcelWriter(filepath,
                            engine='xlsxwriter')
    df1 = pd.read_csv(path + "tripresults/maintripresult/" + poolsize + "/" + checkfolder(
        path + "tripresults/maintripresult/" + poolsize), index_col=False)
    df2 = pd.read_csv(
        path + "tripresults/" + poolsize + "/" + checkfolder(path + "tripresults/" + poolsize),
        index_col=False)
    if regressionType == RegressionTypeEnum.MentorBusiness:
        df1.drop(df1.columns[0], axis=1, inplace=True)
        df2.drop(df2.columns[0], axis=1, inplace=True)
    compare = Compare(
        df1,
        df2,
        join_columns='s3_key',  # You can also specify a list of columns eg ['policyID','statecode']
        abs_tol=0,  # Optional, defaults to 0
        rel_tol=0,  # Optional, defaults to 0
        df1_name='Original',  # Optional, defaults to 'df1'
        df2_name='New'  # Optional, defaults to 'df2'
    )

    versions = pd.DataFrame(
        {"Version Type": ["Base Version", "New Version", "Report Created Time"], "Version": [checkfolder(
            path + "tripresults/maintripresult/" + poolsize), checkfolder(path + "tripresults/" + poolsize),
            str(datetime.datetime.now())]})
    versions.to_excel(writer, sheet_name='Summary', startrow=1, startcol=1)
    print("comparing JSONs to get identical match report")
    writer = JSONcomparision(path, poolsize, writer, regressionType, threadsize)
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
        print("ERROR! Allowed only one csv file under " + path)
        exit()
    return fname


def driverScoreComparision(writer, df1, df2):
    df1score = df1.copy(deep=True)
    df1score.loc[df1score['score'] == 'None', 'score'] = 0
    df1score['score'] = df1score['score'].astype(int)
    old_score = df1score.groupby("driver_id", as_index=False)["score"].mean()
    df2score = df2.copy(deep=True)
    df2score.loc[df2score['score'] == 'None', 'score'] = 0
    df2score['score'] = df2score['score'].astype(int)
    new_score = df2score.groupby("driver_id", as_index=False)["score"].mean()
    df_all = pd.concat([old_score.set_index('driver_id'), new_score.set_index('driver_id')],
                       axis='columns', keys=['Old', 'New'])
    df_final = df_all.swaplevel(axis='columns')[old_score.columns[1:]]
    df_final = df_final.style.apply(highlight_diff, axis=None)
    df_final.to_excel(writer, sheet_name='Driver Summary', startrow=11, startcol=1)


# compareTrips('/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/', "non-armada", '3.2.5')
def multi_run_wrapper(args):
    return checktwoJSONfiles(*args)

def JSONcomparision(path, poolsize, writer, regressionType, threadsize):
    import pandas as pd
    rootpath = path + "jsonfiles/" + regressionType.value + "/"

    filelist = []
    basepath = rootpath + str(JSONfilenameEnum.base.value) + "/" + str(poolsize) + "/"
    for root, dirs, files in os.walk(rootpath + str(JSONfilenameEnum.file.value) + "/" + str(poolsize) + "/"):
        for name in files:
            path = os.path.join(root, name)

            if path.endswith('.json'):
                driver_id = path.split('/')[-2]
                filelist.append([path, basepath + driver_id + "/" + name, name])
    result = pd.DataFrame(columns=["s3_key", "isIdentical", "comparision"])
    isallfilesidentical = True

    pool = Pool(threadsize)
    try:
        with pool as p:
            print("Pool-size:", len(filelist))
            comparisonresult = list(tqdm.tqdm(p.imap(multi_run_wrapper, filelist), total=len(filelist)))
            for item in comparisonresult:
                if not bool(item[0]):
                    isallfilesidentical = False
                new_row = {'s3_key': item[1], 'isIdentical': not bool(item[0]), "comparision": item[0]}
                result = result.append(new_row, ignore_index=True)

    except Exception as e:
        print(e)
        pool.terminate()
        pool.join()
        exit()

    head = pd.DataFrame(columns=["All trips are " + ("identical" if not isallfilesidentical else "not identical")])
    head.to_excel(writer, sheet_name='JSON Comparision', startrow=1, startcol=1)
    result.to_excel(writer, sheet_name='JSON Comparision', startrow=2, startcol=1)
    print("Files are ", "identical" if not isallfilesidentical else "not identical")
    return writer
