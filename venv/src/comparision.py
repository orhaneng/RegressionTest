from src.datacompy.core import *
from src.identicalJsonComparision import checktwoJSONfiles
from multiprocessing import Pool
import tqdm
import os.path
import pandas as pd
import xlsxwriter
import sys
from src.versioningfiles import VersionFile
import os
import numpy as np
import datetime
from src.Enums import *


def compareTrips(path, poolsize, version, regressionType, threadsize, identicaljsonreport):
    VersionFile(path + "reports/" + poolsize + "/", ".xlsx")

    filepath = path + "reports/" + poolsize + "/regression_report" + version + ".xlsx"
    writer = pd.ExcelWriter(filepath,
                            engine='xlsxwriter')
    df1 = pd.read_csv(path + "tripresults/maintripresult/" + poolsize + "/" + checkfolder(
        path + "tripresults/maintripresult/" + poolsize), index_col=False)
    df2 = pd.read_csv(
        path + "tripresults/" + poolsize + "/" + checkfolder(path + "tripresults/" + poolsize),
        index_col=False)
    if regressionType == RegressionTypeEnum.MentorBusiness or regressionType == RegressionTypeEnum.GEOTAB:
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
    if identicaljsonreport == IdenticalJSONReportEnum.Yes:
        print("comparing JSONs to get an identical match report")
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

    events1 = df1.groupby(['driver_id'], as_index=False)[
        'displayed_speeding_count', 'displayed_speeding_15_count', 'displayed_speeding_20_count', 'hard_braking_count', 'hard_acceleration_count', 'phone_manipulation_count', 'hard_cornering_count'].agg(
        'sum')

    events2 = df2.groupby(['driver_id'], as_index=False)[
        'displayed_speeding_count', 'displayed_speeding_15_count', 'displayed_speeding_20_count', 'hard_braking_count', 'hard_acceleration_count', 'phone_manipulation_count', 'hard_cornering_count'].agg(
        'sum')

    distance1 = df1.groupby(['driver_id'], as_index=False)['distance'].sum()
    distance2 = df2.groupby(['driver_id'], as_index=False)['distance'].sum()

    df_event_comp = pd.DataFrame(columns=['driver_id', 'speeding_old', 'speeding_new', 'hard_braking_old',
                                          'hard_braking_new', 'hard_acceleration_old', 'hard_acceleration_new',
                                          'phone_manipulation_old', 'phone_manipulation_new', 'hard_cornering_old',
                                          'hard_cornering_new'])

    df_event_comp['driver_id'] = events1['driver_id']
    df_event_comp['speeding_old'] = round(
        ((events1['displayed_speeding_count'] * 100) / (distance1['distance'] * 0.000621371)), 2)
    df_event_comp['speeding_new'] = round(
        ((events2['displayed_speeding_count'] * 100) / (distance2['distance'] * 0.000621371)), 2)
    df_event_comp['speeding_15_old'] = round(
        ((events1['displayed_speeding_15_count'] * 100) / (distance1['distance'] * 0.000621371)), 2)
    df_event_comp['speeding_15_new'] = round(
        ((events2['displayed_speeding_15_count'] * 100) / (distance2['distance'] * 0.000621371)), 2)
    df_event_comp['speeding_20_old'] = round(
        ((events1['displayed_speeding_20_count'] * 100) / (distance1['distance'] * 0.000621371)), 2)
    df_event_comp['speeding_20_new'] = round(
        ((events2['displayed_speeding_20_count'] * 100) / (distance2['distance'] * 0.000621371)), 2)
    df_event_comp['hard_braking_old'] = round(
        ((events1['hard_braking_count'] * 100) / (distance1['distance'] * 0.000621371)), 2)
    df_event_comp['hard_braking_new'] = round(
        ((events2['hard_braking_count'] * 100) / (distance2['distance'] * 0.000621371)), 2)
    df_event_comp['hard_acceleration_old'] = round(
        ((events1['hard_acceleration_count'] * 100) / (distance1['distance'] * 0.000621371)), 2)
    df_event_comp['hard_acceleration_new'] = round(
        ((events2['hard_acceleration_count'] * 100) / (distance2['distance'] * 0.000621371)), 2)
    df_event_comp['phone_manipulation_old'] = round(
        ((events1['phone_manipulation_count'] * 100) / (distance1['distance'] * 0.000621371)), 2)
    df_event_comp['phone_manipulation_new'] = round(
        ((events2['phone_manipulation_count'] * 100) / (distance2['distance'] * 0.000621371)), 2)
    df_event_comp['hard_cornering_old'] = round(
        ((events1['hard_cornering_count'] * 100) / (distance1['distance'] * 0.000621371)), 2)
    df_event_comp['hard_cornering_new'] = round(
        ((events2['hard_cornering_count'] * 100) / (distance2['distance'] * 0.000621371)), 2)

    '''

    events1 = df1.groupby(['driver_id'], as_index=False)[
    'displayed_speeding_count', 'hard_braking_count', 'hard_acceleration_count', 'phone_manipulation_count', 'hard_cornering_count','displayed_speeding_15_count'].agg(
    'sum')

    events2 = df2.groupby(['driver_id'], as_index=False)[
        'displayed_speeding_count', 'hard_braking_count', 'hard_acceleration_count', 'phone_manipulation_count', 'hard_cornering_count','displayed_speeding_15_count'].agg(
        'sum')
    df_event_comp = pd.DataFrame(columns=['driver_id', 'speeding_old', 'speeding_new', 'hard_braking_old',
                                          'hard_braking_new', 'hard_acceleration_old', 'hard_acceleration_new',
                                          'phone_manipulation_old', 'phone_manipulation_new', 'hard_cornering_old',
                                          'hard_cornering_new','displayed_speeding_15_count_old','displayed_speeding_15_count_new'])

    df_event_comp['driver_id'] = events1['driver_id']
    df_event_comp['speeding_old'] = events1['displayed_speeding_count']
    df_event_comp['speeding_new'] = events2['displayed_speeding_count']
    df_event_comp['hard_braking_old'] = events1['hard_braking_count']
    df_event_comp['hard_braking_new'] = events2['hard_braking_count']
    df_event_comp['hard_acceleration_old'] = events1['hard_acceleration_count']
    df_event_comp['hard_acceleration_new'] = events2['hard_acceleration_count']
    df_event_comp['phone_manipulation_old'] = events1['phone_manipulation_count']
    df_event_comp['phone_manipulation_new'] = events2['phone_manipulation_count']
    df_event_comp['hard_cornering_old'] = events1['hard_cornering_count']
    df_event_comp['hard_cornering_new'] = events2['hard_cornering_count']
    df_event_comp['displayed_speeding_15_count_old'] = events1['displayed_speeding_15_count']
    df_event_comp['displayed_speeding_15_coun_new'] = events2['displayed_speeding_15_count']

    '''
    df_final.to_excel(writer, sheet_name='Driver Summary', startrow=11, startcol=1)
    df_event_comp.to_excel(writer, sheet_name='Driver Summary', startrow=13, startcol=4, index=False)


# compareTrips('/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/', "non-armada", '3.2.5')
def multi_run_wrapper(args):
    return checktwoJSONfiles(*args)


def JSONcomparision(path, poolsize, writer, regressionType, threadsize):
    rootpath = path + "jsonfiles/" + regressionType.value + "/"

    filelist = []
    basepath = rootpath + str(JSONfilenameEnum.base.value) + "/" + str(poolsize) + "/"
    for root, dirs, files in os.walk(rootpath + str(JSONfilenameEnum.file.value) + "/" + str(poolsize) + "/"):
        for name in files:
            path = os.path.join(root, name)
            if path.endswith('.json'):
                driver_id = path.split('/')[-2]
                if os.path.isfile(path) and os.path.isfile(basepath + driver_id + "/" + name):
                    filelist.append([path, basepath + driver_id + "/" + name, name])
                else:
                    print("missing json files detected.")
                    if not os.path.isfile(path):
                        print(path)
                    if not os.path.isfile(basepath + driver_id + "/" + name):
                        print(basepath + driver_id + "/" + name)
    result = pd.DataFrame(columns=["s3_key", "isIdentical", "comparision"])
    isallfilesidentical = True

    pool = Pool(threadsize)
    try:
        with pool as p:
            print("Pool-size:", len(filelist))
            comparisonresult = list(tqdm.tqdm(p.imap(multi_run_wrapper, filelist), total=len(filelist)))
            for item in comparisonresult:
                if bool(item[0]):
                    isallfilesidentical = False
                new_row = {'s3_key': item[1], 'isIdentical': not bool(item[0]), "comparision": item[0]}
                result = result.append(new_row, ignore_index=True)

    except Exception as e:
        print(e)
        pool.terminate()
        pool.join()
        exit()

    head = pd.DataFrame(columns=["All trips are " + ("identical" if isallfilesidentical else "not identical")])
    head.to_excel(writer, sheet_name='JSON Comparision', startrow=1, startcol=1)
    result.to_excel(writer, sheet_name='JSON Comparision', startrow=2, startcol=1)
    print("Files are ", "identical" if isallfilesidentical else "not identical")
    return writer

# compareTrips(path, poolsize, version, regressionType, threadsize, identicaljsonreport):
# compareTrips("/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/",PoolSize.POOL_1000.value,'3.3.19.5',RegressionTypeEnum.MentorBusiness,4, IdenticalJSONReportEnum.No)
