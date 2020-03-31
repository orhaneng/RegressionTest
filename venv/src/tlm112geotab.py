from src.Enums import *
from multiprocessing import Pool
import tqdm

import os

os.system("source activate base")
import pandas as pd
import shutil
import subprocess
import platform
import time
import requests
import json
import sys
import signal
import mysql.connector

threadcount = 1

def multi_run_wrapper(args):
    return copyFilesfromS3toRegressionServer(*args)

def copyFilesfromS3toRegressionServer(s3listbyTripId, driver_id, trip_id, source, FOLDER_PATH):

    os.putenv('s3list', ' '.join(s3listbyTripId))
    #os.putenv('path', FOLDER_PATH+'tripfiles/tlm112-geotab/rawfiles/$x')
    subprocess.call(FOLDER_PATH + 'pmanalysis_tlm_112/shell_script.sh')

    if source == 'MENTOR_GEOTAB':
        regressionType = RegressionTypeEnum.TLM112GEOTAB
    log = processDriver(driver_id, regressionType, trip_id, FOLDER_PATH)

    for item in s3listbyTripId:
        os.system(
            "rm -r " + FOLDER_PATH + "tripfiles/tlm112/" + item)
    if not os.listdir(FOLDER_PATH + 'tripfiles/tlm112/' + s3listbyTripId[0].split('/')[0]):
        os.system('rm -r ' + FOLDER_PATH + 'tripfiles/tlm112/' + s3listbyTripId[0].split('/')[0])
    return log


def processDriver(driver_id, regressiontype, trip_id, FOLDER_PATH):
    if regressiontype == RegressionTypeEnum.TLM112GEOTAB:
        server_url = 'http://localhost:8080/api/v2/drivers'
        headers = {'Content-type': 'application/json'}
        file_dir = FOLDER_PATH+"tripfiles/tlm112-geotab/json/"+str(driver_id)+"/"+str(driver_id)+"-"+str(trip_id)+".json"
        upload_url = server_url + '/' + str(driver_id) + '/trips'
        response = requests.post(upload_url, data=open(file_dir, 'rb'), headers=headers)

    elif regressiontype == RegressionTypeEnum.NonArmada:
        server_url = 'http://localhost:8080/api/v3/drivers/'
        timestamp = '{"time": ' + str(int(round(time.time() * 1000))) + '}'
        upload_url = server_url + str(driver_id) + '/trips/' + session_id
        headers = {'Content-Type': 'application/json'}
        response = requests.post(upload_url, data=timestamp, headers=headers)
    if response.status_code != 200:
        print("driver_id:" + str(driver_id) + " " + "-status:" + str(
            response.status_code) + "-filename:" + session_id + " reason:" + str(response.reason))
    response_json = json.loads(response.content)
    count = 0;
    for item in response_json['eventCounts']:
        if item['behaviouralImpact'] == 'NEGATIVE':
            for eventitem in item['eventTypeCounts']:
                if eventitem['eventType'] == 'PHONE_MANIPULATION':
                    count = eventitem['count']
                    break
    log_row = [driver_id, response_json['tripId'], response.status_code, count]
    return log_row

def processgetstartendtimefromJSON(FOLDER_PATH):
    cnx = mysql.connector.connect(user='omer', password='3$@Wed#f%g67dfg34%gH2s8',
                                  host='prod-telematics-aurora-cluster.cluster-ro-cikfoxuzuwyj.us-west-2.rds.amazonaws.com',
                                  database='telematics')

    listquery = []
    exampleList = pd.read_csv(FOLDER_PATH + "pmanalysis_tlm_112/geotab/geotabtrips.csv",
                              index_col=False, nrows=5)

    df_result = pd.DataFrame(
        columns=['driver_id', 'trip_id', 'file_name', 'expire_in_days', 'start_time', 'end_time', 's3_key',
                 'created_at', 'updated_at'])
    count = len(exampleList)
    '''
    for i, row in exampleList.iterrows():
        JSONpath = FOLDER_PATH + "tripfiles/tlm112-geotab/json/" + str(row['driver_id']) + "/" + str(
            row['driver_id']) + '-' + str(row['trip_id']) + '.json'
        with open(JSONpath, 'r') as myfile:
            trip = json.loads(myfile.read())
            starttimestamp = trip['startTimestamp']
            endtimestamp = trip['endTimestamp']
            query = 'select * from telematics.trip_file where (start_time between ' + str(
                starttimestamp) + ' and ' + str(endtimestamp) + ' and driver_id =' + str(
                row['driver_id']) + ') or (end_time between ' + str(starttimestamp) + ' and ' + str(
                endtimestamp) + ' and driver_id =' + str(row['driver_id']) + ')'
            cursor = cnx.cursor()
            cursor.execute(query)

            for (driver_id, trip_id, file_name, expire_in_days, start_time, end_time, s3_key, created_at,
                 updated_at) in cursor:
                df_result = df_result.append(
                    {'driver_id': row['driver_id'], 'trip_id': row['trip_id'], 'file_name': file_name,
                     'expire_in_days': expire_in_days, 'start_time': str(start_time),
                     'end_time': str(end_time), 's3_key': s3_key, 'created_at': created_at,
                     'updated_at': updated_at},
                    ignore_index=True)
            print(count)
            count = count - 1


    df_result.to_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/pmanalysis_tlm_112/geotab/telematics.csv", index=False)
    '''

    df_result = pd.read_csv("/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/pmanalysis_tlm_112/geotab/telematics.csv")
    cnx.close()

    processTrips(df_result, exampleList, FOLDER_PATH)

def processTrips(df_result, exampleList, FOLDER_PATH):
    threadjobs = []
    print("process trips start")
    for index, row in exampleList.iterrows():
        s3listbyTripId = df_result[df_result["trip_id"] == row["trip_id"]]['s3_key'].to_list()
        threadjobs.append([s3listbyTripId, row['driver_id'], row['trip_id'], row['source'], FOLDER_PATH])

    print("before pool")
    pool = Pool(threadcount)
    try:
        with pool as p:
            print("Pool-size:", len(threadjobs))
            result = list(tqdm.tqdm(p.imap(multi_run_wrapper, threadjobs), total=len(threadjobs)))

    except Exception as e:
        print(e)
        pool.terminate()
        pool.join()
        exit()
    exampleList["PM_COUNT"] = ""
    exampleList["STATUS"] = ""
    for item in result:
        exampleList.loc[(exampleList['trip_id'] == item[1]) & (exampleList['driver_id'] == item[0]), ['PM_COUNT']] = \
            item[3]
        exampleList.loc[(exampleList['trip_id'] == item[1]) & (exampleList['driver_id'] == item[0]), ['STATUS']] = item[
            2]

    exampleList.to_csv(FOLDER_PATH + "pmanalysis_tlm_112/non-geotab/dataafterprocess.csv")



