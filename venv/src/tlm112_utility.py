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

threadcount = 3


def multi_run_wrapper(args):
    return copyFilesfromS3toRegressionServer(*args)


def multi_run_wrapperAurora(args):
    return connectAurora(*args)


def killoldtelematicsprocess():
    p = subprocess.Popen(['ps', '-ef'], stdout=subprocess.PIPE)
    out, err = p.communicate()
    index = 0
    for line in out.splitlines():
        if 'telematics' in str(line):
            if platform.node() != 'dev-app-01-10-100-2-42.mentor.internal':
                for item in str(line).split(' '):
                    if RepresentsInt(item):
                        index = index + 1
                        try:
                            if index == 2:
                                print(item + " is being killed")
                                os.kill(int(item), signal.SIGKILL)
                        except:
                            continue
            else:
                for item in str(line).split(' '):
                    if RepresentsInt(item):
                        index = index + 1
                        if index == 1:
                            print(item + " is being killed")
                            os.kill(int(item), signal.SIGKILL)
        index = 0


def startTelematics(FOLDER_PATH):
    os.system(
        "cp -rf " + FOLDER_PATH + "build/backupconfigfolder/tlm112/config " + FOLDER_PATH + "build/telematics-server/")
    print("Starting telematics...")
    os.system(
        "sh " + FOLDER_PATH + "build/telematics-server/server.sh start")
    time.sleep(10)


def RepresentsInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def connectAurora(query):
    print("in thread")
    cnx = mysql.connector.connect(user='omer', password='3$@Wed#f%g67dfg34%gH2s8',
                                  host='prod-telematics-aurora-cluster.cluster-ro-cikfoxuzuwyj.us-west-2.rds.amazonaws.com',
                                  database='telematics')
    cursor = cnx.cursor()
    #result = []
    cursor.execute(query)
    df_result = pd.DataFrame(columns=['driver_id', 'trip_id', 's3_key'])
    for (driver_id, trip_id, s3key) in cursor:
        #result.append([driver_id, trip_id, s3key])
        df_result = df_result.append({'driver_id': driver_id, 'trip_id': trip_id, 's3_key': s3key},
                                     ignore_index=True)

    cnx.close()
    print("thread finished")
    return df_result


def processCSVtoGetS3key(FOLDER_PATH):
    import mysql.connector
    exampleList = pd.read_csv(FOLDER_PATH + "pmanalysis_tlm_112/data_week_11_17_to_11_23.csv",
                              index_col=False)
    result = "select driver_id, trip_id,s3_key from trip_file where "
    query = []
    count = 1
    listquery = []
    for i, row in exampleList.iterrows():
        query.append("(driver_id = '" + str(row[0]) + "' and trip_id='" + str(row[1]) + "') or ")
        if count == 3000:
            break
        if count % 1000 == 0 or count == len(exampleList):
            listquery.append([result + "".join(query)[:-3]])
            query = []
        count = count + 1

    pool = Pool(threadcount)
    try:
        with pool as p:
            print("Pool-size:", len(listquery))
            resultlist = list(tqdm.tqdm(p.imap(multi_run_wrapperAurora, listquery), total=len(listquery)))


    except Exception as e:
        print(e)
        pool.terminate()
        pool.join()
        exit()

    df_result = pd.DataFrame(columns=['driver_id', 'trip_id', 's3_key'])

    mergedf = pd.concat(resultlist)

    df_result.to_csv(FOLDER_PATH + "pmanalysis_tlm_112/weekly_trips_final.csv", index=False)

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

    exampleList.to_csv(FOLDER_PATH + "pmanalysis_tlm_112/dataafterprocess.csv")


def processDriver(driver_id, regressiontype, session_id, FOLDER_PATH):
    if regressiontype == RegressionTypeEnum.MentorBusiness:
        server_url = 'http://localhost:8080/api/v2/drivers'
        file_dir = batch_file_dir + driver_id + '/' + file_name
        upload_url = server_url + '/' + driver_id + '/trips'
        response = requests.post(upload_url, files={'uploadedfile': open(file_dir, 'rb')})
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


def copyFilesfromS3toRegressionServer(s3listbyTripId, driver_id, trip_id, source, FOLDER_PATH):
    session_id = ''
    if source == "MENTOR_NON_GEOTAB":
        session_id = trip_id.split('-')[1]
        regressionType = RegressionTypeEnum.NonArmada

    os.putenv('s3list', ' '.join(s3listbyTripId))
    subprocess.call(FOLDER_PATH + 'pmanalysis_tlm_112/shell_script.sh')
    log = processDriver(driver_id, regressionType, session_id, FOLDER_PATH)

    for item in s3listbyTripId:
        os.system(
            "rm -r " + FOLDER_PATH + "tripfiles/tlm112/" + item)
    if not os.listdir(FOLDER_PATH + 'tripfiles/tlm112/' + s3listbyTripId[0].split('/')[0]):
        os.system('rm -r ' + FOLDER_PATH + 'tripfiles/tlm112/' + s3listbyTripId[0].split('/')[0])
    return log
