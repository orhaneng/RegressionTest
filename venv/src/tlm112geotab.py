from multiprocessing import Pool
import tqdm

import os

# os.system("source activate base")
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

threadcount = 4


def multi_run_wrapper(args):
    return copyFilesfromS3toRegressionServer(*args)


def multi_run_wrapperAurora(args):
    return connectAurora(*args)


def copyFilesfromS3toRegressionServer(s3listbyTripId, driver_id, trip_id, source, FOLDER_PATH):
    log = [driver_id, trip_id, "", 0]
    try:
        os.putenv('s3list', ' '.join(s3listbyTripId))
        # os.putenv('path', FOLDER_PATH+'tripfiles/tlm112-geotab/rawfiles/$x')
        subprocess.call(FOLDER_PATH + 'pmanalysis_tlm_112/shell_script.sh')
        log = processDriver(driver_id, trip_id, FOLDER_PATH)
        for item in s3listbyTripId:
            os.system(
                "rm -r " + FOLDER_PATH + "tripfiles/tlm112/" + item)
        if not os.listdir(FOLDER_PATH + 'tripfiles/tlm112/' + s3listbyTripId[0].split('/')[0]):
            os.system('rm -r ' + FOLDER_PATH + 'tripfiles/tlm112/' + s3listbyTripId[0].split('/')[0])
    except Exception as e:
        log[2] = e
    finally:
        return log


def processDriver(driver_id, trip_id, FOLDER_PATH):
    log_row = [driver_id, trip_id, "", 0]
    try:
        server_url = 'http://localhost:8080/api/v2/drivers'
        headers = {'Content-type': 'application/json'}
        file_dir = FOLDER_PATH + "tripfiles/tlm112-geotab/json/" + str(driver_id) + "/" + str(
            driver_id) + "-" + str(
            trip_id) + ".json"
        upload_url = server_url + '/' + str(driver_id) + '/trips'
        response = requests.post(upload_url, data=open(file_dir, 'rb'), headers=headers)
        if response.status_code != 200:
            print("driver_id:" + str(driver_id) + " " + "-status:" + str(
                response.status_code) + "-filename:" + session_id + " reason:" + str(response.reason))
        response_json = json.loads(response.content)
        count = 0;
        if 'eventCounts' in response_json:
            print()
            print()
            print("in event counts")
            print()
            print()
            for item in response_json['eventCounts']:
                if item['behaviouralImpact'] == 'NEGATIVE':
                    for eventitem in item['eventTypeCounts']:
                        if eventitem['eventType'] == 'PHONE_MANIPULATION':
                            print()
                            print()
                            print("in PHONE_MANIPULATION")
                            print()
                            print()
                            count = eventitem['count']
                            break
        log_row = [driver_id, trip_id, response.status_code, count]
    except Exception as e:
        log_row = [driver_id, trip_id, e, count]
    finally:
        return log_row


def connectAurora(driver_id, trip_id, FOLDER_PATH):
    cnx = mysql.connector.connect(user='omer', password='3$@Wed#f%g67dfg34%gH2s8',
                                  host='prod-telematics-aurora-cluster.cluster-ro-cikfoxuzuwyj.us-west-2.rds.amazonaws.com',
                                  database='telematics')

    df_result = pd.DataFrame(
        columns=['driver_id', 'trip_id', 'file_name', 'expire_in_days', 'start_time', 'end_time', 's3_key',
                 'created_at', 'updated_at'])

    JSONpath = FOLDER_PATH + "tripfiles/tlm112-geotab/json/" + str(driver_id) + "/" + str(driver_id) + '-' + str(
        trip_id) + '.json'
    jsonurl = "http://172.31.182.19:8080/api/v2/drivers/" + str(
            driver_id) + "/trips/" + str(
            trip_id) + "?facet=all"
    response_json = requests.get(jsonurl).content.decode(
        "utf-8")
    file_dir = FOLDER_PATH + "tripfiles/tlm112-geotab/json/" + str(driver_id) + "/" + str(
        driver_id) + "-" + str(
        trip_id) + ".json"
    os.makedirs(FOLDER_PATH + "tripfiles/tlm112-geotab/json/" + str(driver_id), exist_ok=True)
    trip = json.loads(response_json)
    with open(file_dir, 'w') as outfile:
        json.dump(trip, outfile)


    #with open(JSONpath, 'r') as myfile:
    #    trip = json.loads(myfile.read())
    starttimestamp = trip['startTimestamp']
    endtimestamp = trip['endTimestamp']
    query = r'select * from telematics.trip_file where ((start_time between ' + str(
        starttimestamp) + ' and ' + str(endtimestamp) + ') or (end_time between ' + str(starttimestamp) + ' and ' + str(
        endtimestamp) + ' )) and driver_id =' + "'" + str(driver_id) + "'"
    cursor = cnx.cursor()
    cursor.execute(query)
    for (driver_id_db, trip_id_db, file_name, expire_in_days, start_time, end_time, s3_key, created_at,
         updated_at) in cursor:
        df_result = df_result.append(
            {'driver_id': str(driver_id), 'trip_id': str(trip_id), 'file_name': file_name,
             'expire_in_days': expire_in_days, 'start_time': str(start_time),
             'end_time': str(end_time), 's3_key': s3_key, 'created_at': created_at,
             'updated_at': updated_at},
            ignore_index=True)

    cnx.close()
    return df_result


def processgetstartendtimefromJSON(FOLDER_PATH):
    print("geotab starts")
    print("thread count = " + str(threadcount))
    from datetime import datetime
    now = datetime.now()
    print("start")
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)

    exampleList = pd.read_csv(FOLDER_PATH + "pmanalysis_tlm_112/geotab/data1000.csv",
                              index_col=False, nrows=10)

    count = len(exampleList)
    listquery = []
    for i, row in exampleList.iterrows():
        listquery.append([row['driver_id'], row['trip_id'], FOLDER_PATH])

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

    from datetime import datetime
    now = datetime.now()
    print("data getting from aurora completed")
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)

    mergedf = pd.concat(resultlist)

    mergedf.to_csv(
        FOLDER_PATH + "pmanalysis_tlm_112/geotab/telematics.csv",
        index=False)

    mergedf = pd.read_csv(
        FOLDER_PATH + "pmanalysis_tlm_112/geotab/telematics.csv")

    processTrips(mergedf, exampleList, FOLDER_PATH)


def processTrips(df_result, exampleList, FOLDER_PATH):
    threadjobs = []
    print("process trips start")
    from datetime import datetime
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)

    for index, row in exampleList.iterrows():
        s3listbyTripId = df_result[df_result["trip_id"] == row["trip_id"]]['s3_key'].to_list()
        threadjobs.append([s3listbyTripId, row['driver_id'], row['trip_id'], row['source'], FOLDER_PATH])

    print("before pool")
    from datetime import datetime
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)

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

    exampleList.to_csv(FOLDER_PATH + "pmanalysis_tlm_112/geotab/dataafterprocess.csv")
    from datetime import datetime
    now = datetime.now()
    print("finished")
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)
