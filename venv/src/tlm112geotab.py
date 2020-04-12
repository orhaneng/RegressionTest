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
import threading
import datetime
import logging

threadcount = 16


def multi_run_wrapper(args):
    return copyFilesfromS3toRegressionServer(*args)


def multi_run_wrapperAurora(args):
    return connectAurora(*args)


def copyFilesfromS3toRegressionServer(s3listbyTripId, driver_id, trip_id, source, FOLDER_PATH, RESULT_FILE_PATH,
                                      resultfilename, index):
    threadstart = datetime.datetime.now()

    log = [driver_id, trip_id, "", "", ""]
    timelog = ""
    try:
        if len(s3listbyTripId) > 0:
            os.putenv('s3list', ' '.join(s3listbyTripId))
            s3start = datetime.datetime.now()
            subprocess.call(FOLDER_PATH + 'pmanalysis_tlm_112/shell_script.sh')
            s3time = (datetime.datetime.now() - s3start).total_seconds()
            processDriverstart = datetime.datetime.now()
            log = processDriver(driver_id, trip_id, FOLDER_PATH, RESULT_FILE_PATH, resultfilename)
            processtime = (datetime.datetime.now() - processDriverstart).total_seconds()
            for item in s3listbyTripId:
                os.system(
                    "rm -r " + FOLDER_PATH + "tripfiles/tlm112/" + item)
            if not os.listdir(FOLDER_PATH + 'tripfiles/tlm112/' + s3listbyTripId[0].split('/')[0]):
                os.system('rm -r ' + FOLDER_PATH + 'tripfiles/tlm112/' + s3listbyTripId[0].split('/')[0])
        else:
            log[2] = "no s3_key"
            s3time = ""
            processtime = ""

        os.system(
            "rm -r " + FOLDER_PATH + "tripfiles/tlm112-geotab/json/" + str(driver_id) + "/" + str(
                driver_id) + "-" + str(trip_id) + ".json")
        if not os.listdir(FOLDER_PATH + "tripfiles/tlm112-geotab/json/" + str(driver_id)):
            os.system('rm -r ' + FOLDER_PATH + "tripfiles/tlm112-geotab/json/" + str(driver_id))
        timelog = ",s3time:" + str(s3time) + ",telematics:" + str(processtime)
        print("trip_id:" + str(trip_id) + ",driverid:" + str(driver_id) + timelog)
        log[4] = timelog
    except Exception as e:
        status = log[2]
        log[2] = status + str(e)
    finally:
        threadtime = (datetime.datetime.now() - threadstart).total_seconds()
        times = str(log[4])
        log[4] = "THREADTIME=" + str(threadtime) + "," + times
        logging.info(
            "index=" + str(index) + ",driver_id=" + str(driver_id) + ",trip_id=" + str(trip_id) + ",status=" + str(
                log[2]) + ",count=" + str(log[3]) + "," + str(log[4]))
        return log


def processDriver(driver_id, trip_id, FOLDER_PATH, RESULT_FILE_PATH, resultfilename):
    log_row = [driver_id, trip_id, "", "", ""]
    count = "";
    try:
        server_url = 'http://localhost:8080/api/v2/drivers'
        headers = {'Content-type': 'application/json'}
        file_dir = FOLDER_PATH + "tripfiles/tlm112-geotab/json/" + str(driver_id) + "/" + str(
            driver_id) + "-" + str(
            trip_id) + ".json"
        upload_url = server_url + '/' + str(driver_id) + '/trips'
        response = requests.post(upload_url, data=open(file_dir, 'rb'), headers=headers, timeout=300)
        if response.status_code != 200:
            print("driver_id:" + str(driver_id) + " " + "-status:" + str(
                response.status_code) + "-filename:" + session_id + " reason:" + str(response.reason))
        response_json = json.loads(response.content)
        log_row = [driver_id, trip_id, response.status_code, count, ""]
        if 'eventCounts' in response_json:
            count = "0"
            for item in response_json['eventCounts']:
                if item['behaviouralImpact'] == 'NEGATIVE':
                    for eventitem in item['eventTypeCounts']:
                        if eventitem['eventType'] == 'PHONE_MANIPULATION':
                            count = str(eventitem['count'])
                            break
        log_row[3] = count
        # data = pd.read_csv(FOLDER_PATH + RESULT_FILE_PATH + resultfilename + ".csv")
        # data.loc[(data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['complete','status','description','pmcount']] = [True,response.status_code,response.reason,str(count)]
        # csv_output_lock = threading.Lock()

        import os.path
        from os import path

        trip_idserver = trip_id
        if "tripId" in response_json:
            trip_idserver = response_json["tripId"]

        if response.status_code == 200:
            if not path.exists(FOLDER_PATH + RESULT_FILE_PATH + "json/" + str(driver_id)):
                os.makedirs(FOLDER_PATH + RESULT_FILE_PATH + "json/" + str(driver_id), exist_ok=True)
            os.system("mv " + FOLDER_PATH + "jsonfiles/temp/" + str(driver_id) + "/trip." + str(driver_id) + "." + str(
                trip_idserver) + ".json "
                      + FOLDER_PATH + RESULT_FILE_PATH + "json/" + str(driver_id) + "/trip." + str(
                driver_id) + "." + str(
                trip_id) + ".json")
            if not os.listdir(FOLDER_PATH + "jsonfiles/temp/" + str(driver_id)):
                os.system('rm -r ' + FOLDER_PATH + "jsonfiles/temp/" + str(driver_id))
        # with csv_output_lock:
        #    data.to_csv(FOLDER_PATH + RESULT_FILE_PATH + resultfilename + ".csv", index=False)

    except Exception as e:
        try:
            log_row = [driver_id, trip_id, "processDriver:" + str(e), count, ""]
            # data.loc[(data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['status']] = e
            # with csv_output_lock:
            #    data.to_csv(FOLDER_PATH + RESULT_FILE_PATH + resultfilename + ".csv", index=False)
        except Exception as e:
            print(str(e))

    finally:
        return log_row


def connectAurora(driver_id, trip_id, FOLDER_PATH, RESULT_FILE_PATH, resultfilename, index):
    df_result = pd.DataFrame(
        columns=['driver_id', 'trip_id', 'file_name', 'expire_in_days', 'start_time', 'end_time', 's3_key',
                 'created_at', 'updated_at'])

    JSONpath = FOLDER_PATH + "tripfiles/tlm112-geotab/json/" + str(driver_id) + "/" + str(driver_id) + '-' + str(
        trip_id) + '.json'
    dynamostart = datetime.datetime.now()
    jsonurl = "http://prod-uploader-845833724.us-west-2.elb.amazonaws.com/api/v2/drivers/" + str(
        driver_id) + "/trips/" + str(
        trip_id) + "?facet=all"
    response_json = requests.get(jsonurl).content.decode(
        "utf-8")
    dynamotime = (datetime.datetime.now() - dynamostart).total_seconds()
    file_dir = FOLDER_PATH + "tripfiles/tlm112-geotab/json/" + str(driver_id) + "/" + str(
        driver_id) + "-" + str(
        trip_id) + ".json"
    os.makedirs(FOLDER_PATH + "tripfiles/tlm112-geotab/json/" + str(driver_id), exist_ok=True)
    trip = json.loads(response_json)
    with open(file_dir, 'w') as outfile:
        json.dump(trip, outfile)

    # with open(JSONpath, 'r') as myfile:
    #    trip = json.loads(myfile.read())
    starttimestamp = trip['startTimestamp']
    endtimestamp = trip['endTimestamp']
    query = r'select * from telematics.trip_file where ((start_time between ' + str(
        starttimestamp) + ' and ' + str(endtimestamp) + ') or (end_time between ' + str(starttimestamp) + ' and ' + str(
        endtimestamp) + ' )) and driver_id =' + "'" + str(driver_id) + "'"

    aurorastart = datetime.datetime.now()

    cnx = mysql.connector.connect(user='omer', password='3$@Wed#f%g67dfg34%gH2s8',
                                  host='prod-telematics-aurora-cluster.cluster-ro-cikfoxuzuwyj.us-west-2.rds.amazonaws.com',
                                  database='telematics')
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

    auroratime = (datetime.datetime.now() - aurorastart).total_seconds()
    logging.info(
        "index=" + str(index) + ",driver_id=" + str(driver_id) + ",trip_id=" + str(trip_id) + ",auroratime=" + str(
            auroratime) + ",dynamotime=" + str(dynamotime))
    return df_result


def processgetstartendtimefromJSON(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, exampleList):
    print("geotab starts")
    print("thread count = " + str(threadcount))
    from datetime import datetime
    now = datetime.now()
    print("start")
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)

    count = len(exampleList)
    listquery = []
    for i, row in exampleList.iterrows():
        listquery.append(
            [row['driver_id'], row['trip_id'], FOLDER_PATH, RESULT_FILE_PATH, resultfilename, row['index']])

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

    mergedf.to_csv(
        FOLDER_PATH + RESULT_FILE_PATH + resultfilename + "telematics.csv",
        index=False)

    killoldtelematicsprocess()
    startTelematics(FOLDER_PATH)
    processTrips(mergedf, exampleList, FOLDER_PATH, RESULT_FILE_PATH, resultfilename)


def processTrips(df_result, exampleList, FOLDER_PATH, RESULT_FILE_PATH, resultfilename):
    threadjobs = []
    print("process trips start")
    from datetime import datetime
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)
    logging.info("s3_key matching started")
    for index, row in exampleList.iterrows():
        s3listbyTripId = df_result[df_result["trip_id"] == row["trip_id"]]['s3_key'].to_list()
        threadjobs.append(
            [s3listbyTripId, row['driver_id'], row['trip_id'], row['source'], FOLDER_PATH, RESULT_FILE_PATH,
             resultfilename, row["index"]])
    logging.info("s3_key matching ended")
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

    logging.info("threadpool is done")

    exampleList["PM_COUNT"] = ""
    exampleList["STATUS"] = ""

    for item in result:
        exampleList.loc[
            (exampleList['trip_id'] == item[1]) & (exampleList['driver_id'] == item[0]), ['PM_COUNT', 'STATUS']] = [
            item[3], item[2]]

    exampleList.to_csv(FOLDER_PATH + RESULT_FILE_PATH + "/" + resultfilename + "dataafterprocess.csv")

    from datetime import datetime
    now = datetime.now()
    print("finished")
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)


def killoldtelematicsprocess():
    p = subprocess.Popen(['ps', '-ef'], stdout=subprocess.PIPE)
    out, err = p.communicate()
    os.system("sudo pkill -9 java")
    index = 0
    for line in out.splitlines():
        if 'telematics' in str(line):
            if platform.node() != 'dev-app-01-10-100-2-42.mentor.internal' or platform.node() != 'e_production-telematics-3.3.19.5-172-31-181-104.app.edriving.com':
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