from multiprocessing import Pool
import tqdm
import logging
import os
import datetime

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

threadcount = 16


def multi_run_wrapper(args):
    return copyFilesfromS3toRegressionServer(*args)


def multi_run_wrapperAurora(args):
    return connectAurora(*args)


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


def connectAurora(trip_id, driver_id):
    cnx = mysql.connector.connect(user='omer', password='3$@Wed#f%g67dfg34%gH2s8',
                                  host='prod-telematics-aurora-cluster.cluster-ro-cikfoxuzuwyj.us-west-2.rds.amazonaws.com',
                                  database='telematics')
    cursor = cnx.cursor()

    query = "select driver_id, trip_id,s3_key from trip_file where (driver_id = '" + str(
        driver_id) + "' and trip_id='" + str(trip_id) + "')"
    cursor.execute(query)
    s3list = []
    for (driver_id, trip_id, s3key) in cursor:
        s3list.append(s3key)
    cnx.close()
    return s3list


def processCSVtoGetS3key(exampleList, FOLDER_PATH, RESULT_FILE_PATH, resultfilename,weekstart, weekend):
    print("non-geotab starts")
    print("thread count = " + str(threadcount))
    from datetime import datetime
    now = datetime.now()
    print("start")
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)

    exampleList['FOLDER_PATH'] = FOLDER_PATH
    exampleList['RESULT_FILE_PATH'] = RESULT_FILE_PATH
    exampleList['resultfilename'] = resultfilename

    df_result = exampleList.values

    from datetime import datetime
    now = datetime.now()
    print("before pool")
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)

    pool = Pool(threadcount)
    try:
        with pool as p:
            print("Pool-size:", len(df_result))
            result = list(tqdm.tqdm(p.imap(multi_run_wrapper, df_result), total=len(df_result)))

    except Exception as e:
        print(e)
        pool.terminate()
        pool.join()
        exit()
    logging.info("threadpool is done")
    exampleList["PM_COUNT"] = ""
    exampleList["STATUS"] = ""
    exampleList["LOG"] = ""
    exampleList = exampleList.drop(columns=['FOLDER_PATH', 'RESULT_FILE_PATH', 'resultfilename'])
    for item in result:
        exampleList.loc[
            (exampleList['trip_id'] == item[1]) & (exampleList['driver_id'] == item[0]), ['PM_COUNT', 'STATUS',
                                                                                          'LOG']] = [
            item[3], item[2], item[4]]

    exampleList.to_csv(FOLDER_PATH + RESULT_FILE_PATH + "/" + resultfilename + "dataafterprocess.csv")
    from datetime import datetime
    now = datetime.now()
    print("finish")
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)


def processDriver(driver_id, session_id, trip_id, FOLDER_PATH, RESULT_FILE_PATH, resultfilename):
    log_row = [driver_id, trip_id, "", 0, ""]
    count = ""
    try:
        server_url = 'http://localhost:8080/api/v3/drivers/'
        timestamp = '{"time": ' + str(int(round(time.time() * 1000))) + '}'
        upload_url = server_url + str(driver_id) + '/trips/' + session_id
        headers = {'Content-Type': 'application/json'}
        response = requests.post(upload_url, data=timestamp, headers=headers, timeout=300)
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

        trip_idserver = trip_id
        if "tripId" in response_json:
            trip_idserver = response_json["tripId"]

        # csv_output_lock = threading.Lock()

        import os.path
        from os import path
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
        # os.system("mv " + FOLDER_PATH + "jsonfiles/temp/" + str(driver_id) + "/trip." + str(driver_id) + "." + str(
        #    trip_id) + ".json " + FOLDER_PATH + RESULT_FILE_PATH + "json/" + str(driver_id))
        # data.loc[(data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['complete','status','description','pmcount']] = [True,response.status_code,response.reason,str(count)]
        # with csv_output_lock:
        #    data.to_csv(FOLDER_PATH + RESULT_FILE_PATH + resultfilename + ".csv", index=False)

    except Exception as e:
        try:
            log_row = [driver_id, trip_id, "processDriver:" + str(e), count, ""]
        #    data.loc[(data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['status']] = e
        #    #with csv_output_lock:
        #    data.to_csv(FOLDER_PATH + RESULT_FILE_PATH + resultfilename + ".csv", index=False)
        except Exception as e:
            print(str(e))
    finally:
        return log_row
    return log_row


def copyFilesfromS3toRegressionServer(indeks, trip_id, driver_id, source, local_date, index,count,status,log, FOLDER_PATH, RESULT_FILE_PATH,
                                      resultfilename):
    threadstart = datetime.datetime.now()
    log = [driver_id, trip_id, "", 0, ""]
    try:
        aurorastart = datetime.datetime.now()
        s3listbyTripId = connectAurora(trip_id, driver_id)
        auroratime = (datetime.datetime.now() - aurorastart).total_seconds()

        session_id = trip_id.split('-')[1]
        if len(s3listbyTripId) > 1:
            os.putenv('s3list', ' '.join(s3listbyTripId))
        elif len(s3listbyTripId) == 1:
            os.putenv('s3list', s3listbyTripId[0])
        s3start = datetime.datetime.now()

        os.system(
            "aws s3 cp s3://mentor.trips.production-365/" + str(driver_id) + " " + FOLDER_PATH +
            "tripfiles/tlm112/" + str(driver_id) + " --recursive --exclude='*' --include='" + session_id + "*'")
        # subprocess.call(FOLDER_PATH + 'pmanalysis_tlm_112/shell_script.sh')
        s3time = (datetime.datetime.now() - s3start).total_seconds()

        processDriverstart = datetime.datetime.now()
        log = processDriver(driver_id, session_id, trip_id, FOLDER_PATH, RESULT_FILE_PATH, resultfilename)
        processtime = (datetime.datetime.now() - processDriverstart).total_seconds()

        timelog = "auroratime:" + str(
            auroratime) + ",s3time:" + str(s3time) + ",telematics:" + str(processtime)
        print("trip_id:" + str(trip_id) + ",driverid:" + str(driver_id) + timelog)
        log[4] = timelog
        for item in s3listbyTripId:
            os.system(
                "rm -r " + FOLDER_PATH + "tripfiles/tlm112/" + item)
        if not os.listdir(FOLDER_PATH + 'tripfiles/tlm112/' + s3listbyTripId[0].split('/')[0]):
            os.system('rm -r ' + FOLDER_PATH + 'tripfiles/tlm112/' + s3listbyTripId[0].split('/')[0])
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
    return log
