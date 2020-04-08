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
    print("in thread")
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

def processTripsgeotab(exampleList, FOLDER_PATH, RESULT_FILE_PATH, resultfilename):

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
            list(tqdm.tqdm(p.imap(multi_run_wrapper, df_result), total=len(df_result)))

    except Exception as e:
        print(e)
        pool.terminate()
        pool.join()
        exit()

    from datetime import datetime
    now = datetime.now()
    print("finish")
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)


def processDriver(driver_id, session_id, trip_id, FOLDER_PATH, RESULT_FILE_PATH, resultfilename):
    log_row = [driver_id, trip_id, "", 0]
    try:
        server_url = 'http://localhost:8080/api/v3/drivers/'
        timestamp = '{"time": ' + str(int(round(time.time() * 1000))) + '}'
        upload_url = server_url + str(driver_id) + '/trips/' + session_id
        headers = {'Content-Type': 'application/json'}
        response = requests.post(upload_url, data=timestamp, headers=headers)
        if response.status_code != 200:
            print("driver_id:" + str(driver_id) + " " + "-status:" + str(
                response.status_code) + "-filename:" + session_id + " reason:" + str(response.reason))

        response_json = json.loads(response.content)
        count = 0
        if 'eventCounts' in response_json:
            for item in response_json['eventCounts']:
                if item['behaviouralImpact'] == 'NEGATIVE':
                    for eventitem in item['eventTypeCounts']:
                        if eventitem['eventType'] == 'PHONE_MANIPULATION':
                            count = eventitem['count']
                            break
        log_row = [driver_id, trip_id, response.status_code, count]

        data = pd.read_csv(FOLDER_PATH + RESULT_FILE_PATH + resultfilename + ".csv")
        data.loc[(data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['complete']] = True
        data.loc[
            (data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['status']] = response.status_code
        data.loc[
            (data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['description']] = response.reason
        data.loc[(data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['pmcount']] = str(count)

        csv_output_lock = threading.Lock()
        with csv_output_lock:
            data.to_csv(FOLDER_PATH + RESULT_FILE_PATH + resultfilename + ".csv", index=False)

        import os.path
        from os import path
        if not path.exists(FOLDER_PATH + RESULT_FILE_PATH+ "json/" + str(driver_id)):
            os.makedirs(FOLDER_PATH + RESULT_FILE_PATH + "json/" + str(driver_id), exist_ok=True)
        os.system("mv " + FOLDER_PATH + "jsonfiles/temp/" + str(driver_id) + "/trip." + str(driver_id) + "." + str(
            trip_id) + ".json " + FOLDER_PATH + RESULT_FILE_PATH + "json/" + str(driver_id))


    except Exception as e:
        log_row = [driver_id, trip_id, e, count]
        data.loc[(data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['status']] = "error"
        data.loc[(data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['status']] = e
        data.to_csv(FOLDER_PATH + RESULT_FILE_PATH + resultfilename + ".csv", index=False)

    finally:
        return log_row
    return log_row


def copyFilesfromS3toRegressionServer(trip_id, driver_id, source, local_date, completed,
                                      status, desc, count, FOLDER_PATH, RESULT_FILE_PATH,
                                      resultfilename):
    log = [driver_id, trip_id, "", 0]
    try:

        s3listbyTripId = connectAurora(trip_id, driver_id)
        session_id = trip_id.split('-')[1]
        if len(s3listbyTripId) > 1:
            os.putenv('s3list', ' '.join(s3listbyTripId))
        elif len(s3listbyTripId) == 1:
            os.putenv('s3list', s3listbyTripId)
        subprocess.call(FOLDER_PATH + 'pmanalysis_tlm_112/shell_script.sh')
        log = processDriver(driver_id, session_id, trip_id, FOLDER_PATH, RESULT_FILE_PATH, resultfilename)
        for item in s3listbyTripId:
            os.system(
                "rm -r " + FOLDER_PATH + "tripfiles/tlm112/" + item)
        if not os.listdir(FOLDER_PATH + 'tripfiles/tlm112/' + s3listbyTripId[0].split('/')[0]):
            os.system('rm -r ' + FOLDER_PATH + 'tripfiles/tlm112/' + s3listbyTripId[0].split('/')[0])
    except Exception as e:
        log[2] = e
    finally:
        return log
    return log
