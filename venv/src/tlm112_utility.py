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


def connectAurora(query, FOLDER_PATH):
    print("in thread")
    cnx = mysql.connector.connect(user='omer', password='3$@Wed#f%g67dfg34%gH2s8',
                                  host='prod-telematics-aurora-cluster.cluster-ro-cikfoxuzuwyj.us-west-2.rds.amazonaws.com',
                                  database='telematics')
    cursor = cnx.cursor()
    cursor.execute(query)
    df_result = pd.DataFrame(columns=['driver_id', 'trip_id', 's3_key'])
    df_result2 = pd.DataFrame(columns=['driver_id', 'trip_id', 's3_key', 'FOLDER_PATH'])

    for (driver_id, trip_id, s3key) in cursor:
        df_result = df_result.append({'driver_id': driver_id, 'trip_id': trip_id, 's3_key': s3key},
                                     ignore_index=True)

        df_result2temp = df_result2[(df_result2['trip_id'] == trip_id) & (df_result2['driver_id'] == driver_id)]
        if len(df_result2temp) == 0:
            df_result2 = df_result2.append(
                {'driver_id': driver_id, 'trip_id': trip_id, 's3_key': s3key, 'FOLDER_PATH': FOLDER_PATH},
                ignore_index=True)
        else:
            s3 = df_result2temp['s3_key'].values
            s3list = str(s3[0]).split('&')
            s3list.append(s3key)
            df_result2.loc[
                (df_result2['trip_id'] == trip_id) & (df_result2['driver_id'] == driver_id), 's3_key'] = '&'.join(
                s3list)

    cnx.close()
    print("thread finished")
    return df_result2


def processCSVtoGetS3key(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, exampleList):
    print("non-geotab starts")
    print("thread count = " + str(threadcount))
    from datetime import datetime
    now = datetime.now()
    print("start")
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)

    import mysql.connector
    result = "select driver_id, trip_id,s3_key from trip_file where "
    query = []
    count = 1
    listquery = []

    for i, row in exampleList.iterrows():
        query.append("(driver_id = '" + str(row[1]) + "' and trip_id='" + str(row[0]) + "') or ")
        if count % 1000 == 0 or count == len(exampleList):
            listquery.append([result + "".join(query)[:-3], FOLDER_PATH])
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

    mergedf = pd.concat(resultlist)
    mergedf['FOLDER_PATH'] = FOLDER_PATH
    mergedf['RESULT_FILE_PATH'] = RESULT_FILE_PATH
    mergedf['resultfilename'] = resultfilename
    mergedf['complete'] = False
    mergedf['status'] = ""
    mergedf['description'] = ""
    mergedf['pmcount'] = ""


    mergedf.to_csv(FOLDER_PATH + RESULT_FILE_PATH + resultfilename + ".csv", index=False)
    processTrips(mergedf, exampleList, FOLDER_PATH, RESULT_FILE_PATH, resultfilename)


def processTrips(df_result, exampleList, FOLDER_PATH, RESULT_FILE_PATH, resultfilename):
    from datetime import datetime
    now = datetime.now()
    print("processTrips")
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)
    print("process trips start")
    count = 0

    df_result = df_result.values
    '''
    for index, row in exampleList.iterrows():
        s3listbyTripId = df_result[df_result["trip_id"] == row["trip_id"]]['s3_key'].to_list()
        threadjobs.append([s3listbyTripId, row['driver_id'], row['trip_id'], row['source'], FOLDER_PATH, count])
        count = count + 1
    '''
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
    # exampleList["PM_COUNT"] = ""
    # exampleList["STATUS"] = ""
    # for item in result:
    #     exampleList.loc[(exampleList['trip_id'] == item[1]) & (exampleList['driver_id'] == item[0]), ['PM_COUNT']] = \
    #         item[3]
    #     exampleList.loc[(exampleList['trip_id'] == item[1]) & (exampleList['driver_id'] == item[0]), ['STATUS']] = item[
    #         2]

    # exampleList.to_csv(FOLDER_PATH + "pmanalysis_tlm_112/non-geotab/dataafterprocess.csv")
    os.system("mv "+FOLDER_PATH+"jsonfiles/temp " +FOLDER_PATH+RESULT_FILE_PATH+"json")
    os.makedirs(FOLDER_PATH+"jsonfiles/temp")

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
        data.loc[(data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['status']] = response.status_code
        data.loc[(data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['description']] = response.reason
        data.loc[(data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['pmcount']] = str(count)

        data.to_csv(FOLDER_PATH + RESULT_FILE_PATH + resultfilename + ".csv", index=False)


    except Exception as e:
        log_row = [driver_id, trip_id, e, count]
        data.loc[(data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['status']] = "error"
        data.loc[(data['trip_id'] == trip_id) & (data['driver_id'] == int(driver_id)), ['status']] = e

    finally:
        return log_row
    return log_row


def copyFilesfromS3toRegressionServer(driver_id, trip_id, s3listbyTripId, FOLDER_PATH, RESULT_FILE_PATH, resultfilename,complete,status,desc, count):
    log = [driver_id, trip_id, "", 0]
    try:
        session_id = trip_id.split('-')[1]
        s3listbyTripId = s3listbyTripId.split('&')
        os.putenv('s3list', ' '.join(s3listbyTripId))
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
