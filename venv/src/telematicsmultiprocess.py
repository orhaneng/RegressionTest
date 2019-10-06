"""
"""
import os
import pycurl
import pandas as pd
import requests
import json
import datetime
from multiprocessing import Pool
import tqdm
import time
from enum import Enum


# TODO solve import enum
class RegressionTypeEnum(Enum):
    MentorBusiness = "1"
    NonArmada = "2"


def uploadTripFilesandProcess(batch_file_dir, threadCount, regressionProcessType, regressiontype):
    log = []
    file_names = []
    driver_id_set = None
    # Get file names and directories
    driverCount = 0
    for root, dirs, files in os.walk(batch_file_dir):
        if driver_id_set == None:
            driver_id_set = dirs
            continue
        files.sort()
        driverCount = driverCount + 1
        file_names.append(files)
    input = []
    driverlist = []
    for idx in range(len(driver_id_set)):
        driverlist.append(driver_id_set[idx])
        if len(file_names[idx]) > 0:
            sessionidlist = []
            for jdx in range(len(file_names[idx])):
                if regressiontype.value == "1":
                    if file_names[idx][jdx].endswith('.bin_v2.gz'):
                        input.append(
                            tuple((driver_id_set[idx], batch_file_dir, file_names[idx][jdx], idx, jdx,
                                   regressionProcessType, regressiontype)))
                elif regressiontype.value == "2":
                    sessionidlist.append(file_names[idx][jdx].split('_')[0])
            if regressiontype.value == "2":
                sessionidlist = list(set(sessionidlist))
                for sessionid in sessionidlist:
                    input.append(
                        tuple((
                            driver_id_set[idx], "", sessionid, idx, 0, regressionProcessType,
                            regressiontype)))

    # print(driverlist)
    print("Processing trips...")
    pool = Pool(threadCount)
    try:
        with pool as p:
            print("Pool-size:", len(input))
            result = list(tqdm.tqdm(p.imap(multi_run_wrapper, input), total=len(input)))
            [log.append(item) for item in result]

    except Exception as e:
        print(e)
        pool.terminate()
        pool.join()
        exit()

    log_dataframe = pd.DataFrame(log)
    log_dataframe.columns = ['trip_id', 's3_key']
    return log_dataframe


def multi_run_wrapper(args):
    return processDriver(*args)


def processDriver(driver_id, batch_file_dir, file_name, idx, jdx, regressionProcessType, regressiontype):
    if regressiontype.value == "1":
        server_url = 'http://localhost:8080/api/v2/drivers'
        file_dir = batch_file_dir + driver_id + '/' + file_name
        upload_url = server_url + '/' + driver_id + '/trips'
        response = requests.post(upload_url, files={'uploadedfile': open(file_dir, 'rb')})
        response_json = json.loads(response.content)
    elif regressiontype.value == "2":
        server_url = 'http://localhost:8080/api/v3/drivers/'
        timestamp = '{"time": ' + str(int(round(time.time() * 1000))) + '}'
        upload_url = server_url + str(driver_id) + '/trips/' + file_name
        headers = {'Content-Type': 'application/json'}
        response = requests.post(upload_url, data=timestamp, headers=headers)
        response_json = json.loads(response.content)

    # print("driver_id:" + str(driver_id) + " " + str(idx) + "/" + str(jdx) + "-status:" + str(
    #    response.status_code) + "-filename:" + file_name + " reason:" + str(response.reason))
    if response.status_code == 500 and "NO DATA FOR REGRESSION MAP SERVICE" in response_json.get(
            'reasonDetail'):
        print("unsaved mapping data " + response_json.get('reasonDetail'))
        raise Exception("unsaved mapping data")
    if response.status_code != 200 and regressionProcessType.value == "3" and regressionProcessType.value == "2":
        print(
            "response is not 200" + "driver_id:" + str(driver_id) + " " + str(idx) + "/" + str(jdx) + "-status:" + str(
                response.status_code) + "-filename:" + file_name + " reason:" + str(response.reason))
        raise Exception(response.reason)

    log_row = []
    log_row.append(str(response_json.get('tripId')))
    log_row.append(file_name)

    return log_row
