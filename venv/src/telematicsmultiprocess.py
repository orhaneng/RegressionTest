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
from src.Enums import *


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
                if file_names[idx][jdx].endswith('.bin_v2.gz') or file_names[idx][jdx].endswith('.bin_v2.gz.opt'):
                    if regressiontype == RegressionTypeEnum.MentorBusiness:

                        input.append(
                            tuple((driver_id_set[idx], batch_file_dir, file_names[idx][jdx], idx, jdx,
                                   regressionProcessType, regressiontype)))
                    elif regressiontype == RegressionTypeEnum.NonArmada:
                        sessionidlist.append(file_names[idx][jdx].split('_')[0])
                if file_names[idx][jdx].endswith('.json'):
                    if regressiontype == RegressionTypeEnum.GEOTAB:
                        input.append(
                            tuple((driver_id_set[idx], batch_file_dir, file_names[idx][jdx], idx, jdx,
                                   regressionProcessType, regressiontype)))
            if regressiontype == RegressionTypeEnum.NonArmada:
                sessionidlist = list(set(sessionidlist))
                for sessionid in sessionidlist:
                    input.append(
                        tuple((
                            driver_id_set[idx], "", sessionid, idx, 0, regressionProcessType,
                            regressiontype)))

    print("Processing trips...threadsize:", threadCount)
    pool = Pool(threadCount)
    try:
        with pool as p:
            print("Pool-size:", len(input))
            log = list(tqdm.tqdm(p.imap(multi_run_wrapper, input), total=len(input)))

    except Exception as e:
        print(e)
        pool.terminate()
        pool.join()
        exit()

    log_dataframe = pd.DataFrame(log)
    if len(log) < 2:
        print("no data")
        exit()
    log_dataframe.columns = ['trip_id', 's3_key']
    return log_dataframe


def multi_run_wrapper(args):
    return processDriver(*args)


def processDriver(driver_id, batch_file_dir, file_name, idx, jdx, regressionProcessType, regressiontype):
    if regressiontype == RegressionTypeEnum.MentorBusiness:
        server_url = 'http://localhost:8080/api/v2/drivers'
        file_dir = batch_file_dir + driver_id + '/' + file_name
        upload_url = server_url + '/' + driver_id + '/trips'
        response = requests.post(upload_url, files={'uploadedfile': open(file_dir, 'rb')})
    elif regressiontype == RegressionTypeEnum.NonArmada:
        server_url = 'http://localhost:8080/api/v3/drivers/'
        timestamp = '{"time": ' + str(int(round(time.time() * 1000))) + '}'
        upload_url = server_url + str(driver_id) + '/trips/' + file_name
        headers = {'Content-Type': 'application/json'}
        response = requests.post(upload_url, data=timestamp, headers=headers)
    elif regressiontype == RegressionTypeEnum.GEOTAB:
        server_url = 'http://localhost:8080/api/v2/drivers'
        headers = {'Content-type': 'application/json'}
        file_dir = batch_file_dir + driver_id + '/' + file_name
        upload_url = server_url + '/' + driver_id + '/trips'
        response = requests.post(upload_url, data=open(file_dir, 'rb'), headers=headers)
    response_json = None
    # if response.status_code == 500 and "NO DATA FOR REGRESSION MAP SERVICE" in response_json.get('reasonDetail'):
    #    print("unsaved mapping data " + response_json.get('reasonDetail'))
    #    raise Exception("unsaved mapping data")
    # if response.status_code == 400 and 'bad GPS Records' in str(response.content):
    #    print('bad gps data-'+"driver_id:" + str(driver_id) + " " + str(idx) + "/" + str(jdx) + "-status:" + str(
    #        response.status_code) + "-filename:" + file_name + " reason:" + str(response.content))
    if response.status_code != 200 and response.status_code != 400:
        print("driver_id:" + str(driver_id) + " " + str(idx) + "/" + str(jdx) + "-status:" + str(
            response.status_code) + "-filename:" + file_name + " reason:" + str(response.reason))

    log_row = []
    if response.status_code == 200:
        response_json = json.loads(response.content)
        log_row.append(str(response_json.get('tripId')))
        if regressiontype == RegressionTypeEnum.MentorBusiness or regressiontype == RegressionTypeEnum.GEOTAB:
            log_row.append(file_name)
        else:
            log_row.append(str(response_json.get('tripId')))

    return log_row
