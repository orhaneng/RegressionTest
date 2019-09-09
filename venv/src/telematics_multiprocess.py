"""
"""
import os
import pycurl
import pandas as pd
import requests
import json
import datetime
from multiprocessing import Pool

server_url = 'http://localhost:8080/api/v2/drivers'
file_counter = 0
file_names = []
files_names_dict = {}

def upload_bin_batch_v2(batch_file_dir):
    print("upload_bin_batch_v2")
    log = []
    driver_id_set = None
    currentDT = datetime.datetime.now()
    print("start at " + str(currentDT))
    # Get file names and directories
    driverCount = 0;
    for root, dirs, files in os.walk(batch_file_dir):
        if driver_id_set == None:
            driver_id_set = dirs
            continue
        files.sort()
        driverCount = driverCount + 1
        file_names.append(files)
    input = []
    for idx in range(len(driver_id_set)):
        # print(str(idx) + "/387")
        if idx == 5:
           break
        input.append(tuple((driver_id_set[idx], idx,batch_file_dir)))
    pool = Pool(3)
    result = pool.map(multi_run_wrapper, input)

    for item in result:
        for item2 in item:
            log.append(item2)
    log_dataframe = pd.DataFrame(log)
    log_dataframe.columns = ['trip_id', 's3_key']
    return log_dataframe


def multi_run_wrapper(args):
    return processDriver(*args)


def processDriver(driver_id, idx,batch_file_dir):
    log = []
    if len(driver_id) > 0 and len(file_names[idx]) > 0:  # Ignore empty folders
        for jdx in range(len(file_names[idx])):
            if file_names[idx][jdx].endswith('.bin_v2.gz'):
                file_dir = batch_file_dir + '/' + driver_id + '/' + file_names[idx][jdx]
                upload_url = server_url + '/' + driver_id + '/trips'
                response = requests.post(upload_url, files={'uploadedfile': open(file_dir, 'rb')})
                response_json = json.loads(response.content)
                log_row = []
                log_row.append(str(response_json.get('tripId')))
                log_row.append(file_names[idx][jdx])
                log.append(log_row)
                print("Driver index:" + str(idx)+"/"+str(len(file_names[idx]))+" status:"+str(response.status_code))
    return log
