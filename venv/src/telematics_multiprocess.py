"""
"""
import os
import pycurl
import pandas as pd
import requests
import json
import datetime
from multiprocessing import Pool

#batch_file_dir = '/Users/omerorhan/Documents/EventDetection/multiprocess'
batch_file_dir = '/home/ec2-user/omer/tripfiles'
server_url = 'http://localhost:8080/api/v2/drivers'
tripIdcsv = '/home/ec2-user/yichuan_testing/tripid.csv'
#tripIdcsv = '/Users/omerorhan/Documents/EventDetection/multiprocess/tripid.csv'

file_counter = 0
file_names = []
files_names_dict = {}


def upload_bin_batch_v2():
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
        print(str(idx) + "/387")
        #if idx == 5:
        #    break
        input.append(tuple((driver_id_set[idx], idx)))
    pool = Pool(8)
    result = pool.map(multi_run_wrapper, input)

    for item in result:
        for item2 in item:
            log.append(item2)

    return log


def multi_run_wrapper(args):
    return processDriver(*args)


def processDriver(driver_id, idx):
    log = []
    if len(driver_id) > 0 and len(file_names[idx]) > 0:  # Ignore empty folders
        for jdx in range(len(file_names[idx])):
            if file_names[idx][jdx].endswith('.bin_v2.gz'):
                file_dir = batch_file_dir + '/' + driver_id + '/' + file_names[idx][jdx]
                upload_url = server_url + '/' + driver_id + '/trips'
                response = requests.post(upload_url, files={'uploadedfile': open(file_dir, 'rb')})
                if response.status_code != 200:
                    print(response.status_code)
                    print(response)
                    print(file_dir)
                response_json = json.loads(response.content)
                log_row = []
                log_row.append(str(response_json.get('tripId')))
                log_row.append(str(driver_id))
                log_row.append(file_names[idx][jdx])
                log_row.append('Progress:')
                log.append(log_row)
                print("Driver index:" + str(idx))
    return log

    #   Batch process on V2


currentDT = datetime.datetime.now()
print("start at " + str(currentDT))
log = upload_bin_batch_v2()
log_dataframe = pd.DataFrame(log)
log_dataframe.to_csv(tripIdcsv, index=False)
finishDT = datetime.datetime.now()
print("start at " + str(currentDT))
print("finish at " + str(finishDT))
