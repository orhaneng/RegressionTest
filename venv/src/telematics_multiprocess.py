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


def uploadTripFilesandProcess(batch_file_dir, threadCount):
    log = []
    file_names = []
    driver_id_set = None
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
        # if idx == 1:
        #   break
        input.append(tuple((driver_id_set[idx], idx, batch_file_dir, file_names)))
    pool = Pool(threadCount)
    try:
        result = pool.map(multi_run_wrapper, input)
        for item in result:
            for item2 in item:
                log.append(item2)
    except:
        print("Unsaved mapping data. Run RegressionMapBase step!")
        pool.terminate()
        pool.join()
        exit()

    log_dataframe = pd.DataFrame(log)
    log_dataframe.columns = ['trip_id', 's3_key']
    return log_dataframe


def multi_run_wrapper(args):
    return processDriver(*args)


def processDriver(driver_id, idx, batch_file_dir, file_names):
    log = []
    if len(driver_id) > 0 and len(file_names[idx]) > 0:  # Ignore empty folders
        for jdx in range(len(file_names[idx])):
            if file_names[idx][jdx].endswith('.bin_v2.gz'):
                file_dir = batch_file_dir + driver_id + '/' + file_names[idx][jdx]
                upload_url = server_url + '/' + driver_id + '/trips'
                response = requests.post(upload_url, files={'uploadedfile': open(file_dir, 'rb')})
                response_json = json.loads(response.content)
                print("Driver:"+str(driver_id)+" index:" + str(idx) + "/" + str(len(file_names[idx])) + "/" + str(jdx) + " status:" + str(
                    response.status_code) + " file name:"+file_names[idx][jdx])
                if response_json.get('httpStatus') == 500 and "NO DATA FOR REGRESSION MAP SERVICE" in response_json.get(
                        'reasonDetail'):
                    print("unsaved mapping data " + response_json.get('reasonDetail'))
                    raise Exception("unsaved mapping data")
                log_row = []
                log_row.append(str(response_json.get('tripId')))
                log_row.append(file_names[idx][jdx])
                log.append(log_row)
    return log
