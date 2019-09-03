#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 15:50:19 2017

@author: yichuanniu
"""
import os
import pycurl
import pandas as pd
import requests
import json


def upload_bin_batch_v2(batch_file_dir, server_url):
    log = []
    driver_id_set = None
    file_names = []

    # Get file names and directories
    for root, dirs, files in os.walk(batch_file_dir):
        if driver_id_set == None:
            driver_id_set = dirs
            continue
        files.sort()
        file_names.append(files)
    #########   Upload multiple file of csv data with driver IDs     ##########
    total_files = 0
    file_counter = 0
    for idx in range(len(driver_id_set)):
        if len(driver_id_set[idx]) > 0 and len(file_names[idx]) > 0:  # Ignore empty folders
            driver_id = driver_id_set[idx]
            for jdx in range(len(file_names[idx])):
                if file_names[idx][jdx].endswith('.bin_v2.gz'):
                    total_files += 1

    for idx in range(len(driver_id_set)):
        if len(driver_id_set[idx]) > 0 and len(file_names[idx]) > 0:  # Ignore empty folders
            driver_id = driver_id_set[idx]
            for jdx in range(len(file_names[idx])):
                if file_names[idx][jdx].endswith('.bin_v2.gz'):
                    file_dir = batch_file_dir + '/' + driver_id + '/' + file_names[idx][jdx]
                    upload_url = server_url + '/' + driver_id + '/trips'
                    response = requests.post(upload_url, files={'uploadedfile': open(file_dir, 'rb')})
                    response_json = json.loads(response.content)
                    file_counter += 1
                    log_row = []
                    log_row.append(str(response_json.get('tripId')))
                    log_row.append(str(driver_id))
                    log_row.append(file_names[idx][jdx])
                    log_row.append('Progress: ' + str(file_counter) + \
                                   ' / ' + str(total_files) + ' = ' + \
                                   str('%.3f' % (file_counter / total_files * 100)) + '%')

                    log.append(log_row)
    return log


if __name__ == '__main__':
    ########################################################################################

    #   Batch process on V2
    log = upload_bin_batch_v2('/home/ec2-user/yichuan_testing/upload_1_thread/Apr_weely_data',
                              'http://localhost:8080/api/v2/drivers')
    log_dataframe = pd.DataFrame(log)
    log_dataframe.to_csv("/home/ec2-user/yichuan_testing/tripid.csv", index=False)
