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
import datetime
from dateutil import *
from dateutil.tz import *


def multi_run_wrapper(args):
    return getgeotablocationsSpeedLimit(*args)


def getgeotablocationsSpeedLimit(driver_id, trip_id, timestamp):
    utc_zone = tz.gettz('UTC')

    # timestamp = round(timestamp)
    jsonurl = "http://prod-uploader-845833724.us-west-2.elb.amazonaws.com/api/v2/drivers/" + str(
        driver_id) + "/trips/" + str(
        trip_id) + "?facet=all"
    response_json = requests.get(jsonurl).content.decode(
        "utf-8")
    trip = json.loads(response_json)
    df_result = pd.DataFrame(
        columns=['driver_id', 'trip_id', 'latitude', 'longitude', 'timestamp', 'speedLimit', 'speed', 'speed_limit_kph',
                 'speed_limit_mps', 'confidence', 'ramp'])
    data = "latitude,longitude,timestamp\n"
    count = 0
    if 'route' in trip:
        for item in trip['route']:
            if 'speedLimit' in item:
                df_result = df_result.append(
                    {'driver_id': trip['driverId'], 'trip_id': trip['tripId'],
                     'latitude': item['latitude'],
                     'longitude': item['longitude'], 'timestamp': item['timestamp'],
                     'speedLimit': item['speedLimit'],
                     'speed': item['speed']},
                    ignore_index=True)

                data = data + str(item['latitude']) + "," + str(
                    item['longitude']) + "," + datetime.datetime.fromtimestamp(
                    (item['timestamp'] / 1000.0)).astimezone(utc_zone).strftime('%Y-%m-%dT%H:%M:%S.%f%z') + "\n"
                count = count + 1
    server_url = 'https://rme.api.here.com/2/matchroute.json?app_id=yhsdwnC8DkOnH1pJs7k9&app_code=TzCzBAdNjTW9J-jd1iFTdw&routemode=car&attributes=SPEED_LIMITS_FCn(FROM_REF_SPEED_LIMIT,TO_REF_SPEED_LIMIT)' \
                 ',LINK_ATTRIBUTE_FCn(RAMP)'
    response = requests.post(server_url, data=data)
    trip = json.loads(response.content)

    print(trip)

    dictlinks = {}
    for item in trip["RouteLinks"]:
        dictlinks[item["linkId"]] = item

    for item in trip['TracePoints']:
        routepoint = dictlinks[item['linkIdMatched']]
        timestamp = item["timestamp"]
        speedlimit = 999999999
        if 'attributes' in routepoint:
            for route in routepoint['attributes']:
                if route == "SPEED_LIMITS_FCN":
                    for layer in routepoint['attributes']["SPEED_LIMITS_FCN"]:
                        if str(item["linkIdMatched"])[0] == '-':
                            speedlimit = layer['TO_REF_SPEED_LIMIT']
                            break
                        else:
                            speedlimit = layer['FROM_REF_SPEED_LIMIT']
                            break
                if route == 'LINK_ATTRIBUTE_FCN' and len(routepoint['attributes']["LINK_ATTRIBUTE_FCN"])>0:
                    if 'RAMP' in routepoint['attributes']["LINK_ATTRIBUTE_FCN"][0]:
                        ramp = routepoint['attributes']["LINK_ATTRIBUTE_FCN"][0]['RAMP']
        df_result.loc[(df_result['timestamp'] == timestamp), ['speed_limit_kph', 'speed_limit_mps',
            'confidence','ramp']] = [speedlimit, (float(speedlimit) + 0.5) * 0.277778,item['confidenceValue'],ramp]

    return df_result


def multi():
    pool = Pool(1)
    data = pd.read_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/amazonnewspeedingevents/geotabspeeding.csv")

    df_result = pd.DataFrame(
        columns=['driver_id', 'trip_id', 'latitude', 'longitude', 'timestamp', 'speedLimit', 'speed', 'speed_limit'])

    listquery = []
    #
    for i, row in data.iterrows():
        listquery.append(
            [row['driver_id'], row['trip_id'], row['date_part']])
        if i == 0:
            break
    try:
        with pool as p:
            print("Pool-size:", len(listquery))
            resultlist = list(tqdm.tqdm(p.imap(multi_run_wrapper, listquery), total=len(listquery)))
    except Exception as e:
        print(e)
        pool.terminate()
        pool.join()
        exit()
    resultlist = pd.concat(resultlist)
    resultlist.to_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/amazonnewspeedingevents/geotabspeedinglocation.csv")


multi()

'''
server_url = 'https://rme.api.here.com/2/matchroute.json?app_id=yhsdwnC8DkOnH1pJs7k9&app_code=TzCzBAdNjTW9J-jd1iFTdw&routemode=car&attributes=SPEED_LIMITS_FCn(FROM_REF_SPEED_LIMIT,TO_REF_SPEED_LIMIT),LINK_ATTRIBUTE2_FCn(PARKING_LOT_ROAD)'
# headers = {'Content-Type': 'text/plain'}
file_dir = "/Users/omerorhan/Documents/EventDetection/regression_server/amazonnewspeedingevents/jsons/loca.txt"
data = open(file_dir, 'rb')


timestamp = 1590323846063/1000.0
from dateutil import *
from dateutil.tz import *

utc_zone = tz.gettz('UTC')

timestamp = datetime.datetime.fromtimestamp(
                    timestamp).astimezone(utc_zone).strftime('%Y-%m-%dT%H:%M:%S.%f%z')


data = "latitude,longitude,timestamp\n 34.152603,-84.50947,"+timestamp+"\n"
response = requests.post(server_url, data=data)
print(response.content)

'''
