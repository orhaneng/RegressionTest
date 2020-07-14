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
from datetime import timedelta


def multi_run_wrapper(args):
    return getgeotablocationsSpeedLimit(*args)


def multi_run_wrapper(args):
    return getgeotablocations(*args)


def multi_run_wrapper2(args):
    return onebyone(*args)


def getgeotablocationsSpeedLimit(driver_id, trip_id, timestamp):
    df_result = pd.DataFrame(
        columns=['driver_id', 'trip_id', 'latitude', 'longitude', 'timestamp', 'speedLimit_GEOTAB',
                 'speed_GEOTAB_MPH', 'speed_limit_HERE_kph',
                 'speed_limit_HERE_MPH', 'confidence', 'ramp'])

    utc_zone = tz.gettz('UTC')

    # timestamp = round(timestamp)
    jsonurl = "http://prod-uploader-845833724.us-west-2.elb.amazonaws.com/api/v2/drivers/" + str(
        driver_id) + "/trips/" + str(
        trip_id) + "?facet=all"
    response_json = requests.get(jsonurl).content.decode(
        "utf-8")
    trip = json.loads(response_json)

    data = "latitude,longitude,timestamp\n"
    count = 0
    print("total", str(len(trip['route'])))
    if 'route' in trip:
        for item in trip['route']:
            try:
                if 'speedLimit' in item:
                    df_result = df_result.append(
                        {'driver_id': trip['driverId'], 'trip_id': trip['tripId'],
                         'latitude': item['latitude'],
                         'longitude': item['longitude'], 'timestamp': item['timestamp'],
                         'speedLimit_GEOTAB': item['speedLimit'],
                         'speed_GEOTAB': item['speed'],
                         'speed_GEOTAB_MPH': round(item['speedLimit'] * 2.23694)},
                        ignore_index=True)

                    data = data + str(item['latitude']) + "," + str(
                        item['longitude']) + "," + datetime.datetime.fromtimestamp(
                        (item['timestamp'] / 1000.0)).astimezone(utc_zone).strftime('%Y-%m-%dT%H:%M:%S.%f%z') + "\n"
                    count = count + 1
                    print(str(count))
                    server_url = 'https://rme.api.here.com/2/matchroute.json?app_id=yhsdwnC8DkOnH1pJs7k9&app_code=TzCzBAdNjTW9J-jd1iFTdw&routemode=car&attributes=SPEED_LIMITS_FCn(FROM_REF_SPEED_LIMIT,TO_REF_SPEED_LIMIT)' \
                                 ',LINK_ATTRIBUTE_FCn(RAMP)'
                    response = requests.post(server_url, data=data)
                    here = json.loads(response.content)

                    dictlinks = {}
                    for item in here["RouteLinks"]:
                        dictlinks[item["linkId"]] = item

                    for item in here['TracePoints']:
                        routepoint = dictlinks[item['linkIdMatched']]
                        timestamp = item["timestamp"]
                        speedlimit = None
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
                                if route == 'LINK_ATTRIBUTE_FCN' and len(
                                        routepoint['attributes']["LINK_ATTRIBUTE_FCN"]) > 0:
                                    if 'RAMP' in routepoint['attributes']["LINK_ATTRIBUTE_FCN"][0]:
                                        ramp = routepoint['attributes']["LINK_ATTRIBUTE_FCN"][0]['RAMP']
                        speedlimitmph = None
                        if speedlimit != None:
                            speedlimitmph = round(
                                (float(speedlimit) * 0.621371))
                        df_result.loc[
                            (df_result['timestamp'] == timestamp), ['speed_limit_HERE_kph', 'speed_limit_HERE_MPH',
                                                                    'confidence', 'ramp']] = [speedlimit, speedlimitmph,
                                                                                              item['confidenceValue'],
                                                                                              ramp]
            except Exception as e:
                print(e)
    return df_result


def getgeotablocations(driver_id, trip_id, timestamp):
    df_result = pd.DataFrame(
        columns=['driver_id', 'trip_id', 'latitude', 'longitude', 'timestamp', 'speedLimit_GEOTAB',
                 'speed_GEOTAB_MPH', 'speed_limit_HERE_kph',
                 'speed_limit_HERE_MPH', 'confidence', 'ramp'])

    utc_zone = tz.gettz('UTC')

    # timestamp = round(timestamp)
    jsonurl = "http://prod-uploader-845833724.us-west-2.elb.amazonaws.com/api/v2/drivers/" + str(
        driver_id) + "/trips/" + str(
        trip_id) + "?facet=all"
    response_json = requests.get(jsonurl).content.decode(
        "utf-8")
    trip = json.loads(response_json)

    data = "latitude,longitude,timestamp\n"
    count = 0
    print("total", str(len(trip['route'])))
    if 'route' in trip:
        for item in trip['route']:
            try:
                if 'speedLimit' in item:
                    df_result = df_result.append(
                        {'driver_id': trip['driverId'], 'trip_id': trip['tripId'],
                         'latitude': item['latitude'],
                         'longitude': item['longitude'], 'timestamp': item['timestamp'],
                         'speedLimit_GEOTAB': item['speedLimit'],
                         'speed_GEOTAB': item['speed'],
                         'speed_GEOTAB_MPH': round(item['speedLimit'] * 2.23694)},
                        ignore_index=True)
                else:
                    df_result = df_result.append(
                        {'driver_id': trip['driverId'], 'trip_id': trip['tripId'],
                         'latitude': item['latitude'],
                         'longitude': item['longitude'], 'timestamp': item['timestamp'],
                         'speedLimit_GEOTAB': None,
                         'speed_GEOTAB': item['speed'],
                         'speed_GEOTAB_MPH': None},
                        ignore_index=True)
            except Exception as e:
                print(e)
    return df_result


def multi():
    pool = Pool(4)
    data = pd.read_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/amazonnewspeedingevents/adam/Results.csv")

    listquery = []
    #
    for i, row in data.iterrows():
        listquery.append(
            [row['driver_id'], row['trip_id'], row['date_part']])
        if (i == 100):
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
        "/Users/omerorhan/Documents/EventDetection/regression_server/amazonnewspeedingevents/geotablocations100trips.csv")


#multi()

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


data = "latitude,longitude,timestamp\n 34.147224,-84.49359,"+timestamp+"\n"
response = requests.post(server_url, data=data)
print(response.content)

'''


def onebyone(data):
    timestamp = None
    speedlimitmph = None
    try:
        server_url = 'https://rme.api.here.com/2/matchroute.json?app_id=yhsdwnC8DkOnH1pJs7k9&app_code=TzCzBAdNjTW9J-jd1iFTdw&routemode=car&attributes=SPEED_LIMITS_FCn(FROM_REF_SPEED_LIMIT,TO_REF_SPEED_LIMIT)' \
                     ',LINK_ATTRIBUTE_FCn(RAMP)'
        response = requests.post(server_url, data=data)
        trip = json.loads(response.content)
        dictlinks = {}
        for item in trip["RouteLinks"]:
            dictlinks[item["linkId"]] = item

        for item in trip['TracePoints']:
            routepoint = dictlinks[item['linkIdMatched']]
            timestamp = item["timestamp"]
            if timestamp / 1000 == 1592606129:
                a = ""
            speedlimit = None
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
                    if route == 'LINK_ATTRIBUTE_FCN' and len(routepoint['attributes']["LINK_ATTRIBUTE_FCN"]) > 0:
                        if 'RAMP' in routepoint['attributes']["LINK_ATTRIBUTE_FCN"][0]:
                            ramp = routepoint['attributes']["LINK_ATTRIBUTE_FCN"][0]['RAMP']
            speedlimitmph = None
            if speedlimit != None:
                speedlimitmph = round(
                    (float(speedlimit) * 0.621371))
    except Exception as e:
        print(e)
    return [timestamp, speedlimitmph]


def multiamazon():
    pool = Pool(4)
    dataset = pd.read_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/amazonnewspeedingevents/adam/Speed_Data.csv")

    df_result = pd.DataFrame(
        columns=['driver_id', 'trip_id', 'latitude', 'longitude', 'timestamp', 'speedLimit', 'speed', 'speed_limit'])
    utc_zone = tz.gettz('UTC')

    listquery = []
    dataset['SPEED_LIMIT_HERE_MPH'] = None
    #
    for i, row in dataset.iterrows():
        if i == 5225:
            break
        datahere = "latitude,longitude,timestamp\n"
        datahere = datahere + str(row['latitude']) + "," + str(
            row['longitude']) + "," + datetime.datetime.fromtimestamp(
            (row['timestamp'] / 1000.0)).astimezone(utc_zone).strftime('%Y-%m-%dT%H:%M:%S.%f%z') + "\n"
        listquery.append([datahere])
    #
    try:
        with pool as p:
            print("Pool-size:", len(listquery))
            resultlist = list(tqdm.tqdm(p.imap(multi_run_wrapper2, listquery), total=len(listquery)))
            print(resultlist)
    except Exception as e:
        print(e)
        pool.terminate()
        pool.join()
        exit()

    for item in resultlist:
        dataset.loc[(dataset['timestamp'] == item[0]), ['SPEED_LIMIT_HERE_MPH']] = [item[1]]
    dataset.to_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/amazonnewspeedingevents/adam/Speed_Data2.csv")


multiamazon()


def distanceanalysis():
    data = pd.read_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/amazonnewspeedingevents/geotabspeedinglocation10trips.csv")
    print(len(data))
    data = data[(data['speed_limit_HERE_kph'] > 0) & (data["ramp"] == 'N')]
    print(len(data))
    datageotab = data[(data["speed_GEOTAB_MPH"] - data["speed_limit_HERE_MPH"]) > 4]
    print(len(datageotab))

    datageotab["diff"] = data["speed_GEOTAB_MPH"] - data["speed_limit_HERE_MPH"]
    print("geotab", datageotab["diff"].mean())

    datanongeotab = data[(data["speed_limit_HERE_MPH"] - data["speed_GEOTAB_MPH"]) > 4]
    datanongeotab["diff"] = data["speed_limit_HERE_MPH"] - data["speed_GEOTAB_MPH"]
    print(len(datanongeotab))

    print("nongeotab mean", datanongeotab["diff"].mean())

    # datageotab.to_csv(
    #    "/Users/omerorhan/Documents/EventDetection/regression_server/amazonnewspeedingevents/geotabspeedinglocation100difference.csv")


# distanceanalysis()


# multiamazon()


def procesamazonfile():
    data = pd.read_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/amazonnewspeedingevents/Amazon_Excessive_Speeding_Details_(Daily - test)_20200619_021514.csv")
    utc_zone = tz.gettz('UTC')
    date = datetime.datetime.now(tz=utc_zone)
    data['speed_limit'] = ""
    data['timestamp'] = ""
    data['speed'] = ""
    for i, row in data.iterrows():
        speed_limit = int(row['ExceptionDetailExtraInfo'].split(':')[2].split(' ')[1].strip())
        speed = (row['ExceptionDetailExtraInfo'].split(':')[1].strip().split(' ')[0])
        date = (date + timedelta(seconds=1))
        timestamp = date.timestamp()
        row['speed_limit'] = speed_limit
        data.set_value(i, 'speed_limit', speed_limit)
        data.set_value(i, "timestamp", (round(timestamp * 1000)))
        data.set_value(i, "speed", speed)
    data.to_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/amazonnewspeedingevents/Amazon_Excessive_Speeding_Details_(Daily - test)_20200619_021514_2.csv")


# procesamazonfile()


def amazondistanceanalysis():
    data = pd.read_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/amazonnewspeedingevents/geotabspeedinglocation100.csv")
    print(len(data))
    data = data[data['SPEED_LIMIT_HERE_MPH'] > 0]
    print(len(data))
    print(len(data))

    datageotab = data[(data["speed_limit"] - data["SPEED_LIMIT_HERE_MPH"]) > 4]
    print(len(datageotab))

    datageotab["diff"] = data["speed_limit"] - data["SPEED_LIMIT_HERE_MPH"]
    print("geotab", datageotab["diff"].mean())

    datanongeotab = data[(data["SPEED_LIMIT_HERE_MPH"] - data["speed_limit"]) > 4]
    datanongeotab["diff"] = data["SPEED_LIMIT_HERE_MPH"] - data["speed_limit"]
    print(len(datanongeotab))

    print("nongeotab mean", datanongeotab["diff"].mean())


# amazondistanceanalysis()


def adamresults():
    dataadam = pd.read_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/amazonnewspeedingevents/adam/Results.csv")
    dataadam["HERE_EDRIVING"] = ""

    dataedriving = pd.read_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/amazonnewspeedingevents/adam/geotab_locations_speedlimit.csv")

    count = 0
    for index, item in dataadam.iterrows():

        drivingspeed = dataedriving[(dataedriving['trip_id'] == item[2]) & (dataedriving['driver_id'] == item[1]) & ((
                        dataedriving['latitude'] == item[3]) & (dataedriving['longitude'] == item[4]))]
        if len(drivingspeed) == 0:
            #print(drivingspeed)
            count = count +1
            print(count)
        #print(drivingspeed)
    print(count)
        #dataadam.loc[
        #    (dataadam['trip_id'] == item[2]) & (dataadam['driver_id'] == item[1]) & (
        #                dataedriving['latitude'] == item[3]) & (dataadam['longitude'] == item[4]), ['HERE_EDRIVING']] = [drivingspeed]
        #print(row)




#adamresults()
