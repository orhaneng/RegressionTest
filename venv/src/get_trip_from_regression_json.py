from multiprocessing import Pool
from jsondiff import diff

import boto3
import pandas as pd
import datetime
import requests
import sys
import os
import json
import tqdm

def multi_run_wrapper(args):
    return processJSONFile(*args)

def getTripsFromRegressionServer(path, threadsize):
    result = pd.DataFrame()
    filelist = []
    results= []
    for root, dirs, files in os.walk(path):
        for name in files:
            path = os.path.join(root, name)
            if path.endswith('.json'):
                filelist.append([path])
    pool = Pool(threadsize)

    try:
        with pool as p:
            print("Pool-size:", len(filelist))
            comparisonresult = list(tqdm.tqdm(p.imap(multi_run_wrapper, filelist), total=len(filelist)))
            #[results.append(item) for item in comparisonresult]
            #for item in comparisonresult:
            #    result.append(item)

    except Exception as e:
        print(e)
        pool.terminate()
        pool.join()
        exit()
    result = pd.DataFrame(comparisonresult)
    result.columns = ["trip_id", "driver_id", "start_time", "score",
                                   "stop_count", "stop_duration",
                                   "start_count", "start_duration", "smooth_stop_count",
                                   "smooth_stop_duration", "smooth_accel_count",
                                   "smooth_accel_duration", "right_turn_count", "right_turn_duration",
                                   "left_turn_count", "left_turn_duration", "smooth_right_turn_count",
                                   "smooth_right_turn_duration", "smooth_left_turn_count",
                                   "smooth_left_turn_duration", "hard_acceleration_count",
                                   "hard_acceleration_duration", "hard_braking_count",
                                   "hard_braking_duration", "hard_cornering_count",
                                   "hard_cornering_duration", "call_in_count",
                                   "call_in_duration", "call_out_count", "call_out_duration",
                                   "phone_manipulation_count", "phone_manipulation_duration",
                                   "displayed_speeding_count", "displayed_speeding_duration"]
    result.sort_values(['driver_id', 'start_time'], inplace=True)
    return result


def processJSONFile(file):
    with open(file, encoding='utf-8') as f:
        trip_json = json.load(f)
    trip_events = {}
    count = 0
    count += 1
    trip_id = trip_json["tripId"]

    if trip_json["tripId"] == None:
        trip_events[trip_id] = {}
    event_dict = {}  # trip_events.get(trip_id)

    if trip_json["events"] != None:
        for event in trip_json["events"]:
            e_name = event["eventType"]

            e_start = (datetime.datetime.fromtimestamp(event["startTimestamp"]["seconds"]) + datetime.timedelta(
                microseconds=event["startTimestamp"]["nanos"] / 1000)).isoformat(timespec='milliseconds')
            e_end = (datetime.datetime.fromtimestamp(event["endTimestamp"]["seconds"]) + datetime.timedelta(
                microseconds=event["endTimestamp"]["nanos"] / 1000)).isoformat(timespec='milliseconds')

            start = datetime.datetime(int(e_start[:4]), int(e_start[5:7]), int(e_start[8:10]),
                                      int(e_start[11:13]), int(e_start[14:16]),
                                      int(e_start[17:19]), int(e_start[20:23]) * 1000)
            end = datetime.datetime(int(e_end[:4]), int(e_end[5:7]), int(e_end[8:10]),
                                    int(e_end[11:13]), int(e_end[14:16]),
                                    int(e_end[17:19]), int(e_end[20:23]) * 1000)
            duration = (end - start).total_seconds() + 1
            if event_dict.get(e_name) == None:
                event_dict[e_name] = [0, 0]
            event_dict[e_name][0] += 1
            event_dict[e_name][1] += duration
    trips = pd.DataFrame(columns=["driver_id", "trip_id", "start_time", "score", "events"])
    index = 0
    score = "None"
    if "scores" in trip_json:
        score = "None" if not("rating" in trip_json["scores"][0]) else \
            trip_json["scores"][0]["score"]
    trips.loc[index] = [trip_json["driverId"], trip_json["tripId"], trip_json["startTimestamp"], score, []]
    index += 1
    event_definition = ["STOP", "START", "SMOOTH_STOP", "SMOOTH_START",
                        "RIGHT_TURN", "LEFT_TURN", "SMOOTH_RIGHT_TURN", "SMOOTH_LEFT_TURN",
                        "HARD_ACCELERATION", "HARD_BRAKING", "HARD_CORNERING",
                        "CALL_INCOMING", "CALL_OUTGOING", "PHONE_MANIPULATION",
                        "SPEEDING"]



    result_index = 0
    for index in list(trips.index):
        trip_id = trips.loc[index, "trip_id"]
        row = []
        row.append(trip_id)
        row.append(trips.loc[index, "driver_id"])
        e_start = (datetime.datetime.fromtimestamp(trips.loc[index, "start_time"]["seconds"]) + datetime.timedelta(
            microseconds=trips.loc[index, "start_time"]["nanos"] / 1000)).isoformat(timespec='milliseconds')
        row.append(e_start)
        row.append(trips.loc[index, "score"])
        for definition in event_definition:
            if event_dict != None and event_dict.get(definition) != None:
                row.append(event_dict.get(definition)[0])
                row.append(event_dict.get(definition)[1])
            else:
                row.append(0)
                row.append(0)
    return row