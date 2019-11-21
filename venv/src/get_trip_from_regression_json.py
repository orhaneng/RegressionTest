import boto3
import pandas as pd
import datetime
import requests
import sys


def getTripsFromRegressionServer():
    # Retrieve data from regression server
    #client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')
    from jsondiff import diff
    import json

    with open('/Users/omerorhan/Documents/EventDetection/json/trip1.json') as f:
        trip_json = json.load(f)
    #eventcounts = client.describe_table(TableName='tlm_trip').get("Table").get("ItemCount")
    #print("Processed events:", eventcounts, "trips")
    #paginator = client.get_paginator('scan')
    #page_iterator = paginator.paginate(TableName='tlm_event', Limit=200)
    trip_events = {}
    count = 0
    #for page in page_iterator:
    #    for item in page["Items"]:
    count += 1
    if count % 1000 == 0:
        #sys.stdout.write('\r' + "Total trip count from server is:" + str(count) + "/" + str(eventcounts))
        sys.stdout.flush()
        #print("Processing event:", count)
    trip_id = trip_json["tripId"]

    if trip_json["tripId"] == None:
        trip_events[trip_id] = {}
    event_dict = {} #trip_events.get(trip_id)

    if trip_json["events"] != None:
        for event in trip_json["events"]:
            e_name = event["eventType"]
            e_start = event["startTimestamp"]
            e_end = event["endTimestamp"]

            duration = (e_end - e_start)/1000.0
            if event_dict.get(e_name)== None:
                event_dict[e_name] = [0, 0]
            event_dict[e_name][0] += 1
            event_dict[e_name][1] += duration

    #page_iterator = paginator.paginate(TableName='tlm_trip', Limit=200)
    trips = pd.DataFrame(columns=["driver_id", "trip_id", "start_time", "score", "events"])
    count = 0
    index = 0
    if count > 1000:
        print()
    #for page in page_iterator:
    #    for item in page["Items"]:
    score = "None"
    if trip_json["scores"] != None:
        score = "None" if trip_json["scores"][0]["rating"] == None else \
            trip_json["scores"][0]["value"]
    trips.loc[index] = [trip_json["driverId"], trip_json["tripId"], trip_json["startTimestamp"], score, []]
    index += 1
    #count += page["Count"]
    #sys.stdout.write('\r' + "Total trip count from server is:" + str(count) + "/" + str(eventcounts))
    sys.stdout.flush()
        #print("Total trip count from server is:", str(count))

    event_definition = ["STOP", "START", "SMOOTH_STOP", "SMOOTH_START",
                        "RIGHT_TURN", "LEFT_TURN", "SMOOTH_RIGHT_TURN", "SMOOTH_LEFT_TURN",
                        "HARD_ACCELERATION", "HARD_BRAKING", "HARD_CORNERING",
                        "CALL_INCOMING", "CALL_OUTGOING", "PHONE_MANIPULATION",
                        "SPEEDING"]

    result = pd.DataFrame(columns=["trip_id", "driver_id", "start_time", "score",
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
                                   "displayed_speeding_count", "displayed_speeding_duration"])

    result_index = 0
    print()
    for index in list(trips.index):
        if result_index % 100 == 0:
            sys.stdout.write('\r' + "Local processing:" + str(result_index) + "/" + str(len(list(trips.index))))
            sys.stdout.flush()
            # print("Local processing:"+str( result_index))
        trip_id = trips.loc[index, "trip_id"]
        row = []
        row.append(trip_id)
        row.append(trips.loc[index, "driver_id"])
        row.append(trips.loc[index, "start_time"])
        row.append(trips.loc[index, "score"])
        for definition in event_definition:
            trip_event = trip_events.get(trip_id)
            if trip_event != None and trip_event.get(definition) != None:
                row.append(trip_event.get(definition)[0])
                row.append(trip_event.get(definition)[1])
            else:
                row.append(0)
                row.append(0)
        result.loc[result_index] = row
        result_index += 1
    sys.stdout.write('\r' + "Local processing:" + str(len(list(trips.index))) + "/" + str(len(list(trips.index))))
    sys.stdout.flush()
    print()
    result.sort_values(["driver_id", "start_time", ], inplace=True)
    return result

# result = getTripsFromRegressionServer()
# result.sort_values([ "driver_id", "start_time", ], inplace = True)
# result.to_csv("/Users/omerorhan/Documents/EventDetection/multiprocess/all_event_local.csv", index = False)
getTripsFromRegressionServer()