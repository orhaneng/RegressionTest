import boto3
import pandas as pd
import datetime
import requests


# Retrieve data from regression server
client = boto3.client('dynamodb', endpoint_url='http://10.100.2.42:8000')
print("Processed events:", client.describe_table(TableName='tlm_trip').get("Table").get("ItemCount"), "trips")


paginator = client.get_paginator('scan')
page_iterator = paginator.paginate(TableName='tlm_event', Limit = 200)
trip_events = {}

count = 0
for page in page_iterator:
    for item in page["Items"]:
        count += 1
        if count % 1000 == 0:
            print("Processing event:", count)
        trip_id = item.get("t_id").get("S")
        
        if trip_events.get(trip_id) == None:
            trip_events[trip_id] = {}
        event_dict = trip_events.get(trip_id)
        
        if item.get("data") != None and item.get("data").get("L") != None:
            for event in item.get("data").get("L"):
                event = event.get("M")
                e_name = event.get("type").get("S")
                e_start = event["t_start"]["S"]
                e_end = event["t_end"]["S"]

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
        


page_iterator = paginator.paginate(TableName='tlm_trip', Limit = 200)

trips = pd.DataFrame(columns = ["driver_id", "trip_id", "start_time", "score", "events"])
count = 0
index = 0
for page in page_iterator:
    for item in page["Items"]:
        score = "None"
        if item.get("scores") != None:
            score = "None" if item["scores"]["L"][0]["M"].get("rating") == None else item["scores"]["L"][0]["M"]["score"]["N"]
        trips.loc[index] = [item["d_id"]["S"], item["t_id"]["S"], item["t_start"]["S"], score, []]
        index += 1
    count += page["Count"]
    print("Total trip count from server is:", str(count))

event_definition = ["STOP", "START", "SMOOTH_STOP", "SMOOTH_START",
                    "RIGHT_TURN", "LEFT_TURN", "SMOOTH_RIGHT_TURN", "SMOOTH_LEFT_TURN",
                    "HARD_ACCELERATION", "HARD_BRAKING", "HARD_CORNERING", 
                    "CALL_INCOMING", "CALL_OUTGOING", "PHONE_MANIPULATION",
                    "SPEEDING"];

result = pd.DataFrame(columns = ["trip_id", "driver_id", "start_time", "score",
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
for index in list(trips.index):
    if result_index % 100 == 0:
        print("Local processing:", result_index)
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

result.sort_values([ "driver_id", "start_time", ], inplace = True)
result.to_csv("/home/ec2-user/yichuan_testing/all_event_local.csv", index = False)
