#!/bin/env python

import sys
import json
import pandas as pd

def generate_speeding_breakdown(speeding_event, speeding_locations):
    result = {}

    speeding_locations.sort(key=lambda l: l['timestamp'])

    if len(speeding_locations) > 0:
        for (i, location) in enumerate(speeding_locations):
            if 'speedLimit' in location:
                delta_speed = location['speed'] - location['speedLimit']

                next_timestamp = speeding_locations[i + 1]['timestamp'] if i < len(speeding_locations) - 1 else \
                    speeding_event['endTimestamp']
                current_timestamp = location['timestamp'] if location['timestamp'] >= speeding_event['startTimestamp'] else \
                    speeding_event['startTimestamp']
                delta_time = (next_timestamp - current_timestamp) / 1000.0

                speeding_percentage = delta_speed * 100 / location['speedLimit']
                base_percentage = int(speeding_percentage / 5)

                breakdown_key = str(location['speedLimit']) + "_" + str(base_percentage)
                breakdown = result.get(breakdown_key)
                if not breakdown:
                    breakdown = {
                        "threshold": location['speedLimit'],
                        "rangeFrom": base_percentage * 5,
                        "rangeTo": (base_percentage + 1) * 5,
                        "duration": 0
                    }
                    result[breakdown_key] = breakdown

                breakdown['duration'] = breakdown['duration'] + delta_time

    return list(result.values())


def fix_speeding_breakdown(trip, speeding_event):
    try:
        last_location_timestamp = max(
            [loc['timestamp'] for loc in trip['route'] if loc['timestamp'] <= speeding_event['startTimestamp']])
    except:
        last_location_timestamp = speeding_event['startTimestamp']

    speeding_locations = [loc for loc in trip['route'] if
                          loc['timestamp'] >= last_location_timestamp and loc['timestamp'] <= speeding_event[
                              'endTimestamp']]

    speeding_event['eventBreakdowns'] = generate_speeding_breakdown(speeding_event, speeding_locations)


def fixfile(inputfile, outputfile):
    # trip_json = "".join(sys.stdin.readlines())
    with open(inputfile, 'r') as myfile:
        trip_json = myfile.read()

    trip = json.loads(trip_json)

    if "events" in trip:
        speeding_events = [e for e in trip['events'] if e['eventType'] == 'SPEEDING']
        for speeding_event in speeding_events:
            fix_speeding_breakdown(trip, speeding_event)

    with open(outputfile, 'w') as file:
        file.write(json.dumps(trip))  # use `json.loads` to do the reverse

fixfile("/Users/omerorhan/Documents/EventDetection/regression_server/d-300099246---t-057956929500473b9a47dc3c6cf7004d.json", "/Users/omerorhan/Documents/EventDetection/regression_server/d-300099246---t-057956929500473b9a47dc3c6cf7004d2.json")
def main():
    import os
    batch_file_dir = "/home/ec2-user/regressiontest/tripfiles/geotab/"
    #batch_file_dir = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/filename/"
    count =0
    outputfolder = "/home/ec2-user/regressiontest/tripfiles/geotab_fixed/"
    #outputfolder = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/filename_fixed/"
    for root, dirs, files in os.walk(batch_file_dir):
        for filename in files:
            if filename.endswith('.json'):
                input = os.path.join(root, filename);
                list = root.split('/')
                outpath = outputfolder + list[-1]
                if not os.path.exists(outpath):
                    os.makedirs(outpath)
                outpath = outpath + '/' + filename
                print(input)
                fixfile(input, outpath)
                count = count +1
                print(count)

#if __name__ == "__main__":
#    main()

