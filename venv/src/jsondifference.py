from jsondiff import diff
import json

file1 = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/jsoncomp/trip.300089971.0c58634560af49b29995af1cc1ebfa98_org.json"
with open(file1, encoding='utf-8') as f:
    trip_json1 = json.load(f)

file2 = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/jsoncomp/trip.300089971.0c58634560af49b29995af1cc1ebfa98.json"
with open(file2, encoding='utf-8') as f:
    trip_json2 = json.load(f)

print(diff(trip_json1, trip_json2, syntax='symmetric'))


