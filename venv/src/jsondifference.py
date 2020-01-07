from jsondiff import diff
import json
def clean(text):
    while text.find("tripId") != -1:
        if text.find("tripId") != -1:
            start = text.find("tripId") - 1
            end = text.find(']', start)
            text = text[0: start:] + text[end + 2::]
    return text

def remove_keys(obj, rubbish):
    if isinstance(obj, dict):
        obj = {
            key: remove_keys(value, rubbish)
            for key, value in obj.items()
            if key not in rubbish}
    elif isinstance(obj, list):
        obj = [remove_keys(item, rubbish)
                  for item in obj
                  if str(item) not in rubbish]
    return obj
def cleanJSON(dict):
    dict = remove_keys(dict, "tripId")
    dict = remove_keys(dict, "eventId")
    text = str(dict).replace("\'", "\"")
    return text
'''
file1 = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/jsonfiles/jsoncomp/trip.300089971.1565177948916.bin_v2.gz_base.json"
with open(file1, encoding='utf-8') as f:
    trip_json1 = json.load(f)

file2 = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/jsonfiles/jsoncomp/trip.300089971.1565177948916.bin_v2.gz.json"
with open(file2, encoding='utf-8') as f:
    trip_json2 = json.load(f)

result = (diff(trip_json1, trip_json2, syntax='symmetric'))

print(clean(str(result)))

result.pop("tripId")
print((result.pop("events")))
print(result)
'''


file1 = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/jsonfiles/jsoncomp/trip.300089971.1565177948916.bin_v2.gz_base.json"
with open(file1, encoding='utf-8') as f:
    import yaml
    text = yaml.load(f.read(), Loader=yaml.FullLoader)
    trip_json1 = json.loads(cleanJSON(text))
    print(trip_json1)


