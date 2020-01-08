from jsondiff import diff
from src.Enums import *
import json
import os


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


def cleanJSON(text):
    import yaml
    dict = yaml.load(text, Loader=yaml.FullLoader)
    dict = remove_keys(dict, "tripId")
    dict = remove_keys(dict, "eventId")
    # TODO true/false outputs cause a problem. fix and remove them later
    dict = remove_keys(dict, "isDriver")
    dict = remove_keys(dict, "isPersonal")
    dict = remove_keys(dict, "isDisputed")
    text = str(dict).replace("\'", "\"")
    return text


def checktwoJSONfiles(file1, file2, name):
    with open(file1, encoding='utf-8') as f:
        trip_json1 = json.loads(cleanJSON(f.read()))

    with open(file2, encoding='utf-8') as f:
        trip_json2 = json.loads(cleanJSON(f.read()))

    return [diff(trip_json1, trip_json2), name]
# JSONcomparision("/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/", PoolSize.POOL_1000, None,
#                RegressionTypeEnum.MentorBusiness)
