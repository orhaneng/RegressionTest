from jsondiff import diff
from src.Enums import *
import json
import os
from multiprocessing import Pool
import tqdm
import pandas as pd
import sys
sys.setrecursionlimit(2000)

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
    text = text.replace(": true", ": 'true'")
    text = text.replace(": false", ": 'false'")
    import yaml
    dict = yaml.load(text, Loader=yaml.FullLoader)
    dict = remove_keys(dict, "tripId")
    dict = remove_keys(dict, "eventId")
    dict = remove_keys(dict, "telematicsVersion")
    dict = remove_keys(dict, "mapProvider")
    text = str(dict).replace("\'", "\"")
    return text


def checktwoJSONfiles(file1, file2, name):

    with open(file1, encoding='utf-8') as f:
        trip_json1 = json.loads(cleanJSON(f.read()))

    with open(file2, encoding='utf-8') as f:
        trip_json2 = json.loads(cleanJSON(f.read()))

    return [diff(trip_json1, trip_json2), name]

def multi_run_wrapper(args):
    return checktwoJSONfiles(*args)


def JSONcomparision(path, poolsize, writer, regressionType, threadsize):
    rootpath = path + "jsonfiles/" + regressionType.value + "/"

    filelist = []
    basepath = rootpath + str(JSONfilenameEnum.base.value) + "/" + str(poolsize) + "/"
    for root, dirs, files in os.walk(rootpath + str(JSONfilenameEnum.file.value) + "/" + str(poolsize) + "/"):
        for name in files:
            path = os.path.join(root, name)
            if path.endswith('.json'):
                driver_id = path.split('/')[-2]
                if os.path.isfile(path) and os.path.isfile(basepath + driver_id + "/" + name):
                    filelist.append([path, basepath + driver_id + "/" + name, name])
                else:
                    print("missing json files detected.")
                    if not os.path.isfile(path):
                        print(path)
                    if not os.path.isfile(basepath + driver_id + "/" + name):
                        print(basepath + driver_id + "/" + name)
    result = pd.DataFrame(columns=["s3_key", "isIdentical", "comparision"])
    isallfilesidentical = True

    pool = Pool(threadsize)
    try:
        with pool as p:
            print("Pool-size:", len(filelist))
            comparisonresult = list(tqdm.tqdm(p.imap(multi_run_wrapper, filelist), total=len(filelist)))
            for item in comparisonresult:
                if not bool(item[0]):
                    isallfilesidentical = False
                new_row = {'s3_key': item[1], 'isIdentical': not bool(item[0]), "comparision": item[0]}
                result = result.append(new_row, ignore_index=True)

    except Exception as e:
        print(e)
        pool.terminate()
        pool.join()
        exit()

    head = pd.DataFrame(columns=["All trips are " + ("identical" if not isallfilesidentical else "not identical")])
    #head.to_excel(writer, sheet_name='JSON Comparision', startrow=1, startcol=1)
    #result.to_excel(writer, sheet_name='JSON Comparision', startrow=2, startcol=1)
    print("Files are ", "identical" if not isallfilesidentical else "not identical")
    return writer

#JSONcomparision("/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/", 1000, None,
#                RegressionTypeEnum.MentorBusiness,5 )
