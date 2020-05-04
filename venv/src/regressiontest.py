from src.telematicsmultiprocess import uploadTripFilesandProcess
from src.get_trip_from_regression_json import getTripsFromRegressionServer
from src.comparision import compareTrips
from src.comparision import checkfolder
from src.changejsonfilename import changefilenames
from enum import Enum
from src.Enums import *
from src.versioningfiles import VersionFile
import socket
import os
import sys
import platform
import subprocess
import signal
import time
import shutil
import errno
import pandas as pd
import datetime
import requests
import warnings

if not sys.warnoptions:
    warnings.simplefilter("ignore")
os.system("source activate base")

print("=======REGRESSION TEST==========")


def killoldtelematicsprocess():
    p = subprocess.Popen(['ps', '-ef'], stdout=subprocess.PIPE)
    out, err = p.communicate()
    index = 0
    for line in out.splitlines():
        if 'telematics' in str(line):
            if platform.node() != 'dev-app-01-10-100-2-42.mentor.internal':
                for item in str(line).split(' '):
                    if RepresentsInt(item):
                        index = index + 1
                        try:
                            if index == 2:
                                print(item + " is being killed")
                                os.kill(int(item), signal.SIGKILL)
                        except:
                            continue
            else:
                for item in str(line).split(' '):
                    if RepresentsInt(item):
                        index = index + 1
                        if index == 1:
                            print(item + " is being killed")
                            os.kill(int(item), signal.SIGKILL)
        index = 0


def RepresentsInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def checkDynamoDBProcess():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print("Checking DynamoDB connection")
    if sock.connect_ex(('localhost', 8000)) != 0:
        print("can not be continued without DynamoDB!")
        exit()
    print("Cleaning DynamoDB...")

    dynamodbtry = 0
    try:
        clear_dynamodb()
        dynamodbtry = dynamodbtry + 1
    except:
        if dynamodbtry < 3:
            clear_dynamodb()
        else:
            print("check dynamodb!")
            exit()


def gettinginputs():
    try:
        print(
            "Select your type.. (1-Mentor Business 2-Non-Armada 3-GEOTAB 4-MentorV3)")
        selectionregressiontype = input("Selection:")
        if selectionregressiontype == "1":
            regressionType = RegressionTypeEnum.MentorBusiness
        elif selectionregressiontype == "2":
            regressionType = RegressionTypeEnum.NonArmada
        elif selectionregressiontype == '3':
            regressionType = RegressionTypeEnum.GEOTAB
        elif selectionregressiontype == '4':
            regressionType = RegressionTypeEnum.MentorBusinessV3
        else:
            print("The selection is not valid!")
            exit()

        if regressionType == RegressionTypeEnum.MentorBusiness or regressionType == RegressionTypeEnum.NonArmada or regressionType == RegressionTypeEnum.MentorBusinessV3:
            print(
                "Select your process type.. (1-RegressionTest 2-UpdateBaseTripResults 3-UpdateMapBase)")
        elif regressionType == RegressionTypeEnum.GEOTAB:
            print(
                "Select your process type.. (1-RegressionTest 2-UpdateBaseTripResults)")
        regressionProcessType = RegressionProcessTypeEnum(input("Selection:"))

        identicalJSONReport = None
        if regressionProcessType == RegressionProcessTypeEnum.RegressionTest:
            jsonfilenameEnum = JSONfilenameEnum.file
            identicalJSONReport = IdenticalJSONReportEnum(
                input("Do you need an identical JSON comparision report? (Y/N) :").upper())
        else:
            jsonfilenameEnum = JSONfilenameEnum.base

        if regressionProcessType == RegressionProcessTypeEnum.RegressionTest or regressionProcessType == RegressionProcessTypeEnum.RegressionUpdateBaseTripresults:
            threadsize = 8
        else:
            threadsize = 8

        if regressionType == RegressionTypeEnum.MentorBusiness:
            print("Type your pool-size. (Options:1000, 10000, 20000, 50000, 100000)")
            poolsize = PoolSize(input("Selection:"))
        elif regressionType == RegressionTypeEnum.NonArmada or regressionType == RegressionTypeEnum.MentorBusinessV3:
            print("Type your pool-size. (Options:1000, 10000)")
            pool = input("Selection:")

            if pool == '1000' and regressionType == RegressionTypeEnum.NonArmada:
                poolsize = PoolSize.POOL_NONARMADA_1000
            elif pool == '10000' and regressionType == RegressionTypeEnum.NonArmada:
                poolsize = PoolSize.POOL_NONARMADA_10000
            elif pool == '1000' and regressionType == RegressionTypeEnum.MentorBusinessV3:
                poolsize = PoolSize.POOL_MENTORV3_1000
            elif pool == '10000' and regressionType == RegressionTypeEnum.MentorBusinessV3:
                poolsize = PoolSize.POOL_MENTORV3_10000
            else:
                print("invalid input!")
                exit()
        elif regressionType == RegressionTypeEnum.GEOTAB:
            poolsize = PoolSize.POOL_GEOTAB
        elif regressionType == RegressionTypeEnum.MentorBusinessV3:
            poolsize = PoolSize.POOL_MENTORV3_10000


    except ValueError:
        print("The selection is not valid!")
        exit()
    return regressionProcessType, poolsize, regressionType, jsonfilenameEnum, threadsize, identicalJSONReport


def controlfolderfileprocess(regressionProcessType, regressionType, poolsize):
    if platform.node() == 'dev-app-01-10-100-2-42.mentor.internal':
        FOLDER_PATH = "/home/ec2-user/regressiontest/"
    else:
        FOLDER_PATH = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/"

    if len(os.listdir(FOLDER_PATH + "build/")) == 0:
        print("Can not be continued without build! Put telematics folder under the build directory!")
        exit()

    print("Copying config files!")

    os.system(
        "cp -rf " + FOLDER_PATH + "build/config " + FOLDER_PATH + "build/telematics-server/")

    '''
    if regressionProcessType == RegressionProcessTypeEnum.RegressionMapBase:
        if regressionType == RegressionTypeEnum.NonArmada or regressionType == RegressionTypeEnum.MentorBusinessV3:
            os.system(
                "cp -rf " + FOLDER_PATH + "build/backupbaseconfigfolder/" + poolsize.value + "/config " + FOLDER_PATH + "build/telematics-server/")
        else:
            os.system(
                "cp -rf " + FOLDER_PATH + "build/backupbaseconfigfolder/" + regressionType.value + "/config " + FOLDER_PATH + "build/telematics-server/")
    else:
        if regressionType == RegressionTypeEnum.NonArmada or regressionType == RegressionTypeEnum.MentorBusinessV3:
            os.system(
                "cp -rf " + FOLDER_PATH + "build/backupconfigfolder/" + poolsize.value + "/config " + FOLDER_PATH + "build/telematics-server/")
        else:
            os.system(
                "cp -rf " + FOLDER_PATH + "build/backupconfigfolder/" + regressionType.value + "/config " + FOLDER_PATH + "build/telematics-server/")

    '''
    filename = FOLDER_PATH + "build/config/data-service.properties"
    dataserviceproperties = {}
    with open(filename) as f:
        for line in f:
            values = line.split('=')
            if len(values) > 1:
                key, value = line.split('=')
                dataserviceproperties[key] = value
            if len(values) < 2:
                value = line.split('=')
    
    data = ""
    for key, value in dataserviceproperties.items():
        if key == 'file.bucket':
            value = FOLDER_PATH + "tripfiles/" + poolsize.value+"\n"
        data = data + key + "=" + value

    myfile = open(FOLDER_PATH + "build/telematics-server/config/data-service.properties", 'w')
    myfile.writelines(data)
    myfile.close()

    filename = FOLDER_PATH + "build/config/map-service.properties"
    dataserviceproperties = {}
    with open(filename) as f:
        for line in f:
            values = line.split('=')
            if len(values) == 2:
                key, value = line.split('=')
            if len(values) > 2:
                key = values[0]
                value = line.replace(values[0] + "=", '')
            dataserviceproperties[key] = value

    data = ""
    for key, value in dataserviceproperties.items():
        if key == 'regression.is_recording':
            if regressionProcessType == RegressionProcessTypeEnum.RegressionMapBase:
                value = "true\n"
            else:
                value= "false\n"
        data = data + key + "=" + value

    myfile = open(FOLDER_PATH + "build/telematics-server/config/map-service.properties", 'w')
    myfile.writelines(data)
    myfile.close()

    return FOLDER_PATH


def startregressiontest():
    currentDT = datetime.datetime.now()
    print("Starting Time:" + str(currentDT))
    print("Be sure to put your new telematics folder in /home/ec2-user/regressiontest/build !!")

    regressionProcessType, poolsize, regressionType, jsonfilenameEnum, threadsize, identicalJSONReport = gettinginputs()

    # checkDynamoDBProcess()

    print("Checking telematics folder in build folder...")

    FOLDER_PATH = controlfolderfileprocess(regressionProcessType, regressionType, poolsize)

    print("Killing old telematics processes if exits...")
    killoldtelematicsprocess()

    os.system(
        "rm -r " + FOLDER_PATH + "jsonfiles/temp")

    print("Starting telematics...")
    os.system("sh " + FOLDER_PATH + "build/telematics-server/server.sh start")
    time.sleep(10)

    version = requests.get("http://localhost:8081/version").content.decode("utf-8")
    if regressionProcessType == RegressionProcessTypeEnum.RegressionTest:
        print("Base Telematics version:",
              checkfolder(FOLDER_PATH + "tripresults/maintripresult/" + poolsize.value).split("trip_results")[1].split(
                  ".csv")[0])
    print("Current Telematics version:" + version)

    log_dataframe = uploadTripFilesandProcess(FOLDER_PATH + "tripfiles/" + poolsize.value + "/", threadsize,
                                              regressionProcessType, regressionType)

    # copy json files temp to pools
    os.system(
        "rm -r " + FOLDER_PATH + "jsonfiles/" + regressionType.value + "/" + jsonfilenameEnum.value + "/" + poolsize.value + "/*")
    os.system(
        "mv " + FOLDER_PATH + "jsonfiles/temp/* " + FOLDER_PATH + "jsonfiles/" + regressionType.value + "/" + jsonfilenameEnum.value + "/" + poolsize.value)
    print("reading trips from JSON files")
    trip_results = getTripsFromRegressionServer(
        FOLDER_PATH + "jsonfiles/" + regressionType.value + "/" + jsonfilenameEnum.value + "/" + poolsize.value,
        threadsize)

    combinedresult_s3key = pd.merge(log_dataframe, trip_results, on='trip_id')

    # trip_id is coming random from telematics server. That's why, filenames are being changed by s3_key. It is the only primary paramater.
    if regressionType == RegressionTypeEnum.MentorBusiness or regressionType == RegressionTypeEnum.GEOTAB:
        print("setting s3_key to filename")
        changefilenames(combinedresult_s3key, regressionType, jsonfilenameEnum, poolsize, FOLDER_PATH, threadsize)

    if regressionProcessType == RegressionProcessTypeEnum.RegressionUpdateBaseTripresults or regressionProcessType == RegressionProcessTypeEnum.RegressionMapBase:
        VersionFile(FOLDER_PATH + "tripresults/maintripresult/" + poolsize.value + "/", ".csv")
        combinedresult_s3key.sort_values(["driver_id", "s3_key", ], inplace=True)
        combinedresult_s3key.to_csv(
            FOLDER_PATH + "tripresults/maintripresult/" + poolsize.value + "/trip_results" + version + ".csv",
            index=False)
    else:
        VersionFile(FOLDER_PATH + "tripresults/" + poolsize.value + "/", ".csv")
        combinedresult_s3key.sort_values(["driver_id", "s3_key", ], inplace=True)
        combinedresult_s3key.to_csv(FOLDER_PATH + "tripresults/" + poolsize.value + "/trip_results" + version + ".csv",
                                    index=False)
        comparisionpath = compareTrips(FOLDER_PATH, poolsize.value, version, regressionType, threadsize,
                                       identicalJSONReport)
        print("Your report is ready! Check the reports folder!")
        print(comparisionpath)

    os.system("sh " + FOLDER_PATH + "build/telematics-server/server.sh stop")
    finishdt = datetime.datetime.now()
    print("Start at " + str(currentDT))
    print("Finish at " + str(finishdt))
