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
            "Select your type.. (1-Mentor Business 2-Non-Armada)")
        selectionregressiontype = input("Selection:")
        if selectionregressiontype == "1":
            regressionType = RegressionTypeEnum.MentorBusiness
        elif selectionregressiontype == "2":
            regressionType = RegressionTypeEnum.NonArmada
        else:
            print("The selection is not valid!")
            exit()

        print(
            "Select your process type.. (1-RegressionTest 2-UpdateBaseTripResults 3-UpdateMapBase)")
        regressionProcessType = RegressionProcessTypeEnum(input("Selection:"))

        if regressionProcessType == RegressionProcessTypeEnum.RegressionTest:
            jsonfilenameEnum = JSONfilenameEnum.file
            identicalJSONReport = IdenticalJSONReportEnum(input("Do you need an identical JSON comparision report? (Y/N) :").upper())
        else:
            jsonfilenameEnum = JSONfilenameEnum.base

        if regressionProcessType == RegressionProcessTypeEnum.RegressionTest or RegressionProcessTypeEnum.RegressionUpdateBaseTripresults:
            threadsize = 10
        else:
            threadsize = 2

        if regressionType == RegressionTypeEnum.MentorBusiness:
            print("Type your pool-size. (Options:1000, 10000, 20000, 50000, 100000)")
            poolsize = PoolSize(input("Selection:"))
        else:
            poolsize = PoolSize.POOL_NONARMADA


    except ValueError:
        print("The selection is not valid!")
        exit()
    return regressionProcessType, poolsize, regressionType, jsonfilenameEnum, threadsize, identicalJSONReport


def controlfolderfileprocess(regressionProcessType, regressionType):
    if platform.node() == 'dev-app-01-10-100-2-42.mentor.internal':
        FOLDER_PATH = "/home/ec2-user/regressiontest/"
    else:
        FOLDER_PATH = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/"

    if len(os.listdir(FOLDER_PATH + "build/")) == 0:
        print("Can not be continued without build! Put telematics folder under the build directory!")
        exit()

    print("Copying config files!")
    if regressionProcessType == RegressionProcessTypeEnum.RegressionMapBase:
        os.system(
            "cp -rf " + FOLDER_PATH + "build/backupbaseconfigfolder/" + regressionType.value + "/config " + FOLDER_PATH + "build/telematics-server/")
    else:
        os.system(
            "cp -rf " + FOLDER_PATH + "build/backupconfigfolder/" + regressionType.value + "/config " + FOLDER_PATH + "build/telematics-server/")
    return FOLDER_PATH


def startregressiontest():
    currentDT = datetime.datetime.now()
    print("Starting Time:" + str(currentDT))
    print("Be sure to put your new telematics folder in /home/ec2-user/regressiontest/build !!")

    regressionProcessType, poolsize, regressionType, jsonfilenameEnum, threadsize, identicalJSONReport = gettinginputs()

    # checkDynamoDBProcess()

    print("Checking telematics folder in build folder...")

    FOLDER_PATH = controlfolderfileprocess(regressionProcessType, regressionType)

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
    print("setting s3_key to filename")
    if regressionType == RegressionTypeEnum.MentorBusiness:
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
        comparisionpath = compareTrips(FOLDER_PATH, poolsize.value, version, regressionType, threadsize, identicalJSONReport)
        print("Report is ready! Check reports folder!")
        print(comparisionpath)

    os.system("sh " + FOLDER_PATH + "build/telematics-server/server.sh stop")
    finishdt = datetime.datetime.now()
    print("Start at " + str(currentDT))
    print("Finish at " + str(finishdt))
