from clear_database import clear_dynamodb
from telematicsmultiprocess import uploadTripFilesandProcess
from get_trip_from_regression import getTripsFromRegressionServer
from comparision import compareTrips
from enum import Enum
from versioningfiles import VersionFile
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
import warnings
import requests

if not sys.warnoptions:
    warnings.simplefilter("ignore")
os.system("source activate base")


class RegressionTypeEnum(Enum):
    RegressionTest = "1"
    RegressionUpdateMainTripresults = "2"
    RegressionMapBase = "3"


class PoolSize(Enum):
    POOL_1000 = "1000"
    POOL_10000 = "10000"
    POOL_20000 = "20000"
    POOL_50000 = "50000"
    POOL_100000 = "100000"


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
            "Select your process type.. (1-RegressionTest 2-RegressionUpdateMainTripresults 3-RegressionMapBase)")
        regressionType = RegressionTypeEnum(input("Selection:"))
        print("Type your pool-size. (Options:1000, 10000, 20000, 50000, 100000")
        poolsize = PoolSize(input("Selection:"))
    except ValueError:
        print("The selection is not valid!")
        exit()
    return regressionType, poolsize


def folderFileProcess(regressionType):
    if platform.node() == 'dev-app-01-10-100-2-42.mentor.internal':
        FOLDER_PATH = "/home/ec2-user/regressiontest/"
    else:
        FOLDER_PATH = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/"

    if len(os.listdir(FOLDER_PATH + "build/")) == 0:
        print("Can not be continued without build! Put telematics folder under the build directory!")
        exit()

    print("Copying config files!")
    if regressionType == RegressionTypeEnum.RegressionMapBase:
        os.system(
            "cp -rf " + FOLDER_PATH + "build/backupbaseconfigfolder/config " + FOLDER_PATH + "build/telematics-server/")
    else:
        os.system(
            "cp -rf " + FOLDER_PATH + "build/backupconfigfolder/config " + FOLDER_PATH + "build/telematics-server/")
    return FOLDER_PATH


def regressiontest():
    regressionType, poolsize = gettinginputs()

    checkDynamoDBProcess()

    print("Checking telematics folder under build directory...")
    currentDT = datetime.datetime.now()
    print("Start at " + str(currentDT))

    FOLDER_PATH = folderFileProcess(regressionType)

    print("Killing old telematics processes...")
    killoldtelematicsprocess()

    print("Starting telematics...")
    os.system("sh " + FOLDER_PATH + "build/telematics-server/server.sh start")
    time.sleep(10)

    version = requests.get("http://localhost:8081/version").content.decode("utf-8")
    print("Current Telematics version:" + version)

    if regressionType == RegressionTypeEnum.RegressionMapBase:
        log_dataframe = uploadTripFilesandProcess(FOLDER_PATH + "tripfiles/" + poolsize.value + "/", 1, regressionType)
    else:
        log_dataframe = uploadTripFilesandProcess(FOLDER_PATH + "tripfiles/" + poolsize.value + "/", 4, regressionType)
    trip_results = getTripsFromRegressionServer()

    combinedresult_s3key = pd.merge(log_dataframe, trip_results, on='trip_id')

    if regressionType == RegressionTypeEnum.RegressionUpdateMainTripresults or regressionType == RegressionTypeEnum.RegressionMapBase:
        VersionFile(FOLDER_PATH + "tripresults/maintripresult/" + poolsize.value + "/", ".csv")
        combinedresult_s3key.to_csv(
            FOLDER_PATH + "tripresults/maintripresult/" + poolsize.value + "/trip_results" + version + ".csv")
    else:
        VersionFile(FOLDER_PATH + "tripresults/" + poolsize.value + "/", ".csv")
        combinedresult_s3key.to_csv(FOLDER_PATH + "tripresults/" + poolsize.value + "/trip_results" + version + ".csv")
    comparisionpath = compareTrips(FOLDER_PATH, poolsize.value, version)
    print("Report is ready! Check reports folder!")
    print(comparisionpath)
    os.system("sh " + FOLDER_PATH + "build/telematics-server/server.sh stop")
    finishdt = datetime.datetime.now()
    print("Start at " + str(currentDT))
    print("Finish at " + str(finishdt))


regressiontest()
