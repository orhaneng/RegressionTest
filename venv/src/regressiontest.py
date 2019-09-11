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

os.system("source activate base")
from clear_database import clear_dynamodb
from telematics_multiprocess import uploadTripFilesandProcess
from get_trip_from_regression import getTripsFromRegressionServer
from comparision import compareTrips

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


def regressiontest(FOLDER_PATH):
    print(
        "Please select your process. (1-RegressionTest(Default) 2-RegressionUpdateMainTripresults 3-RegressionMapBase (default:1))")
    regressionType = input("Selection:")
    print("checking telematics folder under build directory")
    currentDT = datetime.datetime.now()
    print("start at " + str(currentDT))
    if len(os.listdir(FOLDER_PATH + "build/")) == 0:
        print("can not be continued without build! Put telematics folder under the build directory!")
        exit()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print("checking DynamoDB connection")
    if sock.connect_ex(('localhost', 8000)) != 0:
        print("can not be continued without DynamoDB!")
        exit()
    clear_dynamodb()
    print("config files are being copied!")
    if regressionType == "2":
        os.system(
            "cp -rf " + FOLDER_PATH + "build/backupbaseconfigfolder/config " + FOLDER_PATH + "build/telematics-server/")
    else:
        os.system(
            "cp -rf " + FOLDER_PATH + "build/backupconfigfolder/config " + FOLDER_PATH + "build/telematics-server/")

    print("killing old telematics processes!")
    killoldtelematicsprocess()
    print("telematics is being started!")
    os.system("sh " + FOLDER_PATH + "build/telematics-server/server.sh start")
    time.sleep(10)
    if regressionType == "3":
        log_dataframe = uploadTripFilesandProcess(FOLDER_PATH + "tripfiles/",3)
    else:
        log_dataframe = uploadTripFilesandProcess(FOLDER_PATH + "tripfiles/", 8)
    trip_results = getTripsFromRegressionServer()
    combinedresult_s3key = pd.merge(log_dataframe, trip_results, on='trip_id')
    if regressionType == "2" or regressionType == "3":
        combinedresult_s3key.to_csv(FOLDER_PATH + "maintripresult/trip_results.csv")
    else:
        combinedresult_s3key.to_csv(FOLDER_PATH + "tripresults/trip_results.csv")
    compareTrips(FOLDER_PATH)
    print("Report is ready! Check reports folder!")
    os.system("sh " + FOLDER_PATH + "build/telematics-server/server.sh stop")
    finishdt = datetime.datetime.now()
    print("start at " + str(currentDT))
    print("finish at " + str(finishdt))


FOLDER_PATH = ""
if platform.node() == 'dev-app-01-10-100-2-42.mentor.internal':
    FOLDER_PATH = "/home/ec2-user/regressiontest/"
else:
    FOLDER_PATH = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/"
regressiontest(FOLDER_PATH)
