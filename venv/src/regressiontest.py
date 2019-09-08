import os, sys
import socket
from clear_database import clear_dynamodb
from telematics_multiprocess import upload_bin_batch_v2
from get_trip_from_regression import getTripsFromRegressionServer
import pandas as pd
from comparision import compareTrips
import platform
import subprocess, signal
import time

print("=======REGRESSION TEST==========")


def regressiontest(FOLDER_PATH):
    if len(os.listdir(FOLDER_PATH + "build/")) > 0:
        print("current(under build folder) version will be tested!")
    else:
        print("can not be continued without build! Put telematics folder under the build directory!")
        exit()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if sock.connect_ex(('localhost', 8000)) != 0:
        print("activate DynamoDB! Please open new command prompt and run code in Readme.txt")
        response = input("Is DynamoDB activated(Y/N)?")
        if response.upper() != 'Y' and sock.connect_ex(('localhost', 8000)) != 0:
            pass
        else:
            print("You can not continue without DynamoDB!")
            exit()
    clear_dynamodb()
    import subprocess, signal
    p = subprocess.Popen(['ps', '-ef'], stdout=subprocess.PIPE)
    out, err = p.communicate()
    #for line in out.splitlines():
        #if 'OmitStackTraceInFastThrow' in str(line):
            #pid = (str(line.split(None, 1)[1]).split(' ')[0]).split("'")[1]
            #os.kill(pid, signal.SIGKILL)
    os.system("sh " + FOLDER_PATH + "build/telematics-server/server.sh start")
    time.sleep(6)
    log_dataframe = upload_bin_batch_v2(FOLDER_PATH + "tripfiles/")
    trip_results = getTripsFromRegressionServer()
    combinedresult_s3key = pd.merge(log_dataframe, trip_results, on='trip_id')
    combinedresult_s3key.to_csv(FOLDER_PATH + "tripresults/trip_results.csv")
    compareTrips(FOLDER_PATH)
    print("Report is ready! Check reports folder!")
    os.system("sh " + FOLDER_PATH + "build/telematics-server/server.sh stop")


FOLDER_PATH = ""
if platform.node() == 'dev-app-01-10-100-2-42.mentor.internal':
    FOLDER_PATH = "/home/ec2-user/regressiontest/"
else:
    FOLDER_PATH = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/"

regressiontest(FOLDER_PATH)

