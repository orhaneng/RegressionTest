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

os.system("source activate base")
from clear_database import clear_dynamodb
from telematics_multiprocess import upload_bin_batch_v2
from get_trip_from_regression import getTripsFromRegressionServer
from comparision import compareTrips

print("=======REGRESSION TEST==========")


def killoldtelematicsprocess():
    p = subprocess.Popen(['ps', '-ef'], stdout=subprocess.PIPE)
    print(p.pid)
    out, err = p.communicate()
    index=0

    for line in out.splitlines():
        print(line)
        if 'telematics' in str(line):
            for item in str(line).split(' '):
                # print(item)
                if RepresentsInt(item):
                    try:
                        print(item + " is being killed")
                        os.kill(int(item), signal.SIGKILL)
                    except:
                        continue

def RepresentsInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def regressiontest(FOLDER_PATH):
    print("checking telematics folder under build directory")
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
    os.system("cp -rf " + FOLDER_PATH + "build/mainconfigfolder/config " + FOLDER_PATH + "build/telematics-server/")
    print("killing old telematics processes!")
    #killoldtelematicsprocess()
    print("telematics is being started!")
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

#regressiontest(FOLDER_PATH)
#killoldtelematicsprocess()
'''

import subprocess

proc1 = subprocess.Popen(['ps', '-ef'], stdout=subprocess.PIPE)
proc2 = subprocess.Popen(['grep', 'telematics'], stdin=proc1.stdout,stdout=subprocess.PIPE, stderr=subprocess.PIPE)

for item in proc2.communicate()[0].splitlines():
    print(item)
print(proc2.kill())
print(proc2)
os.kill(proc2.pid, signal.SIGKILL)
proc1.stdout.close() # Allow proc1 to receive a SIGPIPE if proc2 exits.
out, err = proc2.communicate()
'''