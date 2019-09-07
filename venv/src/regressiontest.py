import os
import socket
from clear_database import clear_dynamodb

print("=======REGRESSION TEST==========")
FOLDER_PATH = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/"

if len(os.listdir(FOLDER_PATH+"build/"))>0:
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
os.system("sh /Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/build/telematics-server/server.sh start")

