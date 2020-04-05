import os

os.system("source activate base")
import pandas as pd
import shutil
import subprocess
import platform
import time

from tlm112_utility import *
from tlm112geotab import *


if platform.node() == 'dev-app-01-10-100-2-42.mentor.internal':
    FOLDER_PATH = "/home/ec2-user/regressiontest/"
else:
    FOLDER_PATH = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/"


def startProcessNonGeotabFiles():
    killoldtelematicsprocess()
    startTelematics(FOLDER_PATH)
    processCSVtoGetS3key(FOLDER_PATH)
    killoldtelematicsprocess()

def startProcessGeotabFiles():
    killoldtelematicsprocess()
    startTelematics(FOLDER_PATH)
    processgetstartendtimefromJSON(FOLDER_PATH)
    killoldtelematicsprocess()



startProcessGeotabFiles()