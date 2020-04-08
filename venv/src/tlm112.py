import os

# os.system("source activate base")
import pandas as pd
import shutil
import subprocess
import platform
import time

from tlm112_utility import *
from tlm112geotab import *

# if platform.node() == 'dev-app-01-10-100-2-42.mentor.internal':
# FOLDER_PATH = "/home/ec2-user/regressiontest/"
# else:
FOLDER_PATH = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/"
weeks = {1: ['2019-10-06', '2019-10-12'],
         2: ['2019-10-13', '2019-10-19'],
         3: ['2019-10-20', '2019-10-26'],
         4: ['2019-10-27', '2019-11-02'],
         5: ['2019-11-03', '2019-11-09']}

def startProcessNonGeotabFiles(FOLDER_PATH,RESULT_FILE_PATH,resultfilename,data):
    processCSVtoGetS3key(FOLDER_PATH,RESULT_FILE_PATH,resultfilename,data)


def startProcessGeotabFiles(FOLDER_PATH,RESULT_FILE_PATH,resultfilename,data):
    #processgetstartendtimefromJSON(FOLDER_PATH,RESULT_PATH,data)
    a = ""
def connect2Redshift():
    import psycopg2
    import pandas.io.sql as sqlio
    from datetime import datetime

    for i in range(1, 6):
        weekstart = weeks.get(i)[0]
        weekend = weeks.get(i)[1]
        source = "MENTOR_NON_GEOTAB"
        con = psycopg2.connect(dbname='productionreportingdb',
                               host='edriving-telematics-production-reporting-db.c4bepljyphir.us-west-2.redshift.amazonaws.com',
                               port='5439', user='telematics_readonly', password='telematicsReadOnly123')
        query = "select trip_id, driver_id, source, local_date from trips where source in ('" + source + "') and local_date >= '" + \
                weekstart + "' and local_date <= '" + weekend + "' and status = 'SUCCESS' AND is_driver = 'true' AND is_personal = 'false' AND is_" \
                                                                "disputed = 'false' limit 5 "
        data = sqlio.read_sql_query(query, con)
        RESULT_FILE_PATH ="jsonfiles/" + weekstart + "_" + weekend + "_" + source + "/"
        resultfilename = weekstart + "_" + weekend + "_" + source
        os.makedirs(FOLDER_PATH+"jsonfiles/" + weekstart + "_" + weekend + "_" + source , exist_ok=True)
        data["completed"] = False
        data.to_csv(FOLDER_PATH+RESULT_FILE_PATH+resultfilename+".csv")
        if source == "MENTOR_NON_GEOTAB":
            startProcessNonGeotabFiles(FOLDER_PATH,RESULT_FILE_PATH,resultfilename,data)
        if source == "MENTOR_GEOTAB":
            startProcessGeotabFiles(FOLDER_PATH, RESULT_FILE_PATH,resultfilename,data)

killoldtelematicsprocess()
startTelematics(FOLDER_PATH)
connect2Redshift()


# startProcessNonGeotabFiles()
