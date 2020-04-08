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
         5: ['2019-11-03', '2019-11-09'],
         6: ['2019-11-10', '2019-11-16'],
         7: ['2019-11-17', '2019-11-23'],
         8: ['2019-11-24', '2019-11-30'],
         9: ['2019-12-01', '2019-12-07'],
         10: ['2019-12-08', '2019-12-14'],
         11: ['2019-12-15', '2019-12-21'],
         12: ['2019-12-22', '2019-12-28'],
         13: ['2019-12-29', '2020-01-04']}


def startProcessNonGeotabFiles(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, data):
    processCSVtoGetS3key(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, data)


def startProcessGeotabFiles(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, data):
    processgetstartendtimefromJSON(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, data)


def connect2Redshift():
    import psycopg2
    import pandas.io.sql as sqlio
    from datetime import datetime

    for i in range(1, 6):
        weekstart = weeks.get(i)[0]
        weekend = weeks.get(i)[1]
        source = "MENTOR_GEOTAB"
        # source = "MENTOR_NON_GEOTAB"
        con = psycopg2.connect(dbname='productionreportingdb',
                               host='edriving-telematics-production-reporting-db.c4bepljyphir.us-west-2.redshift.amazonaws.com',
                               port='5439', user='telematics_readonly', password='telematicsReadOnly123')
        query = "select trip_id, driver_id, source, local_date from trips where source in ('" + source + "') and local_date >= '" + \
                weekstart + "' and local_date <= '" + weekend + "' and status = 'SUCCESS' AND is_driver = 'true' AND is_personal = 'false' AND is_" \
                                                                "disputed = 'false' limit 5"
        source = "MENTORGEOTAB"
        data = sqlio.read_sql_query(query, con)
        RESULT_FILE_PATH = "jsonfiles/" + weekstart + "_" + weekend + "#" + source + "/"
        resultfilename = weekstart + "_" + weekend + "#" + source
        os.makedirs(FOLDER_PATH + RESULT_FILE_PATH, exist_ok=True)
        data["completed"] = False
        if source == "MENTORNONGEOTAB":
            startProcessNonGeotabFiles(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, data)
            data.to_csv(FOLDER_PATH + RESULT_FILE_PATH + resultfilename + ".csv")

        if source == "MENTORGEOTAB":
            data['FOLDER_PATH'] = FOLDER_PATH
            data['RESULT_FILE_PATH'] = RESULT_FILE_PATH
            data['resultfilename'] = resultfilename
            data['complete'] = False
            data['status'] = ""
            data['description'] = ""
            data['pmcount'] = ""
            data.to_csv(FOLDER_PATH + RESULT_FILE_PATH + resultfilename + ".csv")
            startProcessGeotabFiles(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, data)


killoldtelematicsprocess()
startTelematics(FOLDER_PATH)
connect2Redshift()

# startProcessNonGeotabFiles()
