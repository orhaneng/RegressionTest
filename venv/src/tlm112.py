import os

# os.system("source activate base")
import pandas as pd
import shutil
import subprocess
import platform
import time
import logging
import numpy as np
from tlm112_utility import *
from tlm112geotab import *

# if platform.node() == 'dev-app-01-10-100-2-42.mentor.internal':
#FOLDER_PATH = "/home/ec2-user/regressiontest/"
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

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename=FOLDER_PATH + "jsonfiles/" + datetime.now().strftime('%d-%m_%H-%M') + 'logger.log')
    urllib3_logger = logging.getLogger('urllib3')
    urllib3_logger.setLevel(logging.CRITICAL)

    for i in range(1, 3):
        weekstart = weeks.get(i)[0]
        weekend = weeks.get(i)[1]
        source = "MENTOR_GEOTAB"
        #source = "MENTOR_NON_GEOTAB"

        RESULT_FILE_PATH = "jsonfiles/" + weekstart + "_" + weekend + "#" + source + "/"
        resultfilename = weekstart + "_" + weekend + "#" + source
        os.makedirs(FOLDER_PATH + RESULT_FILE_PATH, exist_ok=True)
        logging.info("week=" + str(weeks[i]) + ",source=" + source + " STARTED")

        redshiftstart = datetime.now()

        con = psycopg2.connect(dbname='productionreportingdb',
                               host='edriving-telematics-production-reporting-db.c4bepljyphir.us-west-2.redshift.amazonaws.com',
                               port='5439', user='telematics_readonly', password='telematicsReadOnly123')
        query = "select trip_id, driver_id, source, local_date from trips where source in ('" + source + "') and local_date >= '" + \
                weekstart + "' and local_date <= '" + weekend + "' and status = 'SUCCESS' AND is_driver = 'true' AND is_personal = 'false' AND is_" \
                                                                "disputed = 'false'"
        data = sqlio.read_sql_query(query, con)

        redshifttime = (datetime.now() - redshiftstart).total_seconds()
        print("redshifttime=" + str(redshifttime))
        logging.info("redshifttime=" + str(redshifttime))

        if source == "MENTOR_NON_GEOTAB":
            data['index'] = np.arange(data.shape[0])
            data.to_csv(FOLDER_PATH + RESULT_FILE_PATH + resultfilename + ".csv")
            processTripsNongeotab(data, FOLDER_PATH, RESULT_FILE_PATH, resultfilename)

        if source == "MENTOR_GEOTAB":
            data['index'] = np.arange(data.shape[0])
            data['FOLDER_PATH'] = FOLDER_PATH
            data['RESULT_FILE_PATH'] = RESULT_FILE_PATH
            data['resultfilename'] = resultfilename
            data.to_csv(FOLDER_PATH + RESULT_FILE_PATH + resultfilename + ".csv")
            startProcessGeotabFiles(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, data)
        logging.info("week=" + str(weeks[i]) + ",source=" + source + "  FINISHED")


killoldtelematicsprocess()
startTelematics(FOLDER_PATH)
connect2Redshift()
