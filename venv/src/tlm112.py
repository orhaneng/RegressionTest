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
from datetime import datetime

# if platform.node() == 'dev-app-01-10-100-2-42.mentor.internal':
FOLDER_PATH = "/home/ec2-user/regressiontest/"
# else:
#FOLDER_PATH = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/"

'''
weeks = {1: ['2020-02-23', '2020-02-23'],
         2: ['2020-02-24', '2020-02-24'],
         3: ['2020-02-25', '2020-02-25'],
         4: ['2020-02-26', '2020-02-26'],
         5: ['2020-02-27', '2020-02-27'],
         6: ['2020-02-28', '2020-02-28'],
         7: ['2020-02-29', '2020-02-29']
         }
'''

weeks = {1: ['2019-03-01', '2022-03-01'],
         2: ['2020-03-02', '2020-03-02'],
         3: ['2020-03-03', '2020-03-03'],
         4: ['2020-03-04', '2020-03-04'],
         5: ['2020-03-05', '2020-03-05'],
         6: ['2020-03-06', '2020-03-06'],
         7: ['2020-03-07', '2020-03-07']
         }

'''
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
         13: ['2019-12-29', '2020-01-04'],
         14: ['2020-01-05', '2020-01-11'],
         15: ['2020-01-12', '2020-01-18'],
         16: ['2020-01-19', '2020-01-25'],
         17: ['2020-01-26', '2020-02-01'],
         18: ['2020-02-02', '2020-02-08'],
         19: ['2020-02-09', '2020-02-15'],
         20: ['2020-02-16', '2020-02-22'],
         21: ['2020-02-23', '2020-02-29'],
         22: ['2020-03-01', '2020-03-07'],
         23: ['2020-03-08', '2020-03-14'],
         24: ['2020-03-15', '2020-03-21']}
'''


def startProcessNonGeotabFiles(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, data):
    processCSVtoGetS3key(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, data)


def startProcessGeotabFiles(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, data, weekstart, weekend):
    processgetstartendtimefromJSON(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, data, weekstart, weekend)


def connect2Redshift():
    import psycopg2
    import pandas.io.sql as sqlio

    for i in range(1, 2):
        weekstart = weeks.get(i)[0]
        weekend = weeks.get(i)[1]
        source = "MENTOR_GEOTAB"
        # source = "MENTOR_NON_GEOTAB"

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
                                                                "disputed = 'false' and trip_id = 'a513a10e72ca456ebf85fea6dcebf71e' "

        data = sqlio.read_sql_query(query, con)

        logging.info("data size = " + str(len(data)))
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
            startProcessGeotabFiles(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, data, weekstart, weekend)
        logging.info("week=" + str(weeks[i]) + ",source=" + source + "  FINISHED")


def recover():
    FOLDER_PATH = "/home/ec2-user/regressiontest/"

    #FOLDER_PATH = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/"

    source = "MENTOR_GEOTAB"

    weeks = {0: ['2019-09-29', '2019-10-05'],
             1: ['2019-10-06', '2019-10-12'],
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
             13: ['2019-12-29', '2020-01-04'],
             14: ['2020-01-05', '2020-01-11'],
             15: ['2020-01-12', '2020-01-18'],
             16: ['2020-01-19', '2020-01-25'],
             17: ['2020-01-26', '2020-02-01'],
             18: ['2020-02-02', '2020-02-08'],
             19: ['2020-02-09', '2020-02-15'],
             20: ['2020-02-16', '2020-02-22'],
             21: ['2020-02-23', '2020-02-29'],
             22: ['2020-03-01', '2020-03-07'],
             23: ['2020-03-08', '2020-03-14'],
             24: ['2020-03-15', '2020-03-21']}

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename=FOLDER_PATH + "jsonfiles/" + datetime.now().strftime('%d-%m_%H-%M') + 'logger.log')
    urllib3_logger = logging.getLogger('urllib3')
    urllib3_logger.setLevel(logging.CRITICAL)
    for i in range(0, 3):
        weekstart = weeks.get(i)[0]
        weekend = weeks.get(i)[1]
        logging.info("week=" + str(weeks[i]) + ",source=" + source + " STARTED")

        RESULT_FILE_PATH = "jsonfiles/" + weekstart + "_" + weekend + "#" + source + "/"
        resultfilename = weekstart + "_" + weekend + "#" + source
        os.makedirs(FOLDER_PATH + RESULT_FILE_PATH, exist_ok=True)
        resultfilename = weekstart + "_" + weekend + "#" + source
        data = pd.read_csv(
            "/home/ec2-user/pmanalysis/" +weekstart + "_" + weekend+"/"+ weekstart + "_" + weekend+"#MENTOR_GEOTAB/" + weekstart + "_" + weekend+"#MENTOR_GEOTABdataafterprocess.csv")

        #data = pd.read_csv(
        #    "/Users/omerorhan/Documents/EventDetection/pmanalysis/" + weekstart + "_" + weekend + "/" + weekstart + "_" + weekend + "#MENTOR_GEOTAB/" + weekstart + "_" + weekend + "#MENTOR_GEOTABdataafterprocess.csv")

        data['FOLDER_PATH'] = FOLDER_PATH
        data['RESULT_FILE_PATH'] = RESULT_FILE_PATH
        data['resultfilename'] = resultfilename
        data = data[(data['STATUS'] != '20  0') & (data['STATUS'] != 200)]
        logging.info("total data=" + str(len(data)))
        startProcessGeotabFiles(FOLDER_PATH, RESULT_FILE_PATH, resultfilename, data, weekstart, weekend)
        logging.info("week=" + str(weeks[i]) + ",source=" + source + "  FINISHED")


killoldtelematicsprocess()
startTelematics(FOLDER_PATH)
# connect2Redshift()
recover()

'''
from datetime import date, timedelta


def all_sundays(year):
    # January 1st of the given year
    dt = date(year, 1, 1)
    # First Sunday of the given year
    dt += timedelta(days=dt.weekday() + 2)
    while dt.year == year:
        yield dt
        dt += timedelta(days=7)


for key,value in weeks.items():
    year, month, day = value[0].split('-')
    day_name1 = datetime.date(int(year), int(month), int(day))
    year2, month2, day2 = value[1].split('-')
    day_name2= datetime.date(int(year2), int(month2), int(day2))
    print(str(value) +"-"+day_name1.strftime("%A") +"-"+ day_name2.strftime("%A")+"-"+str(int(day2)-int(day)))
    
2019-10-06-2019-10-12,GEOTAB=78814,NONGEOTAB=60023
2019-10-13-2019-10-19,GEOTAB=82500,NONGEOTAB=70405
2019-10-20-2019-10-26,GEOTAB=90641,NONGEOTAB=88484
2019-10-27-2019-11-02,GEOTAB=90686,NONGEOTAB=91421
2019-11-03-2019-11-09,GEOTAB=101405,NONGEOTAB=99710
2019-11-10-2019-11-16,GEOTAB=115601,NONGEOTAB=107327
2019-11-17-2019-11-23,GEOTAB=125474,NONGEOTAB=113711
2019-11-24-2019-11-30,GEOTAB=118656,NONGEOTAB=100955
2019-12-01-2019-12-07,GEOTAB=147935,NONGEOTAB=118646
2019-12-08-2019-12-14,GEOTAB=153897,NONGEOTAB=120535
2019-12-15-2019-12-21,GEOTAB=157417,NONGEOTAB=122694
2019-12-22-2019-12-28,GEOTAB=110414,NONGEOTAB=82142
2019-12-29-2020-01-04,GEOTAB=125871,NONGEOTAB=77649
2020-01-05-2020-01-11,GEOTAB=144681,NONGEOTAB=82061
2020-01-12-2020-01-18,GEOTAB=141964,NONGEOTAB=77943
2020-01-19-2020-01-25,GEOTAB=141224,NONGEOTAB=74709
2020-01-26-2020-02-01,GEOTAB=143605,NONGEOTAB=71813
2020-02-02-2020-02-08,GEOTAB=142910,NONGEOTAB=68174
2020-02-09-2020-02-15,GEOTAB=143483,NONGEOTAB=67765
2020-02-16-2020-02-22,GEOTAB=144737,NONGEOTAB=66644
2020-02-23-2020-02-29,GEOTAB=147352,NONGEOTAB=67943
2020-03-01-2020-03-07,GEOTAB=149140,NONGEOTAB=69015



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
         13: ['2019-12-29', '2020-01-04'],
         14: ['2020-01-05', '2020-01-11'],
         15: ['2020-01-12', '2020-01-18'],
         16: ['2020-01-19', '2020-01-25'],
         17: ['2020-01-26', '2020-02-01'],
         18: ['2020-02-02', '2020-02-08'],
         19: ['2020-02-09', '2020-02-15'],
         20: ['2020-02-16', '2020-02-22'],
         21: ['2020-02-23', '2020-02-29'],
         22: ['2020-03-01', '2020-03-07'],
         23: ['2020-03-08', '2020-03-14'],
         24: ['2020-03-15', '2020-03-21']}

'''
