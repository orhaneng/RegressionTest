import os

os.system("source activate base")
import pandas as pd
import shutil


def processMixedOrderedRawFiles():
    # source_folder_path = "/Users/omerorhan/Documents/EventDetection/regression_server/raw/"
    source_folder_path = "/home/ec2-user/omer/dataForRegressionTestRaw/"
    # destination_folder_path = "/Users/omerorhan/Documents/EventDetection/regression_server/raw/tripfiles/"
    destination_folder_path = "/home/ec2-user/omer/tripfiles/"
    f = []
    processcount = 0
    for (dirpath, dirnames, filenames) in walk(source_folder_path):
        for filename in filenames:
            if filename.endswith('.bin_v2.gz') and filename.startswith('trip'):
                splitlist = filename.split(".")
                if len(splitlist) == 5:
                    driverid = splitlist[1]
                    if not os.path.exists(destination_folder_path + driverid):
                        os.mkdir(destination_folder_path + driverid)
                    copyfile(source_folder_path + filename,
                             destination_folder_path + driverid + "/" + filename)
                    processcount = processcount + 1
                    print(
                        source_folder_path + filename + " copied to " + destination_folder_path + driverid + "/" + filename)
    print(str(processcount) + "trips added")


def processCSVtoGetS3key():
    import mysql.connector
    exampleList = pd.read_csv("/Users/omerorhan/Documents/EventDetection/PMANALYSIS_TLM_112/data.csv",
                              index_col=False)
    cnx = mysql.connector.connect(user='omer', password='3$@Wed#f%g67dfg34%gH2s8',
                                  host='prod-telematics-aurora-cluster.cluster-ro-cikfoxuzuwyj.us-west-2.rds.amazonaws.com',
                                  database='telematics')
    cursor = cnx.cursor()
    df_result = pd.DataFrame(columns=['driver_id', 'trip_id', 's3_key'])
    result = "select driver_id, trip_id,s3_key from trip_file where "
    query = []
    for i, row in exampleList.iterrows():
        query.append("(driver_id = '" + str(row[1]) + "' and trip_id='" + str(row[0]) + "') or ")
        #if i > 1000:
            #print(i)
    cursor.execute(result + "".join(query)[:-3])
    for (driver_id, trip_id, s3key) in cursor:
        df_result = df_result.append({'driver_id': driver_id, 'trip_id': trip_id, 's3_key': s3key},
                                     ignore_index=True)
        result = "select driver_id, trip_id,s3_key from trip_file where "
        query = []
    df_result.to_csv("/Users/omerorhan/Documents/EventDetection/PManalysis/weekly_trips_final.csv",
                     index=False)
    cnx.close()


processCSVtoGetS3key()
def divideDriversIntoPools():
    # PATH = "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/"
    PATH = "/home/ec2-user/regressiontest/"
    exampleList = pd.read_csv(PATH + "dataconversion/final.csv", index_col=False)
    groupedList = exampleList.groupby("driver_id").count().reset_index().sort_values('trip_id', ascending=True)
    groupedList.columns = ['driver_id', 'count', 'count1']
    groupedList = groupedList[["driver_id", 'count']]
    data1000K = pd.DataFrame(columns=['driver_id', 'count'])
    count = 0
    countdriver = 0
    for index, row in groupedList.iterrows():
        count = count + row.loc['count']
        countdriver = countdriver + 1
        new_row = {'driver_id': row.loc['driver_id'], 'count': row.loc['count']}
        data1000K = data1000K.append(new_row, ignore_index=True)
        shutil.copytree(PATH + "tripfiles/100000/" + str(row.loc['driver_id']),
                        PATH + "tripfiles/50000/" + str(row.loc['driver_id']))
        print(countdriver)
        if count > 50000:
            break
    print(count)
    print(data1000K.shape)


def connect2Redshift():
    import psycopg2
    from datetime import datetime
    con = psycopg2.connect(dbname='productionreportingdb',
                           host='edriving-telematics-production-reporting-db.c4bepljyphir.us-west-2.redshift.amazonaws.com',
                           port='5439', user='telematics_readonly', password='telematicsReadOnly123')
    cur = con.cursor()
    cur.execute(
        "select timestamp from trips where trip_id='300760431-6e12a02306ba410cb8bb9393775deedc' and driver_id='300760431';")

    dataframe = pd.read_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/dataconversion/amazon.csv")

    dataframe = dataframe.groupby(['driver_id', 'trip_id']).size().reset_index(name='Freq')[["driver_id", "trip_id"]]
    print(dataframe.shape)
    count = 0
    list = []
    result = pd.DataFrame(columns=['driver_id', 'trip_id', 's3_key', 'timestamp'])
    countsuccess = 0

    for index, row in dataframe.iterrows():
        if count % 100 == 0:
            con.close()
            con = psycopg2.connect(dbname='productionreportingdb',
                                   host='edriving-telematics-production-reporting-db.c4bepljyphir.us-west-2.redshift.amazonaws.com',
                                   port='5439', user='telematics_readonly', password='telematicsReadOnly123')
            cur = con.cursor()

        query = "select timestamp from trips where trip_id='" + str(row[1]) + "' and driver_id='" + str(row[0]) + "';"
        cur.execute(query)
        timestamp = ""
        if cur.rowcount > 0:
            timestamp = str(int(cur.fetchall()[0][0].timestamp() * 1000))
            countsuccess = countsuccess + 1
        list.append(timestamp)
        # new_row = {'driver_id': row[0], 'trip_id': row[1], 's3_key': row[2], 'timestamp': str(timestamp)}
        new_row = {'driver_id': row[0], 'trip_id': row[1], 's3_key': "", 'timestamp': str(timestamp)}
        result = result.append(new_row, ignore_index=True)
        count = count + 1
        print(count)
    print("countsuccess", countsuccess)

    print(result.shape)
    con.close()
    result.to_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/dataconversion/amazon2.csv")

    # print(datetime.timestamp(cur.fetchall()[0][11]))
    # print((cur.fetchall()[0][11].timestamp() * 1000))
    # timestamp = cur.fetchall()[0][11]
    # val_int = int((timestamp.timestamp() * 1000))
    # print(val_int)

    # val_fract = timestamp - val_int
    # 1568992956.16
    # print(cur.fetchall()[0][11].strftime("%s"))
    # 1569021756
    con.close()


# connect2Redshift()


def connect2Aurora():
    import mysql.connector

    con = mysql.connector.connect(user='prodauroramaster', password='u9UQmPk6BtkP3V2Cyfuufvfy8Wm3jGhW5tTtc7FJt',
                                  host='production-aurora-mentor.cluster-ro-cikfoxuzuwyj.us-west-2.rds.amazonaws.com',
                                  database='telematics')
    cur = con.cursor(buffered=True)
    dataframe = pd.read_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/dataconversion/amazon.csv")

    dataframe = dataframe.groupby(['driver_id', 'trip_id']).size().reset_index(name='Freq')[["driver_id", "trip_id"]]
    print(dataframe.shape)
    count = 0
    list = []
    result = pd.DataFrame(columns=['driver_id', 'trip_id', 's3_key', 'timestamp'])
    countsuccess = 0

    for index, row in dataframe.iterrows():
        if count % 100 == 0:
            con.close()
            con = mysql.connector.connect(user='prodauroramaster', password='u9UQmPk6BtkP3V2Cyfuufvfy8Wm3jGhW5tTtc7FJt',
                                          host='production-aurora-mentor.cluster-ro-cikfoxuzuwyj.us-west-2.rds.amazonaws.com',
                                          database='telematics')
            cur = con.cursor(buffered=True)

        query = "select end_time from telematics.driving_sessions where driver_id='" + str(
            row[0]) + "' and session_id = '" + \
                str(row[1].split('-')[1]) + "'"
        cur.execute(query)
        res = cur.fetchall()
        timestamp = ''
        if len(res) > 0:
            timestamp = res[0][0]
        new_row = {'driver_id': row[0], 'trip_id': row[1], 's3_key': "", 'timestamp': str(timestamp)}
        result = result.append(new_row, ignore_index=True)
        count = count + 1
        countsuccess = countsuccess + 1
        print(count)
    print("countsuccess", countsuccess)

    print(result.shape)
    con.close()
    result.to_csv(
        "/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/dataconversion/amazon3.csv")


# connect2Aurora()


def getsessionidindriver():
    import os
    batch_file_dir = '/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/tripfiles/non-armada/'
    log = []
    file_names = []
    driver_id_set = None
    # Get file names and directories
    driverCount = 0
    for root, dirs, files in os.walk(batch_file_dir):
        if driver_id_set == None:
            driver_id_set = dirs
            continue
        files.sort()
        driverCount = driverCount + 1
        file_names.append(files)
    input = []
    driverlist = []
    for idx in range(len(driver_id_set)):
        driverlist.append(driver_id_set[idx])
        sessionidlist = []
        if len(file_names[idx]) > 0:
            for jdx in range(len(file_names[idx])):
                sessionidlist.append(file_names[idx][jdx].split('_')[0])
        setsessionlist = list(set(sessionidlist))
        print(setsessionlist)


#getsessionidindriver()
'''

300423983	300423983-4d77a24858f64b2cac872960742cb1e2	trip.300423983.1568992956162.bin_v2.gz	365	1568992956160	1568994090538	300423983/4d77a24858f64b2cac872960742cb1e2_trip.300423983.1568992956162.bin_v2.gz	2019-09-20 08:41:36.0	
300423983-4d77a24858f64b2cac872960742cb1e2	300423983	9869.0	1079.0	true	false	2019-09-20	22.04	CAR	SUCCESS	GMT-04:00	2019-09-20 08:22:36.16	2019-09-20 08:41:38.143			false	Bayberry Dr, Cape May Court House, NJ	MANUAL_END	MENTOR_NON_GEOTAB


select count(*) from trips
where local_date >= '2019-08-01' and 
local_date < '2019-09-01'
and mode = 'CAR'
and status = 'SUCCESS'
and is_driver = true
and is_personal = false
and driver_id=300003577


select driver_id,trip_id from trips where driver_id in 
(select driver_id from (
select driver_id, DATE(local_date),count(*) from trips
where local_date >= '2019-08-01' and 
local_date < '2019-09-01'
and mode = 'CAR'
and status = 'SUCCESS'
and is_driver = true
and is_personal = false
and source = 'MENTOR'
group by driver_id, DATE(local_date) having count(*) >=5) data 
group by driver_id having count(*)>=15  order by RANDOM() LIMIT 600) and  
 local_date >= '2019-08-01' and 
local_date < '2019-09-01'
and mode = 'CAR'
and status = 'SUCCESS'
and is_driver = true
and is_personal = false
and source = 'MENTOR' 


select count(driver_id) from (
select driver_id, count(*) tripcount from trips where driver_id in 
(select driver_id from (
select driver_id, DATE(local_date),count(*) from trips
where local_date >= '2019-08-01' and 
local_date < '2019-09-01'
and mode = 'CAR'
and status = 'SUCCESS'
and is_driver = true
and is_personal = false
and source = 'MENTOR'
group by driver_id, DATE(local_date) having count(*) >=5) data 
group by driver_id having count(*)>=15  order by RANDOM() limit 600) and  
 local_date >= '2019-08-01' and 
local_date < '2019-09-01'
and mode = 'CAR'
and status = 'SUCCESS'
and is_driver = true
and is_personal = false
and source = 'MENTOR' group by driver_id order by tripcount desc)



--106276   limit 600



select driver_id from (
select driver_id, DATE(local_date),count(*) from trips
where local_date >= '2019-08-01' and 
local_date < '2019-09-01'
and mode = 'CAR'
and status = 'SUCCESS'
and is_driver = true
and is_personal = false
and source = 'MENTOR'
group by driver_id, DATE(local_date) having count(*) >=5) data 
group by driver_id having count(*)>=15 order by RANDOM()  limit 600


select * from telematics.trip_file where (trip_id,driver_id) in (select concat(driver_id,'-',session_id) as trip_id,driver_id
from amzl_geotab.user_device_pairs p
         inner join telematics.driving_sessions s using (session_id)
where p.type = 1 and s.driver_id in (select data.user_id from (
select user_id, count(*) as  tripcount
from amzl_geotab.user_device_pairs p
         inner join telematics.driving_sessions s using (session_id)
where p.type = 1 group by user_id having count(*) >=10 
order by rand() desc limit 1500) data)
order by p.id desc);

'''
