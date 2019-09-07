import pandas as pd


def processCSV():
    exampleList = pd.read_csv("/Users/omerorhan/Documents/EventDetection/regression_server/dataconversion/example.csv",
                              index_col=False)
    result = "select driver_id, trip_id,s3_key from trip_file where "
    query = []
    count = 0
    for index, row in exampleList.iterrows():
        count = count + 1
        query.append("(driver_id = '" + str(row[0]) + "' and trip_id='" + str(row[1]) + "') or ")
        if count % 10 == 0:
            break
            print(count)

    text_file = open("/Users/omerorhan/Documents/EventDetection/regression_server/dataconversion/sql.txt", "w")
    text_file.write(result + ''.join(query)[:-3])
    text_file.close()


def getS3Key():
    import mysql.connector
    cnx = mysql.connector.connect(user='prodauroramaster', password='u9UQmPk6BtkP3V2Cyfuufvfy8Wm3jGhW5tTtc7FJt',
                                  host='production-aurora-mentor.cluster-ro-cikfoxuzuwyj.us-west-2.rds.amazonaws.com',
                                  database='telematics')

    text_file = open("/Users/omerorhan/Documents/EventDetection/regression_server/dataconversion/sql.txt", "r").read()
    cursor = cnx.cursor()
    cursor.execute(text_file)
    df_result = pd.DataFrame(columns=['driver_id', 'trip_id', 's3_key'])
    for (driver_id, trip_id, s3key) in cursor:
        df_result = df_result.append({'driver_id': driver_id, 'trip_id': trip_id, 's3_key': s3key}, ignore_index=True)
    print(df_result.shape)
    cnx.close()
    df_result.to_csv("/Users/omerorhan/Documents/EventDetection/regression_server/dataconversion/final.csv")


processCSV()
getS3Key()
df = pd.DataFrame(columns=['A'])
for i in range(5):
    df = df.append({'A': i}, ignore_index=True)
print(df)





'''


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


'''