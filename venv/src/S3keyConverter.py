import pandas as pd
import mysql.connector


def processCSVtoGetS3key():
    exampleList = pd.read_csv("/Users/omerorhan/Documents/EventDetection/regression_server/dataconversion/example.csv",
                              index_col=False)
    cnx = mysql.connector.connect(user='prodauroramaster', password='u9UQmPk6BtkP3V2Cyfuufvfy8Wm3jGhW5tTtc7FJt',
                                  host='production-aurora-mentor.cluster-ro-cikfoxuzuwyj.us-west-2.rds.amazonaws.com',
                                  database='telematics')
    cursor = cnx.cursor()
    df_result = pd.DataFrame(columns=['driver_id', 'trip_id', 's3_key'])
    result = "select driver_id, trip_id,s3_key from trip_file where "
    query = []
    for i, row in exampleList.iterrows():
        query.append("(driver_id = '" + str(row[0]) + "' and trip_id='" + str(row[1]) + "') or ")
        if i % 1000 == 0:
            print(i)
            cursor.execute(result + "".join(query)[:-3])
            for (driver_id, trip_id, s3key) in cursor:
                df_result = df_result.append({'driver_id': driver_id, 'trip_id': trip_id, 's3_key': s3key},
                                             ignore_index=True)
                result = "select driver_id, trip_id,s3_key from trip_file where "
                query = []
    df_result.to_csv("/Users/omerorhan/Documents/EventDetection/regression_server/dataconversion/final.csv",
                     index=False)
    cnx.close()


# processCSVtoGetS3key()

exampleList = pd.read_csv("/Users/omerorhan/Documents/EventDetection/regression_server/dataconversion/final.csv",
                          index_col=False)

groupedList = exampleList.groupby("driver_id").count().reset_index()
groupedList = groupedList.sort_values('trip_id', ascending=True)
# print(groupedList.sample(6).sum())
groupedList.columns = ['driver_id', 'count', 'count1']
# print(groupedList[["driver_id",'count']].to_csv("/Users/omerorhan/Documents/EventDetection/regression_server/dataconversion/countbydriver.csv",
#                              index=False))


groupedList[["driver_id", 'count']].to_csv(
    "/Users/omerorhan/Documents/EventDetection/regression_server/dataconversion/countbydriver.csv", index=False)

groupedList = groupedList[["driver_id", 'count']]
# print(groupedList)

data1000K = pd.DataFrame(columns=['driver_id', 'count'])
count = 0
new_row = {}
for index, row in groupedList.iterrows():
    count = count + row.loc['count']
    new_row = {'driver_id': row.loc['driver_id'], 'count': row.loc['count']}
    data1000K = data1000K.append(new_row, ignore_index=True)
    if count > 1000:
        break

print(data1000K)
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
