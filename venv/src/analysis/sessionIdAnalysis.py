import mysql.connector
import psycopg2
from datetime import datetime
import pandas as pd
from datetime import *
import requests
import json


def connectRedshift():
    con = psycopg2.connect(dbname='productionreportingdb',
                           host='edriving-telematics-production-reporting-db.c4bepljyphir.us-west-2.redshift.amazonaws.com',
                           port='5439', user='telematics_readonly', password='telematicsReadOnly123')
    cur = con.cursor()

    cur.execute(
        "select driver_id, local_date,source, trip_id from trips where source in ('MENTOR_NON_GEOTAB', 'MENTOR_GEOTAB') and local_date >= '2019-11-01' and status='SUCCESS' LIMIT 10;")

    result = pd.DataFrame(columns=['driver_id', 'local_date', 'source', 'trip_id'])
    if cur.rowcount > 0:
        for row in iter(cur.fetchall()):
            date = datetime.strptime(str(row[1]), '%Y-%m-%d').date()
            new_row = {'driver_id': row[0], 'local_date': date, 'source': row[2], 'trip_id': row[3]}
            result = result.append(new_row, ignore_index=True)
    result.to_csv("/Users/omerorhan/Documents/EventDetection/JIRA/JIRA-486/analysis/redshift.csv", date_format='%Y%m%d')


def connectAurora():
    dataframe = pd.read_csv("/Users/omerorhan/Documents/EventDetection/JIRA/JIRA-486/analysis/redshift.csv")

    # query = "select session_id from amzl_geotab.user_device_pairs where "
    # for index, row in dataframe.iterrows():
    #    date_object = datetime.strptime(row["local_date"], '%Y-%m-%d').date()
    #    thedayafter = date_object + timedelta(days=1)
    #    query = query + "(user_id = '" + str(row["driver_id"]) + "' and active_from between '" + row[
    #        "local_date"] + "' and '" + str(thedayafter) + "' ) OR "

    # query = query[:-3]
    # print(query)

    con = mysql.connector.connect(user='prodauroramaster', password='u9UQmPk6BtkP3V2Cyfuufvfy8Wm3jGhW5tTtc7FJt',
                                  host='production-aurora-mentor.cluster-ro-cikfoxuzuwyj.us-west-2.rds.amazonaws.com',
                                  database='amzl_geotab')

    cur = con.cursor(buffered=True)
    result = pd.DataFrame(
        columns=['driver_id', 'trip_id','local_date', 'source', 'score', 'distraction_count', 'session_id', 'device', 'platform',
                 'appVersion'])

    for index, row in dataframe.iterrows():
        flag = False
        session_id=''
        if row['source'] == 'MENTOR_GEOTAB':
            thedayafter = datetime.strptime(row["local_date"], '%Y-%m-%d').date() + timedelta(days=1)
            query = "select session_id from amzl_geotab.user_device_pairs where (user_id = '" + str(
                row["driver_id"]) + "' and active_from between '" + row["local_date"] + "' and '" + str(
                thedayafter) + "' ) "
            cur.execute(query)
            for session in iter(cur.fetchall()):
                session_id = session
                elastic = requests.get(
                    "https://search-edriving-elk-2-x7jeuzd5bckjxkgtyohttwt66y.us-west-2.es.amazonaws.com/telematics-production-api-*/_search?q=method:POST%20AND%20path:" + str(
                        session[0]) + "&size=10&_source_includes=platform,device,app,appVersion").content.decode(
                    "utf-8")
        elif row['source'] == 'MENTOR_NON_GEOTAB':
            session_id = str(row['trip_id']).split('-')[1]
            elastic = requests.get(
                "https://search-edriving-elk-2-x7jeuzd5bckjxkgtyohttwt66y.us-west-2.es.amazonaws.com/telematics-production-api-*/_search?q=method:POST%20AND%20path:" + str(
                    session_id) + "&size=10&_source_includes=platform,device,app,appVersion").content.decode(
                "utf-8")
        response_json = json.loads(elastic)
        if response_json.get("hits") == None or response_json.get("hits") == "" or response_json.get("hits").get(
                "hits") == None or response_json.get("hits").get("hits") == "":
            continue
        for item in response_json.get("hits").get("hits"):
            if item.get("_source") == None or item.get("_source") == "":
                continue
            if item.get("_source").get("platform") != "AWS Lambda" and len(item.get("_source")) != 0:
                elasticrow = item.get("_source")
                flag = True
                new_row = {'driver_id': row["driver_id"],'trip_id': row["trip_id"], 'local_date': row[1], 'source': row[2],
                           'session_id': str(session_id),
                           'appVersion': elasticrow.get('appVersion'), 'device': elasticrow.get('device'),
                           'platform': elasticrow.get('platform'), 'score': row['score'],
                           'distraction_count': row['manipu_count']}
                result = result.append(new_row, ignore_index=True)
                break

        if index % 100 == 0:
            print(index)
        if index == 1000:
            break
        if not flag:
            print(row["driver_id"], '-', row["local_date"])
    result.to_csv("/Users/omerorhan/Documents/EventDetection/JIRA/JIRA-486/analysis/final.csv")
    print(result)
    con.close()


# connectRedshift()
connectAurora()

'''
select t.driver_id, t.local_date,t.source, t.trip_id,s.score, count(*) from trips t join trip_scores s on s.trip_id=t.trip_id  left join trip_events e on t.trip_id = e.trip_id  where 
 t.source in ('MENTOR_NON_GEOTAB', 'MENTOR_GEOTAB') and t.local_date >= '2019-11-04' and t.local_date < '2019-11-10' and t.status='SUCCESS' 
 and (e.event_id='PHONE_MANIPULATION') group by t.driver_id,t.local_date, t.source, t.trip_id,s.score;
 
 
 select t.trip_id, nvl(m.countman,0) from trips t left join (
select e.trip_id, count(*) as countman from trip_events e where e.event_id='PHONE_MANIPULATION' and e.timestamp>'2019-11-10' 
 group by e.trip_id) m on t.trip_id=m.trip_id where t.local_date >'2019-11-10'


select t.driver_id, t.local_date,t.source, t.trip_id,s.score, nvl(m.countman,0) as manipu_count from trips t join trip_scores s on s.trip_id=t.trip_id left join (
select e.trip_id, count(*) as countman from trip_events e where e.event_id='PHONE_MANIPULATION' and  e.timestamp >= '2019-11-03' and e.timestamp < '2019-11-11' group by e.trip_id) m 
on t.trip_id=m.trip_id where t.local_date >= '2019-11-04' and t.local_date < '2019-11-10' and t.status='SUCCESS' and  t.source in ('MENTOR_NON_GEOTAB', 'MENTOR_GEOTAB') 


select t.driver_id, t.local_date,t.source, t.trip_id,s.score, nvl(m.countman,0) as manipu_count from trips t join trip_scores s on s.trip_id=t.trip_id left join (
select e.trip_id, count(*) as countman from trip_events e where e.event_id='PHONE_MANIPULATION' and  e.timestamp >= '2019-11-03' and e.timestamp < '2019-11-11' group by e.trip_id) m 
on t.trip_id=m.trip_id where t.local_date >= '2019-11-04' and t.local_date < '2019-11-10' and t.status='SUCCESS' and  t.source in ('MENTOR_NON_GEOTAB', 'MENTOR_GEOTAB') 

'''
