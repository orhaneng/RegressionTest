import mysql.connector
import psycopg2
from datetime import datetime
import pandas as pd
from datetime import *
import requests
import json
from multiprocessing import Pool
import tqdm


def multi_run_wrapper(args):
    return multipool(*args)


def multipool(driver_id, local_date, source, trip_id, score,
              distance, manipu_count):
    flag = False
    new_row = None
    if source == 'MENTOR_GEOTAB':
        con = mysql.connector.connect(user='prodauroramaster', password='u9UQmPk6BtkP3V2Cyfuufvfy8Wm3jGhW5tTtc7FJt',
                                      host='production-aurora-mentor.cluster-ro-cikfoxuzuwyj.us-west-2.rds.amazonaws.com',
                                      database='amzl_geotab')
        cur = con.cursor(buffered=True)
        thedayafter = datetime.strptime(local_date, '%Y-%m-%d').date() + timedelta(days=1)
        query = "select session_id from amzl_geotab.user_device_pairs where (user_id = '" + str(
            driver_id) + "' and active_from between '" + local_date + "' and '" + str(
            thedayafter) + "' ) "
        cur.execute(query)
        new_row = None
        if cur.rowcount == 0:
            query = "select session_id from amzl_geotab.user_device_pairs_bak where (user_id = '" + str(
                driver_id) + "' and active_from between '" + local_date + "' and '" + str(
                thedayafter) + "' ) "
            cur.execute(query)
        for session in iter(cur.fetchall()):
            session_id = session
            elastic = requests.get(
                "https://search-edriving-elk-2-x7jeuzd5bckjxkgtyohttwt66y.us-west-2.es.amazonaws.com/telematics-production-api-*/_search?q=method:POST%20AND%20path:" + str(
                    session[0]) + "&size=10&_source_includes=platform,device,app,appVersion").content.decode(
                "utf-8")
            response_json = json.loads(elastic)
            if response_json.get("hits") == None or response_json.get("hits") == "" or response_json.get(
                    "hits").get(
                "hits") == None or response_json.get("hits").get("hits") == "":
                continue
            for item in response_json.get("hits").get("hits"):
                if item.get("_source") == None or item.get("_source") == "":
                    continue
                if item.get("_source").get("platform") != "AWS Lambda" and len(item.get("_source")) != 0:
                    elasticrow = item.get("_source")
                    flag = True
                    new_row = {'driver_id': driver_id, 'trip_id': trip_id, 'local_date': local_date,
                               'source': source,
                               'session_id': str(session_id[0]),
                               'appVersion': elasticrow.get('appVersion'), 'device': elasticrow.get('device'),
                               'platform': elasticrow.get('platform'), 'score': score,
                               'distraction_count': manipu_count, 'distance': distance}
                    con.close()
                    break
        con.close()

    elif source == 'MENTOR_NON_GEOTAB':
        session_id = str(trip_id).split('-')[1]
        elastic = requests.get(
            "https://search-edriving-elk-2-x7jeuzd5bckjxkgtyohttwt66y.us-west-2.es.amazonaws.com/telematics-production-api-*/_search?q=method:POST%20AND%20path:" + str(
                session_id) + "&size=10&_source_includes=platform,device,app,appVersion").content.decode(
            "utf-8")
        response_json = json.loads(elastic)
        if response_json.get("hits") == None and response_json.get("hits") == "" and response_json.get("hits").get(
                "hits") == None and response_json.get("hits").get("hits") == "":
            return new_row
        for item in response_json.get("hits").get("hits"):
            if item.get("_source") == None or item.get("_source") == "":
                continue
            if item.get("_source").get("platform") != "AWS Lambda" and len(item.get("_source")) != 0:
                elasticrow = item.get("_source")
                flag = True
                new_row = {'driver_id': driver_id, 'trip_id': trip_id, 'local_date': local_date,
                           'source': source,
                           'session_id': str(session_id),
                           'appVersion': elasticrow.get('appVersion'), 'device': elasticrow.get('device'),
                           'platform': elasticrow.get('platform'), 'score': score,
                           'distraction_count': manipu_count, 'distance': distance}
                break
    return new_row


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
    dataframe = pd.read_csv("/Users/omerorhan/Documents/EventDetection/JIRA/JIRA-486/analysis/redshiftwithdistance.csv")
    result = pd.DataFrame(
        columns=['driver_id', 'trip_id', 'local_date', 'source', 'score', 'distance', 'distraction_count', 'session_id',
                 'device',
                 'platform',
                 'appVersion'])

    # for index, row in dataframe.iterrows():
    pool = Pool(6)

    input = []
    for index, row in dataframe.iterrows():
        if index > 50000 and index < 100000:
            input.append(
                tuple((row['driver_id'], row['local_date'], row['source'], row['trip_id'], row['score'],
                       row['distance'], row['manipu_count'])))

    try:
        with pool as p:
            item = list(tqdm.tqdm(p.imap(multi_run_wrapper, input), total=len(input)))
            if item != None:
                for row in item:
                    result = result.append(row, ignore_index=True)



    except Exception as e:
        print(e)
        pool.terminate()
        pool.join()
        exit()
    result.to_csv("/Users/omerorhan/Documents/EventDetection/JIRA/JIRA-486/analysis/final.csv")
    print(result)


# connectRedshift()
# connectAurora()


def mergefiles():
    # part1 = pd.read_csv("/Users/omerorhan/Documents/EventDetection/JIRA/JIRA-486/analysis/final0-50000.csv", index_col=False)
    # part2 = pd.read_csv("/Users/omerorhan/Documents/EventDetection/JIRA/JIRA-486/analysis/final50000-100000.csv", index_col=False)
    # part3 = pd.read_csv("/Users/omerorhan/Documents/EventDetection/JIRA/JIRA-486/analysis/final100000-170000.csv")
    finalall = pd.read_csv("/Users/omerorhan/Documents/EventDetection/JIRA/JIRA-486/analysis/final_all.csv")
    # finalall =finalall.drop(columns=["Unnamed: 0","Unnamed: 0.1","Unnamed: 0.1.1"])
    print(finalall.head())
    # finalall = pd.concat([finalallf,part3])
    finalall.to_csv("/Users/omerorhan/Documents/EventDetection/JIRA/JIRA-486/analysis/final_all.csv")


# mergefiles()


def analysis():
    finalall = pd.read_csv("/Users/omerorhan/Documents/EventDetection/JIRA/JIRA-486/analysis/final_all.csv")

    # finalall.to_csv("/Users/omerorhan/Documents/EventDetection/JIRA/JIRA-486/analysis/final_all.csv")
    filtered = finalall[finalall["appVersion"] != 1.19]
    mostcommondevices = finalall.groupby("device")['trip_id'].count().nlargest(20)
    mostcommondevices = mostcommondevices.index.get_level_values(0)
    filtered = filtered[filtered["device"].isin(mostcommondevices)]
    # mostcommondevices.to_csv("/Users/omerorhan/Documents/EventDetection/JIRA/JIRA-486/analysis/countbydevices.csv")
    # print(mostcommondevices)
    # groupbymanipulation = filtered.groupby(['source', 'device'], as_index=False)[
    #    'distraction_count'].mean().sort_values(
    #    ['source', 'distraction_count'], ascending=False)

    # finalall[['distance', 'distraction_count']] = finalall[['distance', 'distraction_count']].astype(float)
    groupbymanipulation = filtered.groupby(['source', 'device'], as_index=False).apply(
        lambda x: (x['distraction_count'].sum() / (x['distance'].sum() / (1000 * 1.6))) * 1000)

    groupbymanipulation.to_csv(
        "/Users/omerorhan/Documents/EventDetection/JIRA/JIRA-486/analysis/groupbydeviceTOP20.csv")
    # print("count of different devices:", len(finalall["device"].unique()))

    # print(groupbymanipulation)


analysis()
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


select t.driver_id, t.local_date,t.source, t.trip_id,s.score,t.distance, nvl(m.countman,0) as manipu_count from trips t join trip_scores s on s.trip_id=t.trip_id left join (
select e.trip_id, count(*) as countman from trip_events e where e.event_id='PHONE_MANIPULATION' and  e.timestamp >= '2019-11-03' and e.timestamp < '2019-11-11' group by e.trip_id) m 
on t.trip_id=m.trip_id where t.local_date >= '2019-11-04' and t.local_date < '2019-11-10' and t.status='SUCCESS' and  t.source in ('MENTOR_NON_GEOTAB', 'MENTOR_GEOTAB') 

'''
