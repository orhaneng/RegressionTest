#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 14:19:36 2017

@author: yichuanniu
"""

import boto3
import psycopg2

def create_mentor_dynamodb_tables():
    client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')
    client.create_table(
    AttributeDefinitions=[{"AttributeName":"id","AttributeType":"N"}],
    TableName='local-driver',
    KeySchema=[{"KeyType":"HASH","AttributeName":"id"}],
    ProvisionedThroughput = {"WriteCapacityUnits":5,"ReadCapacityUnits":5},)

    client.create_table(
    AttributeDefinitions=[{"AttributeName":"driverId","AttributeType":"N"},
                          {"AttributeName":"localDate","AttributeType":"S"}],
    TableName='local-driver-local-daily-score',
    KeySchema=[{"KeyType":"HASH","AttributeName":"driverId"},
               {"KeyType":"RANGE","AttributeName":"localDate"}],
    ProvisionedThroughput = {"WriteCapacityUnits":5,"ReadCapacityUnits":5},)

    client.create_table(
    AttributeDefinitions=[{"AttributeName":"driverId","AttributeType":"N"},
                          {"AttributeName":"localDate","AttributeType":"S"}],
    TableName='local-driver-local-weekly-score',
    KeySchema=[{"KeyType":"HASH","AttributeName":"driverId"},
               {"KeyType":"RANGE","AttributeName":"localDate"}],
    ProvisionedThroughput = {"WriteCapacityUnits":5,"ReadCapacityUnits":5},)

    client.create_table(
    AttributeDefinitions=[{"AttributeName":"id","AttributeType":"N"}],
    TableName='local-group',
    KeySchema=[{"KeyType":"HASH","AttributeName":"id"}],
    ProvisionedThroughput = {"WriteCapacityUnits":5,"ReadCapacityUnits":5},)

    client.create_table(
    AttributeDefinitions=[{"AttributeName":"driverId","AttributeType":"N"}],
    TableName='local-real-time-event',
    KeySchema=[{"KeyType":"HASH","AttributeName":"driverId"}],
    ProvisionedThroughput = {"WriteCapacityUnits":5,"ReadCapacityUnits":5},)

    client.create_table(
    AttributeDefinitions=[{"AttributeName":"driverId","AttributeType":"N"},
                          {"AttributeName":"id","AttributeType":"N"},
                          {"AttributeName":"localDate","AttributeType":"S"},
                          {"AttributeName":"timestamp","AttributeType":"S"}],
    TableName='local-trip',
    KeySchema=[{"KeyType":"HASH","AttributeName":"id"}],
    ProvisionedThroughput = {"WriteCapacityUnits":5,"ReadCapacityUnits":5},
    GlobalSecondaryIndexes=[{"IndexName":"driverId-timestamp-index",
                             "Projection":{"ProjectionType":"ALL"},
                             "ProvisionedThroughput":{"WriteCapacityUnits":5,"ReadCapacityUnits":5},
                             "KeySchema":[{"KeyType":"HASH","AttributeName":"driverId"},{"KeyType":"RANGE","AttributeName":"timestamp"}]},
                          {"IndexName":"driverId-localDate-index",
                           "Projection":{"ProjectionType":"ALL"},
                           "ProvisionedThroughput":{"WriteCapacityUnits":5,"ReadCapacityUnits":5},
                           "KeySchema":[{"KeyType":"HASH","AttributeName":"driverId"},
                                        {"KeyType":"RANGE","AttributeName":"localDate"}]}])

    client.create_table(
    AttributeDefinitions=[{"AttributeName":"tripId","AttributeType":"N"}],
    TableName='local-trip-detail',
    KeySchema=[{"KeyType":"HASH","AttributeName":"tripId"}],
    ProvisionedThroughput = {"WriteCapacityUnits":20,"ReadCapacityUnits":30},)

    client.create_table(
    AttributeDefinitions=[{"AttributeName":"filename","AttributeType":"S"}],
    TableName='local-files',
    KeySchema=[{"KeyType":"HASH","AttributeName":"filename"}],
    ProvisionedThroughput = {"WriteCapacityUnits":5,"ReadCapacityUnits":5},)

    client.create_table(
    AttributeDefinitions=[{"AttributeName":"id","AttributeType":"S"}],
    TableName='local-statistics',
    KeySchema=[{"KeyType":"HASH","AttributeName":"id"}],
    ProvisionedThroughput = {"WriteCapacityUnits":5,"ReadCapacityUnits":2},)

    client.create_table(
    AttributeDefinitions=[{"AttributeName":"driverId","AttributeType":"S"},
                          {"AttributeName":"localDate","AttributeType":"S"}],
    TableName='local-driver-local-score-history',
    KeySchema=[{"KeyType":"HASH","AttributeName":"driverId"},
               {"KeyType":"RANGE","AttributeName":"localDate"}],
    ProvisionedThroughput = {"WriteCapacityUnits":5,"ReadCapacityUnits":5})
    
    ## The followings are new schema for dal-v2
    client.create_table(
    AttributeDefinitions=[{"AttributeName":"t_id","AttributeType":"S"},
                          {"AttributeName":"d_id","AttributeType":"S"},
                          {"AttributeName":"t_start","AttributeType":"S"}],
    TableName='tlm_trip',
    KeySchema=[{"KeyType":"HASH","AttributeName":"t_id"}],
    ProvisionedThroughput = {"WriteCapacityUnits":20,"ReadCapacityUnits":6},
    GlobalSecondaryIndexes=[{"IndexName":"index-tlm_trip-by-driver-id","Projection":{"ProjectionType":"ALL"},
                             "ProvisionedThroughput":{"WriteCapacityUnits":20,"ReadCapacityUnits":5},
                             "KeySchema":[{"KeyType":"HASH","AttributeName":"d_id"},
                                          {"KeyType":"RANGE","AttributeName":"t_start"}]}],
    SSESpecification={"Enabled":True})
    
    client.create_table(
    AttributeDefinitions=[{"AttributeName":"t_id","AttributeType":"S"},
                          {"AttributeName":"chunk","AttributeType":"S"}],
    TableName='tlm_route',
    KeySchema=[{"KeyType":"HASH","AttributeName":"t_id"},
               {"KeyType":"RANGE","AttributeName":"chunk"}],
    ProvisionedThroughput = {"WriteCapacityUnits":500,"ReadCapacityUnits":50},
    SSESpecification={"Enabled":True})
    
    
    client.create_table(
    AttributeDefinitions=[{"AttributeName":"d_id","AttributeType":"S"},
                          {"AttributeName":"dt","AttributeType":"S"}],
    TableName='tlm_driver_scores',
    KeySchema=[{"KeyType":"HASH","AttributeName":"d_id"},
               {"KeyType":"RANGE","AttributeName":"dt"}],
    ProvisionedThroughput = {"WriteCapacityUnits":20,"ReadCapacityUnits":1},
    SSESpecification={"Enabled":True})


    client.create_table(
    AttributeDefinitions=[{"AttributeName":"t_id","AttributeType":"S"},
                          {"AttributeName":"chunk","AttributeType":"S"}],
    TableName='tlm_event',
    KeySchema=[{"KeyType":"HASH","AttributeName":"t_id"},
               {"KeyType":"RANGE","AttributeName":"chunk"}],
    ProvisionedThroughput = {"WriteCapacityUnits":50,"ReadCapacityUnits":50},
    SSESpecification={"Enabled":True})

    client.create_table(
    AttributeDefinitions=[{"AttributeName":"sgmnt","AttributeType":"S"},
                          {"AttributeName":"prov","AttributeType":"S"}],
    TableName='tlm_road_speed_limits',
    KeySchema=[{"KeyType":"HASH","AttributeName":"sgmnt"},
               {"KeyType":"RANGE","AttributeName":"prov"}],
    ProvisionedThroughput = {"WriteCapacityUnits":5,"ReadCapacityUnits":10},
    SSESpecification={"Enabled":True})

    client.create_table(
    AttributeDefinitions=[{"AttributeName":"a_id","AttributeType":"S"},
                          {"AttributeName":"sgmnt","AttributeType":"S"},
                          {"AttributeName":"prov","AttributeType":"S"}],
    TableName='tlm_road_speed_limits_activity',
    KeySchema=[{"KeyType":"HASH","AttributeName":"a_id"}],
    ProvisionedThroughput = {"WriteCapacityUnits":5,"ReadCapacityUnits":10},
    GlobalSecondaryIndexes=[{"IndexName":"index-tlm_road_speed_limits_activity-by-road_segment_id","Projection":{"ProjectionType":"ALL"},"ProvisionedThroughput":{"WriteCapacityUnits":5,"ReadCapacityUnits":10},"KeySchema":[{"KeyType":"HASH","AttributeName":"sgmnt"},{"KeyType":"RANGE","AttributeName":"prov"}]}],
    SSESpecification={"Enabled":True})

def delete_mentor_dynamodb_tables():
    client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')
    existing_tables = client.list_tables()['TableNames']
    if 'local-driver' in existing_tables:
        client.delete_table(TableName='local-driver')
    if 'local-driver-local-daily-score' in existing_tables:
        client.delete_table(TableName='local-driver-local-daily-score')
    if 'local-driver-local-score-history' in existing_tables:
        client.delete_table(TableName='local-driver-local-score-history')
    if 'local-driver-local-weekly-score' in existing_tables:
        client.delete_table(TableName='local-driver-local-weekly-score')
    if 'local-group' in existing_tables:
        client.delete_table(TableName='local-group')
    if 'local-real-time-event' in existing_tables:
        client.delete_table(TableName='local-real-time-event')
    if 'local-trip' in existing_tables:
        client.delete_table(TableName='local-trip')
    if 'local-trip-detail' in existing_tables:
        client.delete_table(TableName='local-trip-detail')
    if 'local-files' in existing_tables:
        client.delete_table(TableName='local-files')
    if 'local-statistics' in existing_tables:
        client.delete_table(TableName='local-statistics')
    if 'tlm_trip' in existing_tables:
        client.delete_table(TableName='tlm_trip')
    if 'tlm_route' in existing_tables:
        client.delete_table(TableName='tlm_route')
    if 'tlm_driver_scores' in existing_tables:
        client.delete_table(TableName='tlm_driver_scores')
    if 'tlm_event' in existing_tables:
        client.delete_table(TableName='tlm_event')
    if 'tlm_road_speed_limits' in existing_tables:
        client.delete_table(TableName='tlm_road_speed_limits')
    if 'tlm_road_speed_limits_activity' in existing_tables:
        client.delete_table(TableName='tlm_road_speed_limits_activity')

def clear_dynamodb():
    delete_mentor_dynamodb_tables()
    create_mentor_dynamodb_tables()
    print("DyanmoDB is clear.")


def clear_postgresdb():
    try:
        conn = psycopg2.connect(("dbname='baseline' host='localhost' user='telematics' password='telematics'"))
        cursor = conn.cursor()
        cursor.execute("ALTER SEQUENCE trips_tripid_seq RESTART WITH 1")
        cursor.execute("DELETE FROM trips")
        cursor.execute("DELETE FROM events_points")
        cursor.execute("DELETE FROM clean_points")
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print("Uh oh, can't connect.")
        print(e)
    print("PostgresDB is clear.")

if __name__ == "__main__":
    clear_dynamodb()
#    clear_postgresdb()
