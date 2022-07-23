import json
import requests
import pandas as pd
import datetime
import geopy
import boto3
import numpy as np

from chalicelib import s3

BUCKET = "tm-bluebikes"
def get_station_status_key(date, timestamp):
    return f"station_status/{date}/{timestamp}/bluebikes.csv"


def store_station_status():
    resp = requests.get("https://gbfs.bluebikes.com/gbfs/en/station_status.json")

    datajson = json.loads(resp.content)

    df = pd.DataFrame.from_records(datajson.get('data').get('stations'))

    timestamp = datajson.get('last_updated')
    date = datetime.datetime.fromtimestamp(timestamp).date()
    
    key = get_station_status_key(date, timestamp)
    
    s3.upload_df_as_csv(BUCKET, key, df)

    
def get_distance(df, lat, lon, n_lat, n_lon):
    loc = (df[lat], df[lon])
    n_loc = (df[n_lat], df[n_lon])
    dist = geopy.distance.distance(loc, n_loc).km
    return dist

def get_station_info_key(date):
    return f"station_info/{date}/station_info.csv"

def store_station_info():
    # get station info
    resp = requests.get('https://gbfs.bluebikes.com/gbfs/en/station_information.json') 
    
    station_info = json.loads(resp.content)
    station_df = pd.DataFrame(station_info.get('data').get('stations'))

    # get region info
    region_resp = requests.get('https://gbfs.bluebikes.com/gbfs/en/system_regions.json') 
    
    region_info = json.loads(region_resp.content)
    region_df = pd.DataFrame(region_info.get('data').get('regions'))

    # combine, write
    df = station_df.merge(region_df, on='region_id', how='left')
    key = get_station_info_key(date=datetime.datetime.today().date())
    
    s3.upload_df_as_csv(BUCKET, key, df)
    return df


def get_neighbor_key(date):
    return f"station_info/{date}/station_neighbors.csv"


def store_neighbor_stations():
    # get station dfs
    df = store_station_info()
    
    # create two frames
    first = df[['station_id', 'lat', 'lon']]
    second = df[['station_id', 'lat', 'lon']].rename(columns={'station_id':'neighbor_station_id', 
                                                              'lat':'neighbor_lat', 
                                                              'lon':'neighbor_lon'
                                                              })
    
    # cross join, filter where station = neighbor station
    dist = first.merge(second, how='cross')
    dist = dist[dist['station_id']!=dist['neighbor_station_id']]
    
    # create distance metric
    dist['distance_km'] = dist.apply(get_distance, axis=1, lat='lat', lon='lon',
                                     n_lat='neighbor_lat', n_lon='neighbor_lon'
                                     )
    
    # filter neighbors to those within 400m
    neighbor = dist[dist['distance_km']<=0.4]
    neighbor = neighbor[['station_id', 'neighbor_station_id', 'distance_km']]
    
    # find the nearest neighbor within 600m
    nearest = dist.groupby('station_id').min('distance_km').reset_index()
    nearest = nearest.merge(dist, on=['station_id','distance_km'], how='left')
    nearest = nearest[['station_id','neighbor_station_id','distance_km']]
    nearest = nearest[nearest['distance_km']<=0.6]
    
    # combine all stations within 400m with the nearest within 600m, drop dupes
    final = pd.concat([neighbor, nearest])
    final = final.drop_duplicates()
    
    # write to s3
    key = get_neighbor_key(date=datetime.datetime.today().date())
    s3.upload_df_as_csv(BUCKET, key, final)
    return final


def get_rideability_key(date):
    return f"rideability/{date}/rideability.csv"


def get_single_day_bluebikes(single_day):
    df = pd.DataFrame()
    
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket='tm-bluebikes', Prefix=single_day)

    for page in pages:
        for obj in page['Contents']:
            print(obj['Key'])
            temp_df = pd.read_csv("s3://tm-bluebikes/"+obj['Key'])
            temp_df['datetimepulled'] = obj['Key'].split('/')[1]
            df = pd.concat([df, temp_df])
                            
    return df


def get_bluebikes_data(day):
    days = [day, day+datetime.datetime.timedelta(days=1)]
    df = pd.DataFrame()
    
    for single_day in days:
        temp_df = get_single_day_bluebikes(single_day=str(single_day))
        df = pd.concat([df, temp_df])
            
    return df


def merge_bluebikes_data(day):
    df = get_bluebikes_data(day)
    
    # create fields to determine rideability
    df['pct_full'] = df['num_bikes_available'] / (df['num_docks_available'] + df['num_bikes_available'])
    df['rideable'] = np.where((df['pct_full']>=0.1) & (df['pct_full']<=0.85),1,0)
    df = df[df['station_status']=='active']

    # create data frames on rideability for station and neighbors
    df_sm = df[['station_id', 'pct_full', 'rideable', 'datetimepulled']]
    df_n = df_sm.rename(columns={'station_id':'neighbor_station_id'
                                 , 'pct_full':'n_pct_full'
                                 , 'rideable':'n_rideable'})
    
    neighbor = pd.read_csv('s3://tm-bluebikes/station_info'+day+'station_neighbors.csv')
    
    # merge data
    df_tot = df_sm.merge(neighbor[['station_id', 'neighbor_station_id']], on='station_id', how='left')
    df_tot = df_tot.merge(df_n, on=['neighbor_station_id', 'datetimepulled'], how='left')
    
    # create new field that determines if station is rideable
    df_tot['tot_rideable'] = df_tot[['rideable', 'n_rideable']].max(axis=1)
    return df_tot


def store_rideability_data(day):
    df = merge_bluebikes_data(day)
    
    # determine if the station is rideable for a given time
    final = df.groupby(['station_id', 'datetimepulled'])['tot_rideable'].max().reset_index()
    
    # convert from epoch & filter out overnight
    final['datetimepulled'] = pd.to_datetime(final['datetimepulled'], unit='s', utc=True).dt.tz_convert('US/Eastern')
    final = final[(final['datetimepulled'].dt.time>=datetime.time(6,0,0)) &
                  (final['datetimepulled'].dt.time<=datetime.time(22,0,0))
                  ]
    
    # add column for date, filter to just that day
    final['date'] = final['datetimepulled'].dt.date
    final = final[final['date']==day]
    
    # group by station id and date and calculate % of observations that were rideable
    ride = final.groupby(['station_id']).agg({'tot_rideable':'mean','datetimepulled':'size'}).reset_index()
    ride = ride.rename(columns={'tot_rideable':'rideability','datetimepulled':'count'})
    
    key = get_rideability_key(date=datetime.datetime.today().date() - datetime.datetime.timedelta(days=1))
    s3.upload_df_as_csv(BUCKET, key, ride)
    return ride
