import datetime
from geopy import distance
import json
import numpy as np
import pandas as pd
import pytz
import requests

from chalicelib import s3

BUCKET = "tm-bluebikes"
TZ = pytz.timezone("US/Eastern")

#################
# STATION STATUS TO BE PULLED FROM FEED EVERY 5 MINUTES
def get_station_status_key(date, timestamp):
    return f"station_status/{date}/{timestamp}/bluebikes.csv"

def store_station_status():
    resp = requests.get("https://gbfs.bluebikes.com/gbfs/en/station_status.json")

    datajson = json.loads(resp.content)

    timestamp = datajson.get('last_updated')
    df = pd.DataFrame.from_records(datajson.get('data').get('stations'))
    df['datetimepulled'] = timestamp

    date = datetime.datetime.fromtimestamp(timestamp, TZ).date()
    key = get_station_status_key(date, timestamp)
    
    s3.upload_df_as_csv(BUCKET, key, df)

    
##################
# STATION INFO TO BE PULLED FROM FEED DAILY AT 6AM
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
    date = datetime.datetime.fromtimestamp(station_info.get('last_updated'), TZ).date()

    key = get_station_info_key(date)
    
    s3.upload_df_as_csv(BUCKET, key, df)


##################
# Neighbor stations and ridability calculated daily
##################
def get_distance(df, lat, lon, n_lat, n_lon):
    loc = (df[lat], df[lon])
    n_loc = (df[n_lat], df[n_lon])
    dist = distance.distance(loc, n_loc).km
    return dist

def haversine(lat, lon, n_lat, n_lon):
    """
    Distance function impl. from StackOverflow compatible with numpy arrays.
    Distances are slightly different than geopy, so perhaps we use 405 meters as cutoff to be kind?
    """
    radius = 6371.
    d_lat = np.radians(lat - n_lat)
    d_lon = np.radians(lon - n_lon)
    a = np.sin(d_lat / 2)**2 + np.cos(np.radians(lat)) * np.cos(np.radians(n_lat)) * np.sin(d_lon / 2)**2
    c = 2. * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    d = radius * c
    return d

def get_neighbor_key(date):
    return f"station_info/{date}/station_neighbors.csv"

def calc_neighbors(date, exclude=[]):
    # get station info
    df = s3.download_csv_as_df(BUCKET, get_station_info_key(date))
    
    # create two frames, removing stations with no capacity ('temporarily disabled')
    first = df.loc[~df['station_id'].isin(exclude), ['station_id', 'lat', 'lon']]
    second = first.rename(columns={'station_id':'neighbor_station_id', 
                                   'lat':'neighbor_lat', 
                                   'lon':'neighbor_lon'
                                   })
    
    # cross join, filter out where station = neighbor station
    dist = first.merge(second, how='cross')
    dist = dist[dist['station_id'] != dist['neighbor_station_id']]
    
    # create distance metric
    dist['distance_km'] = haversine(dist['lat'], dist['lon'], dist['neighbor_lat'], dist['neighbor_lon'])
    dist = dist[['station_id', 'neighbor_station_id', 'distance_km']]
    
    # filter neighbors to those within 400m
    neighbor = dist[dist['distance_km'] <= 0.4]

    # find the nearest neighbor within 600m
    nearest = dist.loc[dist.groupby('station_id')['distance_km'].idxmin()]
    next_nearest = nearest[(nearest.distance_km > 0.4) & (nearest.distance_km <= 0.6)]
    
    # combine all stations within 400m with the nearest within 600m, drop dupes
    final = pd.concat([neighbor, next_nearest])
    
    # write to s3
    key = get_neighbor_key(date)
    s3.upload_df_as_csv(BUCKET, key, final)
    return final


def get_rideability_key(date):
    return f"rideability/{date}/rideability.csv"


def gather_single_day_data(single_day):
    keys = s3.ls(BUCKET, f"station_status/{single_day}")
    dfs = [s3.download_csv_as_df(BUCKET, key) for key in keys]
    df = pd.concat(dfs)

    return df

# TODO: edge case with valet
def calc_daily_stats(day):
    df = gather_single_day_data(day)

    # find uninstalled stations to exclude from neighbor calculation
    # TODO: worry about (un)installing a station midday
    # currently uninstalled for any part of day -> station does not exist
    uninstalled = df[df['is_installed'] == 0].station_id.unique().tolist()
    neighbor = calc_neighbors(day, exclude=uninstalled)
    
    # don't include uninstalled stations in daily output either
    df = df[~df['station_id'].isin(uninstalled)]

    # create fields to determine rideability
    df['pct_full'] = df['num_bikes_available'] / (df['num_docks_available'] + df['num_bikes_available'])
    df['rideable'] = np.where((df['pct_full']>=0.1) & (df['pct_full']<=0.85) & (df['station_status'] == 'active'), 1, 0)


    # create data frames on rideability for station and neighbors
    df_sm = df[['station_id', 'pct_full', 'rideable', 'datetimepulled']]
    df_n = df_sm.rename(columns={'station_id':'neighbor_station_id'
                                 , 'pct_full':'n_pct_full'
                                 , 'rideable':'n_rideable'})
    
    # merge datapoints with neighbors, then merge neighbors with neighbor rideability
    df_tot = df_sm.merge(neighbor[['station_id', 'neighbor_station_id']], on='station_id', how='left')
    df_tot = df_tot.merge(df_n, on=['neighbor_station_id', 'datetimepulled'], how='left')
    
    # create new field that determines if station pair is rideable
    # then group by station_id to determine overall rideability for that timepoint
    df_tot['tot_rideable'] = df_tot[['rideable', 'n_rideable']].max(axis=1)
    final = df_tot.groupby(['station_id', 'datetimepulled'])['tot_rideable'].max().reset_index()
    
    # convert from epoch & filter out overnight
    final['datetimepulled'] = pd.to_datetime(final['datetimepulled'], unit='s', utc=True).dt.tz_convert('US/Eastern')
    final = final[(final['datetimepulled'].dt.time >= datetime.time(6,0,0)) &
                  (final['datetimepulled'].dt.time < datetime.time(22,0,0))
                  ]
    
    # group by station id and date and calculate % of observations that were rideable
    ride = final.groupby(['station_id']).agg({'tot_rideable':'mean','datetimepulled':'size'}).reset_index()
    ride = ride.rename(columns={'tot_rideable':'rideability','datetimepulled':'count'})
    ride['date'] = day

    # TODO: merge station_info (name_x and name_y) with results for readability
    # also consider: determine if unrideable because too many or too few bikes
    
    key = get_rideability_key(day)
    s3.upload_df_as_csv(BUCKET, key, ride)
    return ride
