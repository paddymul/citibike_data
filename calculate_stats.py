import json
from collections import defaultdict
import os
import pandas as pd
import numpy as np
import dateutil 
import matplotlib
matplotlib.use('AGG')
import matplotlib.pyplot as plt

DATA_DIR= os.path.expanduser('~/data_citibike/')
LIMIT=False

example_stations_by_time = defaultdict(dict)
def pandas_process_file(
        fname, field_name="availableDocks", 
        collection_dict=example_stations_by_time):
    stats = json.loads(open(fname).read())
    et = stats['executionTime']
    for s in stats['stationBeanList']:
        collection_dict[s['id']][et] = s[field_name]
    return stats

def process_file_list(f_list, limit=False):
    stations_by_time = defaultdict(dict)

    if not limit:
        limit = len(f_list)
    count = 0
    for fname in f_list[:limit]:
        count += 1
        #print count, limit
        if fname.find('stations-') == -1:
            continue
        try:
            pandas_process_file(fname, collection_dict=stations_by_time)
        except Exception, e:
            print e, fname
    return stations_by_time

def process_directory(d_name, limit=False):
    """This function processes all files in a directory that start with stations- and
    returns a dict of dicts suitable for pandas DataFrame ingestion"""
    disregard, disregard2, station_files = os.walk(d_name).next()
    return process_file_list(map(lambda x: os.path.join(d_name, x), station_files), limit)
    
def files_newer_than(start_time, dir_path):
    t1 = dt.datetime.now()
    fname_list = []
    for fname in os.listdir(dir_path):
        full_path = dir_path + fname
        mtime = dt.datetime.fromtimestamp(os.stat(full_path).st_mtime)
        if mtime > start_time:
            fname_list.append(full_path)
    return fname_list

def process_newer_files(start_time, dir_path, limit=False):
    """This function processes all files in a directory that start with stations- and
    returns a dict of dicts suitable for pandas DataFrame ingestion""" 
    files = files_newer_than(start_time, dir_path)
    return process_file_list(files, limit)

def upload_df(df):
    from boto.s3.key import Key
    import json, os
    from boto.s3.connection import S3Connection
    secret_key = json.loads(open(os.path.expanduser(
            "~/.ec2/s3_credentials.json")).read())
    conn = S3Connection(*secret_key.items()[0])
    save_df(df)

    bucket = conn.get_bucket("citibikedata.com")
    k = Key(bucket)
    k.key = 'store.comp.h5'
    k.set_contents_from_filename('store.comp.h5')
    k.set_acl('public-read')

def save_df(df, path='store.comp.h5'):
    store = pd.HDFStore(path, complevel=9, complib='blosc')
    store['df'] = df
    store.flush()
    store.close()
    return df

def update_df(df):
    import json, os
    df2 = pd.DataFrame(process_newer_files(df.index[-1], DATA_DIR, LIMIT))
    df2.index = df2.index.map(dateutil.parser.parse)
    #df2.to_csv('most_recent.csv')
    #df3 = pd.read_csv('most_recent.csv', index_col=0, parse_dates=[0])
    df3 = df2.sort_index()
    print "new_df has %r items" % df2.ix
    complete_df = pd.concat([df, df2])
    complete_df.sort()
    upload_df(complete_df)
    return complete_df


def process_raw_files():
    raw_dict = process_directory(os.path.expanduser(DATA_DIR), LIMIT)
    df = pd.DataFrame(raw_dict)
    df.index = df.index.map(dateutil.parser.parse)
    df = df.sort_index()
    save_df(df)
    return df
    
HFIVE = 'store.comp.h5'
def grab_existing(force=False):
    import requests
    
    if force or not os.path.exists(HFIVE):
        r = requests.get("http://citibikedata.com/store.comp.h5")
        if r.status_code == 200:
            with open(HFIVE, 'wb') as f:
                for chunk in r.iter_content(1024):
                    f.write(chunk)
        store = pd.HDFStore(HFIVE)
    else:
        store = pd.HDFStore(HFIVE)

    df = store['df']
    store.close()
    return df

def process_dataframe(input_df):
    print "start process_dataframe", dt.datetime.now()
    # we need to sort the dataframe so that rows are arranged chronologically
    df = input_df.sort() 
    df = input_df.sort_index() 
    
    print "after sort", dt.datetime.now()
    # diff_df is the change in station occupancy from time period to time period
    diff_df = df.diff()
    print "after diff", dt.datetime.now()
    starting_trips = diff_df.where(diff_df < 0).fillna(0).abs()
    #starting_summaries = starting_trips.sum(axis=1)
    ending_trips = diff_df.where(diff_df > 0).fillna(0).abs()
    #ending_summaries = ending_trips.sum(axis=1)

    print "after trips_calcs", dt.datetime.now()
    return StationSummaries(df, diff_df, starting_trips, ending_trips)


import datetime as dt
one_hour = dt.timedelta(0,1)
one_day = dt.timedelta(1)
one_week = dt.timedelta(7)
all_time = dt.timedelta(70000)

class StationSummaries(object):
    def __init__(self, df, diff_df, starting_trips, ending_trips):
        self.df, self.diff_df = df, diff_df
        self.starting_trips, self.ending_trips =  starting_trips, ending_trips

    def produce_station_stats(self, station_id, now = False):
        if not now:
            now = dt.datetime.now()
        start_col = self.starting_trips.get(station_id).abs()
        hour_df = start_col[now - one_hour:now]
        day_df = start_col[now - one_day:now]
        week_df = start_col[now-one_week:now]
        all_df = start_col[now-all_time:now]
        summary_stats = dict(
            starting = dict(
                hour=hour_df.sum(),
                day=day_df.sum(),
                week=week_df.sum(),
                all=all_df.sum()))
        return summary_stats

    def produce_station_plots(self, station_id, now = False):
        if not now:
            now = dt.datetime.now()
        start_col = self.starting_trips[str(station_id)]
        available_col = self.df[str(station_id)]
        
        hour_df = start_col[now - one_hour:now]
        day_df = start_col[now - one_day:now]
        week_df = start_col[now-one_week:now]
        all_df = start_col[now-all_time:now]


        a_hour_df = available_col[now - one_hour:now]
        a_day_df = available_col[now - one_day:now]
        a_week_df = available_col[now-one_week:now]
        a_all_df = available_col[now-all_time:now]
        
        directory = "site_root/plots/%s" % str(station_id)
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.plot(hour_df, "site_root/plots/%s/hour.png" % str(station_id))
        self.plot(day_df, "site_root/plots/%s/day.png" % str(station_id))
        self.plot(week_df, "site_root/plots/%s/week.png" % str(station_id))
        self.plot(all_df, "site_root/plots/%s/all.png" % str(station_id))

        self.plot(a_hour_df, "site_root/plots/%s/avail_hour.png" % str(station_id))
        self.plot(a_day_df, "site_root/plots/%s/avail_day.png" % str(station_id))
        self.plot(a_week_df, "site_root/plots/%s/avail_week.png" % str(station_id))
        self.plot(a_all_df, "site_root/plots/%s/avail_all.png" % str(station_id))

        self.plot(hour_df.cumsum(), "site_root/plots/%s/hour_cumsum.png" % str(station_id))
        self.plot(day_df.cumsum(), "site_root/plots/%s/day_cumsum.png" % str(station_id))
        self.plot(week_df.cumsum(), "site_root/plots/%s/week_cumsum.png" % str(station_id))
        self.plot(all_df.cumsum(), "site_root/plots/%s/all_cumsum.png" % str(station_id))


    def produce_system_stats(self, now = False):

        if not now:
            now = dt.datetime.now()

        time_dict = dict(hour=now - one_hour, 
                         day=now-one_day, week=now-one_week, all=now-all_time)
        stt = self.starting_trips[:now]
        base_starts = dict([[label, stt[time:]] for label, time in time_dict.items()])
        station_sums = dict([[label, base_starts[label].sum().abs()] for label, time in time_dict.items()])
        abs_station_sums = dict([[k, v.abs()] for k,v in station_sums.items()])
        [[k, v.sort(axis=1)] for k,v in abs_station_sums.items()]

        popular_starting_stations = dict(
            [[k, v.index.tolist()] for k,v in abs_station_sums.items()])
        [[k, v.reverse()] for k,v in  popular_starting_stations.items()]
        popular_starting_stations2 = dict(
                [[k, map(abs, map(int, v))] for k,v in  popular_starting_stations.items()])


        total_trips = dict(
            [[label, base_starts[label].sum().abs().sum()] for label, time in time_dict.items()])

        summary_stats =  dict(
            total_trips=total_trips,
            popular_starting_stations=popular_starting_stations2)


        return summary_stats


    def plot(self, df, fname):
        fig=plt.figure()
        ax = fig.add_subplot(111)
        ax.plot(df.index,df)
        fig.autofmt_xdate()
        fig.savefig(fname)
        fig.clf()

if __name__ == "__main__":
    ss = process_dataframe(grab_existing())
    ss_dict = {}

    print "generating_station_summaries"
    print ss.produce_station_plots(363)

