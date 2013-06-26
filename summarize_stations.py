
from jinja2 import Environment, FileSystemLoader
import calculate_stats
import datetime as dt
from multiprocessing import Pool
from collections import defaultdict
import json

from boto.s3.connection import S3Connection
from boto.s3.key import Key
import json
import os

import argparse
example_stations_by_time = defaultdict(dict)
def pandas_process_file(fname, field_name="availableDocks", collection_dict=example_stations_by_time):
    stats = json.loads(open(fname).read())
    et = stats['executionTime']
    for s in stats['stationBeanList']:
        collection_dict[s['id']][et] = s[field_name]
    return stats

def write_data_file():
    ab = pandas_process_file('stations-05-28-16_04_24.json')
    station_data = ab['stationBeanList']
    stations_by_id = {}
    for s in station_data:
        stations_by_id[s['id']] = s
    open('station_data.json', "w").write(json.dumps(stations_by_id))
    for k,v in stations_by_id.items():
        dmap = construct_station_dist_map(stations_by_id, v)
        v['closest_stations'] = dmap.keys()
        v['station_distances'] = dict(dmap.items())
        
    return stations_by_id



from collections import OrderedDict
def construct_station_dist_map(by_id, s):
    dist_map = {}
    for k, s2 in by_id.iteritems():
        dist_map[k] = station_distance(s, s2)
    return OrderedDict(sorted(dist_map.items(), key=lambda t: t[1]))

def station_distance(s1, s2):
    return distance(
        s1['latitude'], s1['longitude'],
        s2['latitude'], s2['longitude'])

import math
 
def distance(lat1, lon1, lat2, lon2):
    radius = 6371 # km
 
    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c
    return d


def __upload(fname):
    k = Key(bucket)
    k.key = fname[10:]  #strip off the site_root/
    print fname
    k.set_contents_from_filename(fname)
    k.set_acl('public-read')
    return  k


def upload_html():
    walk_obj = os.walk('site_root')
    all_filenames = []
    for dir_path, unused, filenames in walk_obj:
        for fname in filenames:
            if 'plots' in dir_path:
                continue
            all_filenames.append(os.path.join(dir_path, fname))
    Pool(100).map(__upload, all_filenames)
    print "after p.map"

def upload_to_s3():
    walk_obj = os.walk('site_root')
    all_filenames = []
    for dir_path, unused, filenames in walk_obj:
        for fname in filenames:
            all_filenames.append(os.path.join(dir_path, fname))
    Pool(100).map(__upload, all_filenames)
    print "after p.map"


complete_summaries = {}

# {'all_time_starting_trips': 295.0,
#  u'altitude': u'',
#  u'availableBikes': 21,
#  u'availableDocks': 14,
#  u'city': u'',
#  'closest_stations': [72,
#   480,
#   508,
#   495,],
#  'day_starting_trips': 68.0,
#  'hour_starting_trips': 0,
#  u'id': 72,
#  u'landMark': u'',
#  u'lastCommunicationTime': None,
#  u'latitude': 40.76727216,
#  u'location': u'',
#  u'longitude': -73.99392888,
#  u'postalCode': u'',
#  u'stAddress1': u'W 52 St & 11 Av',
#  u'stAddress2': u'',
#  u'stationName': u'W 52 St & 11 Av',
#  'station_distances': {72: 0.0,
#   79: 5.461241129523938,
#   82: 6.259903786989711,
#   83: 9.396643395659359,
#   116: 2.905835095023139,
#   119: 8.02767027885375,
#   120: 9.415747374933149},
#  u'statusKey': 1,
#  u'statusValue': u'In Service',
#  u'testStation': False,
#  u'totalDocks': 39,
#  'week_starting_trips': 295.0}




def write_station_html(s):
    env = Environment(loader=FileSystemLoader('templates'))
    template = env.get_template('station.html')
    output_from_parsed_template = template.render(s=s, sbid=stations_by_id)
    # to save the results
    with open("site_root/stations/s%d.html" % s['id'], "wb") as fh:
        fh.write(output_from_parsed_template.encode('utf-8'))

def write_system_html(s, stations_by_id):
    env = Environment(loader=FileSystemLoader('templates'))
    template = env.get_template('index.html')

    output_from_parsed_template = template.render(
        s=s, sbid=stations_by_id)
    #sbid_json=json.dumps(stations_by_id))
    # to save the results
    with open("site_root/index.html", "wb") as fh:
        fh.write(output_from_parsed_template.encode('utf-8'))


def produce_single_summary(v):

    complete_summaries[v['id']] = v
    v['fname']= v['stAddress1'].replace(" ", "_").replace("&", "and")

    write_station_html(v)

def update_summaries():
    for k,v in stations_by_id.items():
        if k == 146:
            continue
        try:
            v.update(ss.produce_station_stats(v['id']))
        except Exception, e:
            print "ERROR with k", k
            print e

def produce_all_summaries():
    write_system_html(s_stats, stations_by_id)
    for k,v in stations_by_id.items():
        if k == 146:
            continue
        try:


            print k,v['stAddress1']
            produce_single_summary(v)
        except Exception, e:
            print "ERROR with k", k
            print e

def _plot(station_id):
    
    try:

        ss.produce_station_plots(str(station_id))
        print station_id, stations_by_id[station_id]['stAddress1'].encode('utf-8')
    except Exception,e:
        print station_id, e

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def produce_all_plots():
    #import pdb
    #pdb.set_trace()
    for chunk in chunks(stations_by_id.keys(), 32):
        # I want new process pools because _plot leaks memory, a lot
        # this way I let UNIX do garbage collection on the newly created processes
        Pool(8).map(_plot, chunk)
            

def run_from_ipython():
    try:
        __IPYTHON__
        return True
    except NameError:
        return False


s_stats, stations_by_id, ss = [None, None, None]
def calcs():
    global s_stats, stations_by_id, ss
    t1 = dt.datetime.now()
    print "start write_data_file()"
    stations_by_id = write_data_file()
    t2 = dt.datetime.now()
    print "end write_data_file ", t2 - t1

    existing = calculate_stats.grab_existing()
    t3 = dt.datetime.now()
    print "end grab_exisitng", t3 - t2

    ss = calculate_stats.process_dataframe(existing)
    t4 = dt.datetime.now()
    print "end calculate_stats", t4-t3
    s_stats = ss.produce_system_stats()
    t5 = dt.datetime.now()
    print "end produce_system_stats", t5 - t4


if run_from_ipython():
    calcs()
    update_summaries()
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Description of your program')
    parser.add_argument('-d','--data_collect', default=False, action="store_true",
                        help='parse all the raw json files into a csv')
    parser.add_argument('-s','--summarize', default=False, action="store_true",
                        help='summarize the stations, build the html files')

    parser.add_argument('-a','--update', default=False, action="store_true",
                        help='construct a new dataframe with newly modified files')

    parser.add_argument('-e','--ever', default=False, action="store_true",
                        help='run summarize forever')
    parser.add_argument('-p','--plot', default=False, action="store_true",
                        help='construct the plots')
    parser.add_argument('-u','--upload', default=False, action="store_true",
                        help='upload the stations to s3')
    parser.add_argument('-y','--upload_plots', default=False, action="store_true",
                        help='upload the site_root to s3, including the plots')

    parser.add_argument('-i','--interactive', default=False, action="store_true",
                        help='just produce summary data objects for ipython interogation')

    args = parser.parse_args()



    if args.data_collect:
        calculate_stats.process_raw_files()
    if args.update:
        existing = calculate_stats.grab_existing()
        existing2 = calculate_stats.update_df(existing)

        
    if args.summarize or args.plot or args.interactive or args.ever:
        calcs()
        update_summaries()
    
    if args.summarize:
        produce_all_summaries()
    if args.ever:
        while True:
            produce_all_summaries()
    if args.plot:
        produce_all_plots()
    if args.upload or args.upload_plots:
        secret_key = json.loads(open(os.path.expanduser(
            "~/.ec2/s3_credentials.json")).read())
        conn = S3Connection(*secret_key.items()[0])
        bucket = conn.get_bucket("citibikedata.com")

    if args.upload:
        upload_html()
    if args.upload_plots:
        upload_to_s3()
    
    

