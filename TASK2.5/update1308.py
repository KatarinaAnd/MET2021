#!/usr/bin/python3

# import packages
import os
import sys
import argparse
#import csv
import requests
import pandas as pd
from io import StringIO
import xarray as xr
import matplotlib.pyplot as plt
#import datetime as dt
import json
import yaml
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import pprint

# function for applying arguments in the command line
# python <filename>.py -c <config-file>.cfg -s <start date> -e <end date>
def parse_arguments():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-c","--cfg",dest="cfgfile",
            help="Configuration file", required=True)
    parser.add_argument("-s","--startday",dest="startday",
            help="Start day in the form YYYY-MM-DD", required=True)
    parser.add_argument("-e","--endday",dest="endday",
            help="End day in the form YYYY-MM-DD", required=True)
    args = parser.parse_args()

    try:
        datetime.strptime(args.startday,'%Y-%m-%d')
    except ValueError:
        raise ValueError
    try:
        datetime.strptime(args.endday,'%Y-%m-%d')
    except ValueError:
        raise ValueError

    if args.cfgfile is None:
        parser.print_help()
        parser.exit()

    return args

# function for reading the configuration file
def parse_cfg(cfgfile):
    print("Reading", cfgfile)
    with open(cfgfile, 'r') as ymlfile:
        cfgstr = yaml.full_load(ymlfile)

    return cfgstr

# function creating a log-file
def initialise_logger(outputfile = './log'):
    # Check that logfile exists
    logdir = os.path.dirname(outputfile)
    if not os.path.exists(logdir):
        try:
            os.makedirs(logdir)
        except:
            raise IOError
    # Set up logging
    mylog = logging.getLogger()
    mylog.setLevel(logging.INFO)
    #logging.basicConfig(level=logging.INFO, 
    #        format='%(asctime)s - %(levelname)s - %(message)s')
    myformat = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(myformat)
    mylog.addHandler(console_handler)
    file_handler = logging.handlers.TimedRotatingFileHandler(
            outputfile,
            when='w0',
            interval=1,
            backupCount=7)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(myformat)
    mylog.addHandler(file_handler)

    return(mylog)


# function that gets the list of ships in data storage.
def getships(frostcfg):

    myrequest = 'municipality=skip'

    # Connect and read information
    try:
        r = requests.get(frostcfg['endpointmeta'],
                myrequest,
                auth=(frostcfg['client_id'],""))
    except:
        mylog.error('Something went wrong extracting metadata.')
        raise
    # Check if the request worked, print out any errors
    if not r.ok:
        mylog.error('Returned status code was %s', r.status_code)
        print(r.text)
        raise
    mylist = r.json()
    return(mylist)

args = parse_arguments()
cfgstr = parse_cfg(args.cfgfile)

# Function for making the data to a netcdf


# CHECK THE KEYWORDS as it is just copy pasted from faststasjoner
def get_keywords(chosen_category):
    something_dict = {
        #'none' : 'the variable requested is not listed in the keywords dictionary',
        'air_pressure':"EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC PRESSURE > SURFACE PRESSURE",
        'air_temperature':"EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC TEMPERATURE > SURFACE TEMPERATURE > AIR TEMPERATURE",
        'wind_speed':"EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC WINDS > SURFACE WINDS > WIND SPEED",
        'wind_direction':"EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC WINDS > SURFACE WINDS > WIND DIRECTION",
        'relative_humidity' :"EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC WATER VAPOR > WATER VAPOR INDICATORS > HUMIDITY > RELATIVE HUMIDITY",
        'sea_surface_temperature': 'EARTH SCIENCE > OCEANS > OCEAN TEMPERATURE > SEA SURFACE TEMPERATURE',
        'air_pressure_at_sea_level': 'EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC PRESSURE > SEA LEVEL PRESSURE',
        'cloud_base_height': 'EARTH SCIENCE > ATMOSPHERE > CLOUDS > CLOUD PROPERTIES > CLOUD BASE HEIGHT',
        'precipitation_group_indicator': '-',
        'cloud_area_fraction': 'EARTH SCIENCE > ATMOSPHERE > CLOUDS > CLOUD PROPERTIES > CLOUD FRACTION'
        }
    if chosen_category not in something_dict:
        print('the variable requested is not listed in the keywords dictionary')
    return something_dict.get(chosen_category)

def extractdata(frostcfg,station,stmd,output):

    # Connect and read metadata for the station
    mylog.info('Retrieving metadata for station'+ station)
    myrequest = 'ids='+ station
    try:
        r = requests.get(frostcfg['endpointmeta'],
                myrequest,
                auth=(frostcfg['client_id'],""))
    except:
        mylog.error('Something went wrong extracting metadata')

    # Check if the request worked, print out any errors
    if not r.ok:
        mylog.error('Returned status code was %s with response %s', r.status_code, r.text)
        raise
    metadata = json.loads(r.text)

    # Connect and read information on available parameters at the station
    mylog.info('Retrieving parameters for station: %s', station)
    myrequest = 'sources='+ station+'&elements='+','.join(frostcfg['elements']) + ", longitude, latitude"
    try:
        r = requests.get(frostcfg['endpointparameters'],
                myrequest,
                auth=(frostcfg['client_id'],""))
    except:
        mylog.error('something wen wrong extracting metadata.')
        raise

    # Check if the request worked, print out any errors
    if not r.ok:
        mylog.error('Returned status code was %s, message was:\n%s', r.status_code, r.text)
        raise


    parameterlist = json.loads(r.text)

    # Create request for observations
    # note that longitude and latitude always is added

    mylog.info('Retrieving data for station: %s', station)
    myrequest = {
            'sources': station,
            'elements': ','.join(frostcfg['elements'])  + ', longitude, latitude',
            'fields': ','.join(frostcfg['fields']),
            'referencetime': '/'.join([args.startday,args.endday])
            }
    
    # Connect and read observations
    try:
        r = requests.get(frostcfg['endpointobs'],
                myrequest,
                auth=(frostcfg['client_id'],""))
    except:
        mylog.error('Something went wrong extracting data.')
        raise

    # Check if the request worked, print out any errors
    if r.status_code == 412:
        mylog.error('Information returned indicates that no data is available for this time period for station %s', station)
        return
    if not r.status_code == 200:
        mylog.error('Returned status code was %s\nmessage:\n%s', r.status_code, r.text)
        raise
    
    # Read into  Pandas DataFrame, assuming - is used for missing values.
    df = pd.read_csv(StringIO(r.text),header=0,
        mangle_dupe_cols=True, parse_dates=['referenceTime'],
        index_col=False,na_values=['-'])
    
    # remove unwated symbols in variabel name
    df.columns = df.columns.str.rstrip('\(-\)')

    # extracting information on the time frame, both for the filename and netcdf content
    timemin = min(df['referenceTime'])
    timemax = max(df['referenceTime'])
    datasetstart = timemin.strftime('%Y-%m-%dT%H:%M:%SZ')
    datasetend = timemax.strftime('%Y-%m-%dT%H:%M:%SZ')
    datasetstart4filename = timemin.strftime('%Y%m%d')
    datasetend4filename = timemax.strftime('%Y%m%d')

    # converting time and adding it as index
    mytimes = (pd.to_datetime(df['referenceTime'], utc=True)-pd.Timestamp("1970-01-01", tz='UTC')) // pd.Timedelta('1s')
    df['time'] = mytimes
    df = df.set_index('time')
    
    # creating a list of elements that we want in our df/netcdf
    elements = frostcfg['elements'][0].split(',')
    elements = [x.rstrip(', ').lstrip(' ').lower() for x in elements]
    elements.append('longitude')
    elements.append('latitude')
    df = df[elements].copy()


    # creating netcdf file from df
    # expanding the dims to add lon and lat
    ds_station = xr.Dataset.from_dataframe(df)
    ds_station = ds_station.expand_dims(dim = {'lon': ds_station.longitude.values, 'lat': ds_station.latitude.values})

    ds_station.time.attrs['standard_name'] = 'time'
    ds_station.time.attrs['units'] = 'seconds since 1970-01-01 00:00:00+0'
    ds_station.lon.attrs['standard_name'] = 'longitude'
    ds_station.lon.attrs['units'] = 'degrees'
    ds_station.lat.attrs['standard_name'] = 'latitude'
    ds_station.lat.attrs['units'] = 'degrees'

    # saving lat lon values for global attributes
    lat_min = ds_station.latitude.values.min()
    lat_max = ds_station.latitude.values.max()
    lon_min = ds_station.longitude.values.min()
    lon_max = ds_station.longitude.values.max()

    # removing longitude and latitude as data variables
    ds_station = ds_station.drop_vars(['longitude','latitude'])


    avvars = []
    for item in parameterlist['data']:
        avvars.append(item['elementId'])
    
    for item in ds_station.data_vars.keys():
        if item in avvars:
            varname = item
            print(varname)
            for myel in parameterlist['data']:
                if item in myel['elementId']:
                    ds_station[item].attrs['standard_name'] = myel['elementId']
                    ds_station[item].attrs['units'] = myel['unit']
                    ds_station[item].attrs['long_name'] = myel['elementId'].replace('_', ' ')
                    ds_station[item].attrs['keywords'] = get_keywords(varname)
                    break

    # Need to convert from dataarray to dataset in order to add global attributes
    ds_station.attrs['featureType'] = 'timeSeries'
    ds_station.attrs['title'] = 'Weather station information from ship '+stmd['name']
    ds_station.attrs['summary'] = output['abstract']
    ds_station.attrs['license'] = metadata['license']
    ds_station.attrs['time_coverage_start'] = datasetstart
    ds_station.attrs['time_coverage_end'] = datasetend
    ds_station.attrs['geospatial_lat_min'] = lat_min
    ds_station.attrs['geospatial_lat_max'] = lat_max
    ds_station.attrs['geospatial_lon_min'] = lon_min
    ds_station.attrs['geospatial_lon_max'] = lon_max
    ds_station.attrs['creator_name'] = stmd['PrincipalInvestigator'] 
    ds_station.attrs['creator_email'] = stmd['PrincipalInvestigatorEmail']
    ds_station.attrs['creator_url'] = stmd['PrincipalInvestigatorOrganisationURL']
    ds_station.attrs['creator_institution'] = stmd['PrincipalInvestigatorOrganisation']
    ds_timeseries.attrs['keywords'] = output['keywords']
    ds_station.attrs['keywords_vocabulary'] = 'GCMD'
    ds_station.attrs['publisher_name'] = ''
    ds_station.attrs['publisher_email'] = 'adc@met.no'
    ds_station.attrs['publisher_url'] = 'https://adc.met.no/'
    ds_station.attrs['publisher_institution'] = 'Norwegian Meteorlogical Institute'
    ds_station.attrs['Conventions'] = 'ACDD, CF-1.8'
    ds_station.attrs['date_created'] = metadata['createdAt']
    ds_station.attrs['history'] = metadata['createdAt']+': Data extracted from the MET Observation Database through Frost and stored as NetCDF-CF'
    ds_station.attrs['source'] = 'Soil temperature from permafrost boreholes'
    ds_station.attrs['wigosId'] = metadata['data'][0]['wigosId']
    ds_station.attrs['METNOId'] =  station
    ds_station.attrs['project'] = stmd['Project']
    
    print(ds_station)

    # Dump to Netcdf
    outputfile = output['destdir']+'/ship-'+metadata['data'][0]['wigosId']+'_'+datasetstart4filename+'-'+datasetend4filename+'.nc'
    mylog.info('Dumping data to NetCDF-CF:\n%s', outputfile)
    try:
        ds_station.to_netcdf(outputfile)
    except:
        mylog.error('Creation of NetCDF file didn\'t work properly\n%s', sys.exc_info()[0])
        raise

    return

if __name__ == '__main__':
    
    # Parse command line arguments
    try:
        args = parse_arguments()
    except:
        raise SystemExit('Command line arguments didn\'t parse correctly.')

    # Parse configuration file
    cfgstr = parse_cfg(args.cfgfile)

    # Initialise logging
    mylog = initialise_logger(cfgstr['output']['logfile'])
    mylog.info('Configuration of logging is finished.')

    # Find all ships available
    mylog.info('Retrieve all ships available in the data storage.')
    try:
        ships = getships(cfgstr['frostcfg'])
    except:
        mylog.warn('Couldn\'t get the list of ships in data storage.')
        raise SystemExit()
    #pprint.pprint(ships)

    # Loop through ships
    mylog.info('Processing ships from the list.')


    for station,content in cfgstr['stations'].items():
        if station in ['SN99927']:
            continue
        mylog.info('Requesting data for: %s', station)
        #outputfile = cfgstr['output']['destdir']+'/'+content['filename']+'.nc'
        try:
            extractdata(cfgstr['frostcfg'], station, content, cfgstr['output'])
        except:
            mylog.error('Something went horribly wrong here')
            raise SystemExit()