#!/usr/bin/python3

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
import re

def get_performance_category(chosen_category):
    preformance_dict = {
    'A':'A - The sensor type fulfills the requirements from WMO/CIMOs on measurement accuracy, calibration and maintenance.', 
    'B':'B - Rutines for calibration and maintanance are known. Control of the montage exists. The precision of the measurment is lower than the WMO/CIMO requirements.',
    'C': 'C - The sensor type is assumed to fulfill the WMO/CIMO requirements. Missing measurement for control, rutines for calibration, or maintanence.',
    'D': 'D - The sensor type is assumed to fulfill the WMO/CIMO requirements. Some controls show deviations from the WMO/CIMO requirements.'}
    return preformance_dict.get(chosen_category)

#work on this i tink
def get_keywords(chosen_category):
    something_dict = {
        #'none' : 'the variable requested is not listed in the keywords dictionary',
        'air_pressure':"EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC PRESSURE > SURFACE PRESSURE",
        'air_temperature':"EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC TEMPERATURE > SURFACE TEMPERATURE > AIR TEMPERATURE",
        'wind_speed':"EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC WINDS > SURFACE WINDS > WIND SPEED",
        'wind_direction':"EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC WINDS > SURFACE WINDS > WIND DIRECTION",
        'relative_humidity' :"EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC WATER VAPOR > WATER VAPOR INDICATORS > HUMIDITY > RELATIVE HUMIDITY",
        'dew_point_temperature':"EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC WATER VAPOR > WATER VAPOR INDICATORS > DEW POINT TEMPERATURE",
        'radiation':"EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > LONGWAVE RADIATION > DOWNWELLING LONGWAVE RADIATION",
        'surface_downwelling_longwave_flux_in_air':"EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > LONGWAVE RADIATION > UPPWELLING LONGWAVE RADIATION",
        'surface_upwelling_longwave_flux_in_air' : "EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > LONGWAVE RADIATION > UPPWELLING LONGWAVE RADIATION",
        'surface_downwelling_shortwave_flux_in_air': "EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > SHORTWAVE RADIATION > DOWNWELLING SHORTWAVE RADIATION",
        'surface_upwelling_shortwave_flux_in_air': "EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > SHORTWAVE RADIATION > DOWNWELLING SHORTWAVE RADIATION",
        'surface_albedo': "EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > ALBEDO",
        'surface_downwelling_photosynthetic_radiative_flux_in_air': 'EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION > RADIATIVE FLUX',
        'surface_net_downward_radiative_flux': 'EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION'}
    if chosen_category not in something_dict:
        print('the variable requested is not listed in the keywords dictionary')
    return something_dict.get(chosen_category)

def check_radiation(vname,ds_station):
    rad_dict = {
    'mean(solar_irradiance pt1m)': 'solar_irradiance',
    'mean(surface_albedo pt1h)': 'surface_albedo', 
    'mean(surface_downwelling_longwave_flux_in_air pt1h)': 'surface_downwelling_longwave_flux_in_air',
    'mean(surface_downwelling_longwave_flux_in_air pt1m)': 'surface_downwelling_longwave_flux_in_air',
    'mean(surface_downwelling_photosynthetic_radiative_flux_in_air pt1h)': 'surface_downwelling_photosynthetic_radiative_flux_in_air', 
    'mean(surface_downwelling_shortwave_flux_in_air pt10m)': 'surface_downwelling_shortwave_flux_in_air', 
    'mean(surface_downwelling_shortwave_flux_in_air pt1h)': 'surface_downwelling_shortwave_flux_in_air', 
    'mean(surface_downwelling_shortwave_flux_in_air pt1m)': 'surface_downwelling_shortwave_flux_in_air', 
    'mean(surface_net_downward_radiative_flux pt1h)': 'surface_net_downward_radiative_flux', 
    'mean(surface_net_downward_radiative_flux pt1m)': 'surface_net_downward_radiative_flux', 
    'mean(surface_upwelling_longwave_flux_in_air pt1h)': 'surface_upwelling_longwave_flux_in_air', 
    'mean(surface_upwelling_longwave_flux_in_air pt1m)': 'surface_upwelling_longwave_flux_in_air', 
    'mean(surface_upwelling_shortwave_flux_in_air pt1h)': 'surface_upwelling_shortwave_flux_in_air', 
    'mean(surface_upwelling_shortwave_flux_in_air pt1m)': 'surface_upwelling_shortwave_flux_in_air', 
    'mean(diffuse_downwelling_shortwave_flux_in_air pt1h)': 'diffuse_downwelling_shortwave_flux_in_air', 
    '(duration_of_sunshine p1d)': 'duration_of_sunshine', '(duration_of_sunshine p1m)': 'duration_of_sunshine', 
    '(duration_of_sunshine p1y)': 'duration_of_sunshine', '(duration_of_sunshine pt10m)': 'duration_of_sunshine', 
    '(duration_of_sunshine p1h)': 'duration_of_sunshine'
    }
    if vname in rad_dict:
        ds_station[rad_dict[vname]] = ds_station[vname]
        ds_station = ds_station.drop(['{}'.format(vname)])
        print('olivia')
        return rad_dict[vname],ds_station
    else:
        return vname,ds_station

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

def parse_cfg(cfgfile):
    # Read config file
    print("Reading", cfgfile)
    with open(cfgfile, 'r') as ymlfile:
        cfgstr = yaml.full_load(ymlfile)

    return cfgstr

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

def extractdata(frostcfg,station,stmd,output):

    # Create request for observations

    # Connect and read metadata about the station
    mylog.info('Retrieving station metadata for station: %s', station)
    myrequest = 'ids='+station
    try:
        r = requests.get(frostcfg['endpointmeta'],
                myrequest,
                auth=(frostcfg['client_id'],""))
    except:
        mylog.error('Something went wrong extracting metadata.')
        raise
    # Check if the request worked, print out any errors
    if not r.ok:
        mylog.error('Returned status code was %s saying %s', r.status_code, r.text)
        #print('>>>>',r.text)
        raise
    metadata = json.loads(r.text)

    # Connect and read metadata about the variables
    print(frostcfg['timeResolutions'])
    print(frostcfg['performanceCategories'])
    mylog.info('Retrieving variables metadata for station: %s', station)
    myrequest = 'sources='+station+'&elements='+','.join(frostcfg['elements'])+'&timeresolutions='+frostcfg['timeResolutions']+'&performancecategories='+frostcfg['performanceCategories']
    try:
        r = requests.get(frostcfg['endpointparameters'],
                myrequest,
                auth=(frostcfg['client_id'],""))
    except:
        mylog.error('Something went wrong extracting metadata.')
        raise
    # Check if the request worked, print out any errors
    if not r.ok:
        mylog.error('Returned status code was %s saying %s', r.status_code, r.text)
        #print('>>>>',r.text)
        raise
    variables = json.loads(r.text)
    #print(json.dumps(variables, indent=4, sort_keys=True))
    #print(json.dumps(variables['data'], indent=4, sort_keys=True))

    # Check that the station has data in the period requested.
    # Sometimes this will fail anyway since there is no data due to technical issues and the station is still considered active.
    if 'validTo' in metadata['data'][0].keys():
        if datetime.strptime(args.startday,'%Y-%m-%d') > datetime.strptime(metadata['data'][0]['validTo'],'%Y-%m-%dT%H:%M:%S.%fZ'): 
            mylog.warn('Station %s doesn\'t contain data as late as this.', station)
            return
    if 'validFrom' in metadata['data'][0].keys():
        if datetime.strptime(args.endday,'%Y-%m-%d') < datetime.strptime(metadata['data'][0]['validFrom'],'%Y-%m-%dT%H:%M:%S.%fZ'):
            mylog.warn('Station %s doesn\'t contain data as early as this.', station)
            return
    # Create request for observations
    mylog.info('Retrieving data for station: %s', station)
    myrequest = ('sources='+station+'&elements='
        +'.'.join(frostcfg['elements'])
        +'&fields='+','.join(frostcfg['fields'])
        +'&referencetime='+'/'.join([args.startday,args.endday])+'&performancecategories='+frostcfg['performanceCategories']+'&timeresolutions='+frostcfg['timeResolutions'])
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
    # Read into Pandas DataFrame
    df = pd.read_csv(StringIO(r.text),header=0,
        mangle_dupe_cols=True, parse_dates=['referenceTime'],
        index_col=False,na_values=['-'])

    datasetstart4filename = min(df['referenceTime']).strftime('%Y%m%d')
    datasetend4filename = max(df['referenceTime']).strftime('%Y%m%d')
    datasetstart = min(df['referenceTime']).strftime('%Y-%m-%dT%H:%M:%SZ')
    datasetend = max(df['referenceTime']).strftime('%Y-%m-%dT%H:%M:%SZ')
    mytimes = (pd.to_datetime(df['referenceTime'],
        utc=True)-pd.Timestamp("1970-01-01", tz='UTC')) // pd.Timedelta('1s')
    df['time'] = mytimes
    df = df.set_index('time')
    df = df.rename(columns=lambda x: re.sub('\(-\)','',x))
    #print(variables['data'])
    elements = frostcfg['elements'][0].split(',')
    elements = [x.rstrip(', ').lstrip(' ').lower() for x in elements]
    #print(elements)
    df = df[elements].copy()
    df = df.rename(columns=lambda x: re.sub('\(','_',x))
    df = df.rename(columns=lambda x: re.sub(' ','_',x))
    df = df.rename(columns=lambda x: re.sub('\)','',x))
    print('did it work')
    #print(df)
    # Create Dataset from Dataframe
    ds_station = xr.Dataset.from_dataframe(df)
    ds_station = xr.Dataset.from_dataframe(df)
    # Specify variable attributes
    ds_station.time.attrs['standard_name'] = 'time'
    ds_station.time.attrs['units'] = 'seconds since 1970-01-01 00:00:00+0'
#    check_list = []
#    for item in variables['data']: 
#        varname = item['elementId'].lower()
#        if varname in check_list:
#            print('item exist')
#        else:
#            ds_station[varname].attrs['standard_name'] = varname
#            ds_station[varname].attrs['units'] = item['unit']
#            ds_station[varname].attrs['performance_category'] = get_performance_category(item['performanceCategory'])
#            check_list.append(varname)

    check_list = []
    for item in variables['data']: 
        vname = item['elementId'].lower()
        if vname in check_list:
            continue
        else:
            varname,ds_station = check_radiation(vname,ds_station)
            try:
                val_unit = str(item['level']['value']) + ' ' + str(item['level']['unit'])
                ds_station[varname].attrs['long_name'] = varname.replace('_',' ') + ' ' + val_unit
            except:
                ds_station[varname].attrs['long_name'] = varname.replace('_', ' ')
            ds_station[varname].attrs['standard_name'] = varname
            ds_station[varname].attrs['units'] = item['unit']
            ds_station[varname].attrs['performance_category'] = get_performance_category(item['performanceCategory'])
            ds_station[varname].attrs['keywords'] = get_keywords(varname)
            check_list.append(vname)


    # Add global attributes
    ds_station.attrs['featureType'] = 'timeSeries'
    ds_station.attrs['title'] = 'Weather station '+stmd['name']
    ds_station.attrs['summary'] = output['abstract']
    ds_station.attrs['license'] = metadata['license']
    ds_station.attrs['time_coverage_start'] = datasetstart
    ds_station.attrs['time_coverage_end'] = datasetend
    ds_station.attrs['geospatial_lat_min'] = metadata['data'][0]['geometry']['coordinates'][1]
    ds_station.attrs['geospatial_lat_max'] = metadata['data'][0]['geometry']['coordinates'][1]
    ds_station.attrs['geospatial_lon_min'] = metadata['data'][0]['geometry']['coordinates'][0]
    ds_station.attrs['geospatial_lon_max'] = metadata['data'][0]['geometry']['coordinates'][0]
    ds_station.attrs['creator_name'] = stmd['PrincipalInvestigator'] 
    ds_station.attrs['creator_email'] = stmd['PrincipalInvestigatorEmail']
    ds_station.attrs['creator_url'] = stmd['PrincipalInvestigatorOrganisationURL']
    ds_station.attrs['creator_institution'] = stmd['PrincipalInvestigatorOrganisation']
    #ds_station.attrs['keywords'] = 'Earth Science > Cryosphere > Frozen Ground > Permafrost > Permafrost Temperature,Earth Science > Land Surface > Soils > Soil temperature' # If statement?  Denne vil
    ds_station.attrs['keywords'] = output['keywords']
    ds_station.attrs['keywords_vocabulary'] = 'GCMD'
    ds_station.attrs['publisher_name'] = ''
    ds_station.attrs['publisher_email'] = 'adc@met.no'
    ds_station.attrs['publisher_url'] = 'https://adc.met.no/'
    ds_station.attrs['publisher_institution'] = 'Norwegian Meteorlogical Institute'
    ds_station.attrs['Conventions'] = 'ACDD, CF-1.8'
    ds_station.attrs['date_created'] = metadata['createdAt']
    ds_station.attrs['history'] = metadata['createdAt']+': Data extracted from the MET Observation Database through Frost and stored as NetCDF-CF'
#    ds_station.attrs['source'] = 'Soil temperature from permafrost boreholes'
    ds_station.attrs['wigosId'] = metadata['data'][0]['wigosId']
    ds_station.attrs['METNOId'] =  station
    ds_station.attrs['project'] = stmd['Project']
    #print(metadata)
    

    # Dump to Netcdf
    outputfile = output['destdir']+'/'+stmd['filename']+'_'+datasetstart4filename+'-'+datasetend4filename+'.nc'
    ds_station.to_netcdf(outputfile,
            encoding={'time': {'dtype': 'int32'}})
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

    # Loop through stations
    mylog.info('Process stations requested in configuration file.')
    for station,content in cfgstr['stations'].items():
        if station in ['SN99927']:
            continue
        mylog.info('Requesting data for: %s', station)
        #outputfile = cfgstr['output']['destdir']+'/'+content['filename']+'.nc'
        try:
            extractdata(cfgstr['frostcfg'], station, content, cfgstr['output'])
            print("it worked")
        except:
            print('what even')
            raise SystemExit()