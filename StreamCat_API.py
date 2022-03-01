# -*- coding: utf-8 -*-
"""
Created on Mon Dec 27 11:55:11 2021

@author: mweber
"""

import os
import json
import requests
import configparser
import pandas as pd
import math
from pprint import pprint
from datetime import datetime as dt
import csv
import urllib3
from bs4 import BeautifulSoup

def DBtablesAsDF(config_file):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Rick Debbout <debbout.rick@epa.gov>"
    Create a pandas dataframe listing all the tables currently
    in the StreamCat API database, using a config file that
    contains db url,username, password, and server
    
    Arguments
    ---------
    config_file             : configuration file with db configuration parameters
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    requests.urllib3.disable_warnings()
    r = requests.get(
        f"{config['server']['URL']}/StreamCat/admin/manage/tables",
        headers=config.defaults(),verify=False)
    json_object = json.loads(r.text)
    df=pd.DataFrame.from_records(json_object)
    return df

def ViewDBtable(config_file, table):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Rick Debbout <debbout.rick@epa.gov>"
    List table info for a specific StreamCat API database
    table, using a config file that contains db url,username, 
    password, and server
    
    Arguments
    ---------
    config_file            : configuration file with db configuration parameters
    table                  : database table name
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    r = requests.get(
    f"{config['server']['URL']}/StreamCat/admin/manage/tables/{table}",
    headers=config.defaults(),
    verify=False
    )
    pprint(json.loads(r.text))
    

def DeleteDBtable(config_file, table, just_data=False):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Rick Debbout <debbout.rick@epa.gov>"
    Delete a specific table currently in the StreamCat API 
    database, using a config file that contains db url,username, 
    password, and server
    
    Arguments
    ---------
    config                 : configuration file with db configuration parameters
    table                  : database table name
    just_data              : default=False. If True table structure will
                             be kept but just an empty table; if just_data=False
                             the entire table will be deleted
    """
    
    config = configparser.ConfigParser()
    config.read(config_file)
    requests.urllib3.disable_warnings()
    if just_data==False:
        requests.delete(f"{config['server']['URL']}/StreamCat/admin/manage/tables/{table}",
                        headers=config.defaults(),verify=False)
    if just_data==True:
        requests.delete(f"{config['server']['URL']}/StreamCat/admin/manage/tables/{table}/data",
                        headers=config.defaults(),verify=False)


def CreateDBtable(config_file, table_param):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Rick Debbout <debbout.rick@epa.gov>"
    Create a table in the StreamCat API database, using a 
    config file that contains db url,username, password, and 
    server.  This simply creates the table name, metrics and 
    columns without populating the table with data.
    
    Arguments
    ---------
    config                 : configuration file with db configuration parameters
    table_param            : dictionary object describing table
    
    Example table_param
    ---------
    table_params = {"name": "Precip_Minus_EVT",
           "metrics":[{"name": "precip_minus_evt", 
                       "display_name": "Precipitation Minus Evapotranspiration"}],
           "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
                       {"name": "Precip_Minus_EVTCat", "type": "number"},{"name": "Precip_Minus_EVTWs","type": "number"}]}
    """
    
    config = configparser.ConfigParser()
    config.read(config_file)
    requests.urllib3.disable_warnings()
    table_params_json = json.dumps(table_params)
    response = requests.put(f"{config['server']['URL']}/StreamCat/admin/manage/tables/{table_params['name']}/", 
                            headers=config.defaults(), verify=False, data = table_params_json)
    return(response)

def PopulateDBtable(config_file, table, file_loc, temp_file):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Rick Debbout <debbout.rick@epa.gov>"
    Populate a table in the StreamCat API database, using a 
    config file that contains db url,username, password, and 
    server.  Provide location for hydroregion csv files to load
    and parse to pass to database.
    
    Arguments
    ---------
    config                 : configuration file with db configuration parameters
    table                  : name of metric table to load
    file_loc               : directory path of hydroregion csv files
    temp_file              : temp csv and path to write to, e.g. 'C:/temp/junk.csv'
    """
    
    config = configparser.ConfigParser()
    config.read(config_file)
    requests.urllib3.disable_warnings()
    files = [f for f in os.listdir(file_loc) if f.split('_Region')[0]==table and not f.count('Archive')]
    
    counter=0
    for file in files:
        infile = file_loc + '/' + file
        df = pd.read_csv(infile)
        counter+=len(df)
        iterations = int(math.ceil(len(df)/10000))
        for i in range(0,iterations):
            start=i*10000
            stop=((i+1)*10000)
            temp=df.iloc[start:stop]
            temp.to_csv(f'{temp_file}', index=False)
            filedata = open(f'{temp_file}', "rb")
    
            requests.patch(f"{config['server']['URL']}/StreamCat/admin/manage/tables/{table}/data", 
                       headers=config.defaults(), verify=False, data=filedata)
            filedata.close()
        
def ShowHideDBtable(config_file, table, activate):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Rick Debbout <debbout.rick@epa.gov>"
    Show or hide a table in the StreamCat API database, using a 
    config file that contains db url,username, password, and 
    server.  This makes a particular table visible or invisible 
    to users of the database API.
    
    Arguments
    ---------
    config                 : configuration file with db configuration parameters
    table                  : name of database table to show or hide
    activate               : 0 for hide, 1 for show
    """
    
    config = configparser.ConfigParser()
    config.read(config_file)
    requests.urllib3.disable_warnings()
    if activate==1:
        data='{"active":1}' 
    if activate==0:
        data='{"active":0}'
    response = requests.patch(f"{config['server']['URL']}/StreamCat/admin/manage/tables/{table_params['name']}/", 
                            headers=config.defaults(), verify=False, data = data)
    return(response)

def MissingAPImetrics(config_file):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Rick Debbout <debbout.rick@epa.gov>"
    List metrics currently on StreamCat ftp site that do not yet
    exist in the API database.
    
    Arguments
    ---------
    config_file             : configuration file with db configuration parameters
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    requests.urllib3.disable_warnings()
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    r = requests.get("https://gaftp.epa.gov/epadatacommons/ORD"
    "/NHDPlusLandscapeAttributes/StreamCat/HydroRegions/",
    verify=False)
    soup = BeautifulSoup(r.text, features="lxml")

    ftp_mets = [link.get("href") for link in soup.find_all("a")[5:]]
    ftp_mets=list(set([x.split('_Region')[0] for x in ftp_mets]))
    
    r = requests.get(
        f"{config['server']['URL']}/StreamCat/admin/manage/tables",
        headers=config.defaults(),verify=False)
    json_object = json.loads(r.text)
    api_mets=pd.DataFrame.from_records(json_object)
    
    api_mets = list(api_mets['DSNAME'])
    
    published = [x for x in ftp_mets if x in api_mets]
    not_published = [x for x in ftp_mets if not x in api_mets]
    return(published, not_published)


# Define config file        
config_file='E:/GitProjects/NARS/NARS/api/api_config.ini'
# List tables
test = DBtablesAsDF(config_file)
test.head()
test.tail()
test['DSNAME'][0:20]
test['DSNAME'][21:40]
test['DSNAME'][41:60]
# View a particular table
table='RoadDensityRipBuf100'
# table='ImperviousSurfaces'
table='RoadStreamCrossings'
# table='MinesRipBuf100'
# table='NLCD2001'
ViewDBtable(config_file, table)

# Delete a tables
DeleteDBtable(config_file, table, just_data =True)
# DeleteDBtable(config_file, table, just_data =False)
# Create a table
# table_params = {"name": "Precip_Minus_EVT",
#            "metrics":[{"name": "precip_minus_evt", 
#                        "display_name": "Precipitation Minus Evapotranspiration"}],
#            "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
#                        {"name": "Precip_Minus_EVTCat", "type": "number"},{"name": "Precip_Minus_EVTWs","type": "number"}]}

table_params = {"name": "ImperviousSurfaces",
           "metrics":[{"name": "pctimp2001", "display_name": "Mean Imperviousness 2001"},
                      {"name": "pctimp2004", "display_name": "Mean Imperviousness 2004"},
                      {"name": "pctimp2006", "display_name": "Mean Imperviousness 2006"},
                      {"name": "pctimp2008", "display_name": "Mean Imperviousness 2008"},
                      {"name": "pctimp2011", "display_name": "Mean Imperviousness 2011"},
                      {"name": "pctimp2013", "display_name": "Mean Imperviousness 2013"},
                      {"name": "pctimp2016", "display_name": "Mean Imperviousness 2016"},
                      {"name": "pctimp2019", "display_name": "Mean Imperviousness 2019"},],
           "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
                       {"name": "PctImp2001Cat", "type": "number"},{"name": "PctImp2001Ws","type": "number"},
                       {"name": "PctImp2004Cat", "type": "number"},{"name": "PctImp2004Ws","type": "number"},
                       {"name": "PctImp2006Cat", "type": "number"},{"name": "PctImp2006Ws","type": "number"},
                       {"name": "PctImp2008Cat", "type": "number"},{"name": "PctImp2008Ws","type": "number"},
                       {"name": "PctImp2011Cat", "type": "number"},{"name": "PctImp2011Ws","type": "number"},
                       {"name": "PctImp2013Cat", "type": "number"},{"name": "PctImp2013Ws","type": "number"},
                       {"name": "PctImp2016Cat", "type": "number"},{"name": "PctImp2016Ws","type": "number"},
                       {"name": "PctImp2019Cat", "type": "number"},{"name": "PctImp2019Ws","type": "number"}]}


test=CreateDBtable(config_file, table_params)
print(test.headers)
print(test)

# Populate a table
# table='MinesRipBuf100'
# table='Precip_Minus_EVT'
# table='NLCD2001'
# table='GeoChemPhys2'
table='RoadDensityRipBuf100'
table='RoadStreamCrossings'
file_loc='O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/FTP_Staging/HydroRegions'
temp_file='E:/WorkingData/junk.csv'
LoadTime = dt.now()
PopulateDBtable(config_file, table, file_loc, temp_file)
print("Table load complete in : " + str(dt.now() - LoadTime))

# View a particular table
table='EPA_FRS'
# table='Precip_Minus_EVT'
# table='dams'
ViewDBtable(config_file, table)

# Show or hide a particular table
table='ImperviousSurfaces'
ShowHideDBtable(config_file, table, activate=1)

# list metrics on ftp site published and not published to API database
published, unpublished = MissingAPImetrics(config_file)

