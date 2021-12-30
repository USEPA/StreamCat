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
    

def DeleteDBtable(config_file, table):
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
    """
    
    config = configparser.ConfigParser()
    config.read(config_file)
    requests.urllib3.disable_warnings()
    requests.delete(
    f"{config['server']['URL']}/StreamCat/admin/manage/tables/{table}",
    headers=config.defaults(),
    verify=False
)


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
    files = [f for f in os.listdir(file_loc) if f.count(table) and not f.count('Archive') and not f.count('RipBuf')]
    
    counter=0
    for file in files[1:len(files)]:
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

# Define config file        
config_file='E:/GitProjects/NARS/NARS/api/api_config.ini'
# List tables
test = DBtablesAsDF(config_file)
test.head()
test.tail()
test['DSNAME'][0:20]

# View a particular table
table='Precip_Minus_EVT'
# table='BFI'
ViewDBtable(config_file, table)

# Delete a table
DeleteDBtable(config_file, table)

# Create a table
table_params = {"name": "Precip_Minus_EVT",
           "metrics":[{"name": "precip_minus_evt", 
                       "display_name": "Precipitation Minus Evapotranspiration"}],
           "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
                       {"name": "Precip_Minus_EVTCat", "type": "number"},{"name": "Precip_Minus_EVTWs","type": "number"}]}

test=CreateDBtable(config_file, table_params)
print(test.headers)
print(test)
print(test.text)

# Populate a table
table='Precip_Minus_EVT'
file_loc='O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/FTP_Staging/HydroRegions'
temp_file='E:/WorkingData/junk.csv'
LoadTime = dt.now()
PopulateDBtable(config_file, table, file_loc, temp_file)
print("Table load complete in : " + str(dt.now() - LoadTime))

# View a particular table
table='Precip_Minus_EVT'
table='dams'
ViewDBtable(config_file, table)

# Show or hide a particular table
table='Precip_Minus_EVT'
ShowHideDBtable(config_file, table, activate=1)
