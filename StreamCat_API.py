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
from io import StringIO
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
    #json_object = json.loads(r.text)
    #df=pd.DataFrame.from_records(json_object)
    #return df
    pprint(json.loads(r.text))
    
def ViewMetadatatable(config_file, table):
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
    f"{config['server']['URL']}/StreamCat/admin/manage/tables/{table}/variable_info",
    headers=config.defaults(),
    verify=False
    )
    df = pd.read_csv(StringIO(r.content.decode("utf8" , errors="ignore")))
    return(df)
    

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
        print(file)
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
    response = requests.patch(f"{config['server']['URL']}/StreamCat/admin/manage/tables/{table}", 
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


def UpdateMetricMetadata(config_file, table, infile, temp_file):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
    Modify metric information in the StreamCat API database, 
    using a config file that contains db url,username, password, 
    and server.  You must provide a fresh list of all variable 
    info entries for this resource, as it clears out the 
    existing list and substitutes a new one.
    
    Arguments
    ---------
    config                 : configuration file with db configuration parameters
    table                 : name of table metadata to load
    infile                 : pandas data frame from ViewMetadatatable function     
    """
    
    config = configparser.ConfigParser()
    config.read(config_file)
    requests.urllib3.disable_warnings()
    infile.to_csv(f'{temp_file}', index=False)
    filedata = open(f'{temp_file}', "rb")
    response = requests.put(f"{config['server']['URL']}/StreamCat/admin/manage/tables/{table}/variable_info", 
                            headers=config.defaults(), verify=False, data=filedata)
    return(response)

###############

# Define config file        
config_file='E:/GitProjects/NARS/NARS/api/api_config.ini'
# config_file='E:/GitProjects/NARS/NARS/api/api_config_postgres.ini'

# List tables
test = DBtablesAsDF(config_file)
test.head()
test.tail()
test['DSNAME'][0:20]
test['DSNAME'][21:40]
test['DSNAME'][41:60]
test['DSNAME'][61:70]
test['DSNAME'][61:80]
test['DSNAME'][81:120]
# View a particular table
table='ICI_IWI_v1'
table='NLCD2006'
table='NLCD2001HiSlope'
table='NLCD2019RipBuf100'
ViewDBtable(config_file, table)
# Delete a tables
DeleteDBtable(config_file, table, just_data =True)
# DeleteDBtable(config_file, table, just_data =False)

# Create a table
test = CreateDBtable(config_file, table_params)
print(test)

# Populate a table
# table='MinesRipBuf100'
# table='Precip_Minus_EVT'
# table='NLCD2001'
# table='GeoChemPhys2'
table='ImperviousSurfacesRipBuf100'
table='RoadDensityRipBuf100'
table='Dams'
table='predicted_channel_widths_depths'
# file_loc='O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/FTP_Staging/HydroRegions'
file_loc='O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/predicted-values/widths-depths_v2'
temp_file='E:/WorkingData/junk.csv'

table='MTBS'
table='WWTP'
file_loc='O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/FTP_Staging/HydroRegions'
temp_file='E:/WorkingData/junk2.csv'

LoadTime = dt.now()
PopulateDBtable(config_file, table, file_loc, temp_file)
print("Table load complete in : " + str(dt.now() - LoadTime))

# View a particular table
table='EPA_FRS'
# table='Precip_Minus_EVT'
# table='dams'
ViewDBtable(config_file, table)

# Show or hide a particular table
table='ImperviousSurfacesHiSlope'
ShowHideDBtable(config_file, table, activate=1)

# list metrics on ftp site published and not published to API database
published, unpublished = MissingAPImetrics(config_file)


# View metadata for a table
table = 'WWTP'
df = ViewMetadatatable(config_file, table)
df.loc[df.METRIC_NAME == 'WWTPAllDens[AOI]', 'METRIC_UNITS'] = "number/ km2"

# Read in .csv file of metadata
table='predicted_channel_widths_depths'
met = pd.read_csv('O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/MetaData/StreamCatMetrics.csv', encoding='cp1252')
met = met.loc[met['final_table'] == table]
met.columns = met.columns.str.upper()
met = met[df.columns]
temp_file='E:/WorkingData/junk.csv'
UpdateMetricMetadata(config_file, table, met, temp_file)

# Update metadata for a table
# make any adjustments to metrics in table and update
df['SOURCE_URL'].values[0]
df['SOURCE_URL'] = 'https://nadp.slh.wisc.edu/maps-data/ntn-gradient-maps/'
df['SOURCE_URL'].values[0]

# View metadata for a table
table = 'RefStreamTempPred'
df = ViewMetadatatable(config_file, table)

# Update metadata for a table
# make any adjustments to metrics in table and update
df['SOURCE_URL'].values[0]
df['SOURCE_URL'] = 'https://enviroatlas.epa.gov/enviroatlas/DataFactSheets/pdf/Supplemental/PotentialWetlandArea.pdf'
df['SOURCE_URL'].values[0]
temp_file='E:/WorkingData/junk.csv'
UpdateMetricMetadata(config_file, table, df, temp_file)




table_params = {"name": "predicted_channel_widths_depths",
            "metrics":[{"name": "wetted_width_m", "display_name": "Predicted wetted width"},
                       {"name": "thalweg_depth_cm", "display_name": "Predicted Thalweg Depth"},
                       {"name": "bankfull_width_m", "display_name": "Predicted Bankfull Widthy"},
                       {"name": "bankfull_depth_m", "display_name": "Predicted Bankfull Depth"}],
            "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
                        {"name": "wetted_width_m", "type": "number"},{"name": "thalweg_depth_cm","type": "number"},
                        {"name": "bankfull_width_m", "type": "number"},{"name": "bankfull_depth_m","type": "number"},]}


table_params = {"name": "AgMidHiSlopes2011",
            "metrics":[{"name": "PctAg2011Slp10Cat", "display_name": "Percent of Agriculture on 10% Slope"},
                       {"name": "PctAg2011Slp20Cat", "display_name": "Percent of Agriculture on 20% Slope"}],
            "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
                        {"name": "PctAg2011Slp10Cat", "type": "number"},{"name": "PctAg2011Slp10Ws","type": "number"},
                        {"name": "PctAg2011Slp20Cat", "type": "number"},{"name": "PctAg2011Slp20Ws","type": "number"},]}

table_params = {"name": "WaterInput",
            "metrics":[{"name": "waterinput", "display_name": "Water Input"}],
            "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
                        {"name": "WaterInputCat", "type": "number"},{"name": "WaterInputWs","type": "number"}]}

table_params = {"name": "AgDrain",
            "metrics":[{"name": "pctagdrainage", "display_name": "Agricultural Drainage"},
                       {"name": "pctno_agdrainage", "display_name": "Non Agricultural Drainage"}],
            "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
                        {"name": "PctAgDrainageCat", "type": "number"},{"name": "PctAgDrainageWs","type": "number"},
                        {"name": "PctNo_AgDrainageCat", "type": "number"},{"name": "PctNo_AgDrainageWs","type": "number"},]}

table_params = {"name": "Nsurp_NANI",
            "metrics":[{"name": "nsurp", "display_name": "Nitrogen Surplus"},
                       {"name": "nani", "display_name": "Anthropogenic Nitrogen"}],
            "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
                        {"name": "NsurpCat", "type": "number"},{"name": "NsurpWs","type": "number"},
                        {"name": "NANICat", "type": "number"},{"name": "NANIWs","type": "number"},]}


table_params = {"name": "WWTP",
            "metrics":[{"name": "wwtpmajordens", "display_name": "Major Wastewater Treatment Density"},
                       {"name": "wwtpminordens", "display_name": "Minor Wastewater Treatment Density"},
                       {"name": "wwtpalldens", "display_name": "All Wastewater Treatment Density"}],
            "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
                        {"name": "WWTPMajorDensCat", "type": "number"},{"name": "WWTPMajorDensWs","type": "number"},
                        {"name": "WWTPMinorDensCat", "type": "number"},{"name": "WWTPMinorDensWs","type": "number"},
                        {"name": "WWTPAllDensCat", "type": "number"},{"name": "WWTPAllDensWs","type": "number"},]}



table_params = {"name": "MTBS_Severity_1984",
            "metrics":[{"name": "pctnofire1984", "display_name": "Percent No Fire Burn Class For Year"},
                       {"name": "pctundsev1984", "display_name": "Percent Underburned to Low Burn Severity Class For Year"},
                       {"name": "pctlowsev1984", "display_name": "Percent Low Burn Severity Class For Year"},
                       {"name": "pctmodsev1984", "display_name": "Percent Moderate Burn Severity Class For Year"},
                       {"name": "pcthighsev1984", "display_name": "Percent High Burn Severity Class For Year"},
                       {"name": "pctincvegresp1984", "display_name": "Percent Increased Greenness and Veg Response Class For Year"},
                       {"name": "pctnonprocmask1984", "display_name": "Percent Non Processing Mask Class For Year"}],
            "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
                        {"name": "PctNoFireCat1984Cat", "type": "number"},{"name": "PctNoFire1984Ws","type": "number"},
                        {"name": "PctUndSev1984Cat", "type": "number"},{"name": "PctUndSev1984Ws","type": "number"},
                        {"name": "PctLowSev1984Cat", "type": "number"},{"name": "PctLowSev1984Ws","type": "number"},
                        {"name": "PctModSev1984Cat", "type": "number"},{"name": "PctModSev1984Ws","type": "number"},
                        {"name": "PctHighSev1984Cat", "type": "number"},{"name": "PctHighSev1984Ws","type": "number"},
                        {"name": "PctIncVegResp1984Cat", "type": "number"},{"name": "PctIncVegResp1984Ws","type": "number"},
                        {"name": "PctNonProcMask1984Cat", "type": "number"},{"name": "PctNonProcMask1984Ws","type": "number"}]}

table_params = {"name": "MTBS_Severity_2018",
            "metrics":[{"name": "pctnofire2018", "display_name": "Percent No Fire Burn Class For Year"},
                       {"name": "pctundsev2018", "display_name": "Percent Underburned to Low Burn Severity Class For Year"},
                       {"name": "pctlowsev2018", "display_name": "Percent Low Burn Severity Class For Year"},
                       {"name": "pctmodsev2018", "display_name": "Percent Moderate Burn Severity Class For Year"},
                       {"name": "pcthighsev2018", "display_name": "Percent High Burn Severity Class For Year"},
                       {"name": "pctincvegresp2018", "display_name": "Percent Increased Greenness and Veg Response Class For Year"},
                       {"name": "pctnonprocmask2018", "display_name": "Percent Non Processing Mask Class For Year"}],
            "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
                        {"name": "PctNoFire2018Cat", "type": "number"},{"name": "PctNoFire2018Ws","type": "number"},
                        {"name": "PctUndSev2018Cat", "type": "number"},{"name": "PctUndSev2018Ws","type": "number"},
                        {"name": "PctLowSev2018Cat", "type": "number"},{"name": "PctLowSev2018Ws","type": "number"},
                        {"name": "PctModSev2018Cat", "type": "number"},{"name": "PctModSev2018Ws","type": "number"},
                        {"name": "PctHighSev2018Cat", "type": "number"},{"name": "PctHighSev2018Ws","type": "number"},
                        {"name": "PctIncVegResp2018Cat", "type": "number"},{"name": "PctIncVegResp2018Ws","type": "number"},
                        {"name": "PctNonProcMask2018Cat", "type": "number"},{"name": "PctNonProcMask2018Ws","type": "number"}]}


table_params = {"name": "ImperviousSurfacesRipBuf100",
            "metrics":[{"name": "pctimp2001", "display_name": "Mean Imperviousness 2001"},
                      {"name": "pctimp2004", "display_name": "Mean Imperviousness 2004"},
                      {"name": "pctimp2006", "display_name": "Mean Imperviousness 2006"},
                      {"name": "pctimp2008", "display_name": "Mean Imperviousness 2008"},
                      {"name": "pctimp2011", "display_name": "Mean Imperviousness 2011"},
                      {"name": "pctimp2013", "display_name": "Mean Imperviousness 2013"},
                      {"name": "pctimp2016", "display_name": "Mean Imperviousness 2016"},
                      {"name": "pctimp2019", "display_name": "Mean Imperviousness 2019"}],
            "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
                        {"name": "PctImp2001CatRp100", "type": "number"},{"name": "PctImp2001WsRp100","type": "number"},
                        {"name": "PctImp2004CatRp100", "type": "number"},{"name": "PctImp2004WsRp100","type": "number"},
                        {"name": "PctImp2006CatRp100", "type": "number"},{"name": "PctImp2006WsRp100","type": "number"},
                        {"name": "PctImp2008CatRp100", "type": "number"},{"name": "PctImp2008WsRp100","type": "number"},
                        {"name": "PctImp2011CatRp100", "type": "number"},{"name": "PctImp2011WsRp100","type": "number"},
                        {"name": "PctImp2013CatRp100", "type": "number"},{"name": "PctImp2013WsRp100","type": "number"},
                        {"name": "PctImp2016CatRp100", "type": "number"},{"name": "PctImp2016WsRp100","type": "number"},
                        {"name": "PctImp2019CatRp100", "type": "number"},{"name": "PctImp2019WsRp100","type": "number"}]}

# table_params = {"name": "ImperviousSurfacesMidSlope",
#             "metrics":[{"name": "pctimp2001slp10", "display_name": "Mean Imperviousness 2001 on 10% Slope"},
#                       {"name": "pctimp2004slp10", "display_name": "Mean Imperviousness 2004 on 10% Slope"},
#                       {"name": "pctimp2006slp10", "display_name": "Mean Imperviousness 2006 on 10% Slope"},
#                       {"name": "pctimp2008slp10", "display_name": "Mean Imperviousness 2008 on 10% Slope"},
#                       {"name": "pctimp2011slp10", "display_name": "Mean Imperviousness 2011 on 10% Slope"},
#                       {"name": "pctimp2013slp10", "display_name": "Mean Imperviousness 2013 on 10% Slope"},
#                       {"name": "pctimp2016slp10", "display_name": "Mean Imperviousness 2016 on 10% Slope"},
#                       {"name": "pctimp2019slp10", "display_name": "Mean Imperviousness 2019 on 10% Slope"},],
#             "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
#                         {"name": "PctImp2001Slp10Cat", "type": "number"},{"name": "PctImp2001Slp10Ws","type": "number"},
#                         {"name": "PctImp2004Slp10Cat", "type": "number"},{"name": "PctImp2004Slp10Ws","type": "number"},
#                         {"name": "PctImp2006Slp10Cat", "type": "number"},{"name": "PctImp2006Slp10Ws","type": "number"},
#                         {"name": "PctImp2008Slp10Cat", "type": "number"},{"name": "PctImp2008Slp10Ws","type": "number"},
#                         {"name": "PctImp2011Slp10Cat", "type": "number"},{"name": "PctImp2011Slp10Ws","type": "number"},
#                         {"name": "PctImp2013Slp10Cat", "type": "number"},{"name": "PctImp2013Slp10Ws","type": "number"},
#                         {"name": "PctImp2016Slp10Cat", "type": "number"},{"name": "PctImp2016Slp10Ws","type": "number"},
#                         {"name": "PctImp2019Slp10Cat", "type": "number"},{"name": "PctImp2019Slp10Ws","type": "number"}]}

table_params = {"name": "NLCD2019",
            "metrics":[{"name": "pctbl2019", "display_name": "Bedrock and Similar Earthen Material Percentage 2019"},
                      {"name": "pctconif2019", "display_name": "Evergreeen Forest Percentage 2019"},
                      {"name": "pctcrop2019", "display_name": "Row Crop Percentage 2019"},
                      {"name": "pctdecid2019", "display_name": "Deciduous Forest Percentage 2019"},
                      {"name": "pctgrs2019", "display_name": "Grassland/Herbaceous Percentage 2019"},
                      {"name": "pcthay2019", "display_name": "Pasture/Hay Percentage 2019"},
                      {"name": "pcthbwet2019", "display_name": "Herbaceous Wetland Percentage 2019"},
                      {"name": "pctice2019", "display_name": "Ice/Snow Cover Percentage 2019"},
                      {"name": "pctmxfst2019", "display_name": "Mixed Deciduous/Evergreen Forest"},
                      {"name": "pctow2019", "display_name": "Open Water Percentage 2019"},
                      {"name": "pctshrb2019", "display_name": "Shrub/Scrub Percentage 2019"},
                      {"name": "pcturbhi2019", "display_name": "Developed, High Intensity Land Use"},
                      {"name": "pcturblo2019", "display_name": "Developed, Low Intensity Land Use"},
                      {"name": "pcturbmd2019", "display_name": "Developed, Medium Intensity Land Use"},
                      {"name": "pcturbop2019", "display_name": "Developed, Open Space Land Use Percentage"},
                      {"name": "pctwdwet2019", "display_name": "Woody Wetland Percentage 2019"}],
            "columns": [{"name": "CatPctFull", "type": "number"},{"name": "WsPctFull", "type": "number"},
                        {"name": "PctBl2019Cat", "type": "number"},{"name": "PctBl2019Ws","type": "number"},
                        {"name": "PctConif2019Cat", "type": "number"},{"name": "PctConif2019Ws","type": "number"},
                        {"name": "PctCrop2019Cat", "type": "number"},{"name": "PctCrop2019Ws","type": "number"},
                        {"name": "PctDecid2019Cat", "type": "number"},{"name": "PctDecid2019Ws","type": "number"},
                        {"name": "PctGrs2019Cat", "type": "number"},{"name": "PctGrs2019Ws","type": "number"},
                        {"name": "PctHay2019Cat", "type": "number"},{"name": "PctHay2019Ws","type": "number"},
                        {"name": "PctHbWet2019Cat", "type": "number"},{"name": "PctHbWet2019Ws","type": "number"},
                        {"name": "PctIce2019Cat", "type": "number"},{"name": "PctIce2019Ws","type": "number"},
                        {"name": "PctMxFst2019Cat", "type": "number"},{"name": "PctMxFst2019Ws","type": "number"},
                        {"name": "PctOw2019Cat", "type": "number"},{"name": "PctOw2019Ws","type": "number"},
                        {"name": "PctShrb2019Cat", "type": "number"},{"name": "PctShrb2019Ws","type": "number"},
                        {"name": "PctUrbHi2019Cat", "type": "number"},{"name": "PctUrbHi2019Ws","type": "number"},
                        {"name": "PctUrbLo2019Cat", "type": "number"},{"name": "PctUrbLo2019Ws","type": "number"},
                        {"name": "PctUrbMd2019Cat", "type": "number"},{"name": "PctUrbMd2019Ws","type": "number"},
                        {"name": "PctUrbOp2019Cat", "type": "number"},{"name": "PctUrbOp2019Ws","type": "number"},
                        {"name": "PctWdWet2019Cat", "type": "number"},{"name": "PctWdWet2019Ws","type": "number"}]}


table_params = {"name": "NLCD2001HiSlope",
            "metrics":[{"name": "pctbl2019Slp20", "display_name": "Bedrock and Similar Earthen Material Percentage 2019 "},
                      {"name": "pctconif2019Slp20", "display_name": "Evergreeen Forest Percentage 2019"},
                      {"name": "pctcrop2019Slp20", "display_name": "Row Crop Percentage 2019"},
                      {"name": "pctdecid2019Slp20", "display_name": "Deciduous Forest Percentage 2019"},
                      {"name": "pctgrs2019Slp20", "display_name": "Grassland/Herbaceous Percentage 2019"},
                      {"name": "pcthay2019Slp20", "display_name": "Pasture/Hay Percentage 2019"},
                      {"name": "pcthbwet2019Slp20", "display_name": "Herbaceous Wetland Percentage 2019"},
                      {"name": "pctice2019Slp20", "display_name": "Ice/Snow Cover Percentage 2019"},
                      {"name": "pctmxfst2019Slp20", "display_name": "Mixed Deciduous/Evergreen Forest"},
                      {"name": "pctow2019Slp20", "display_name": "Open Water Percentage 2019"},
                      {"name": "pctshrb2019Slp20", "display_name": "Shrub/Scrub Percentage 2019"},
                      {"name": "pcturbhi2019Slp20", "display_name": "Developed, High Intensity Land Use"},
                      {"name": "pcturblo2019Slp20", "display_name": "Developed, Low Intensity Land Use"},
                      {"name": "pcturbmd2019Slp20", "display_name": "Developed, Medium Intensity Land Use"},
                      {"name": "pcturbop2019Slp20", "display_name": "Developed, Open Space Land Use Percentage"},
                      {"name": "pctwdwet2019Slp20", "display_name": "Woody Wetland Percentage 2019"}],
            "columns": [{"name": "CatPctFullSlp20", "type": "number"},{"name": "WsPctFullSlp20", "type": "number"},
                        {"name": "PctBl2019Slp20Cat", "type": "number"},{"name": "PctBl2019Slp20Ws","type": "number"},
                        {"name": "PctConif2019Cat", "type": "number"},{"name": "PctConif2019Slp20Ws","type": "number"},
                        {"name": "PctCrop2019Slp20Cat", "type": "number"},{"name": "PctCrop2019Slp20Ws","type": "number"},
                        {"name": "PctDecid2019Slp20Cat", "type": "number"},{"name": "PctDecid2019Slp20Ws","type": "number"},
                        {"name": "PctGrs2019Slp20Cat", "type": "number"},{"name": "PctGrs2019Slp20Ws","type": "number"},
                        {"name": "PctHay2019Slp20Cat", "type": "number"},{"name": "PctHay2019Slp20Ws","type": "number"},
                        {"name": "PctHbWet2019Slp20Cat", "type": "number"},{"name": "PctHbWet2019Slp20Ws","type": "number"},
                        {"name": "PctIce2019Slp20Cat", "type": "number"},{"name": "PctIce2019Slp20Ws","type": "number"},
                        {"name": "PctMxFst2019Slp20Cat", "type": "number"},{"name": "PctMxFst2019Slp20Ws","type": "number"},
                        {"name": "PctOw2019Slp20Cat", "type": "number"},{"name": "PctOw2019Slp20Ws","type": "number"},
                        {"name": "PctShrb2019Slp20Cat", "type": "number"},{"name": "PctShrb2019Slp20Ws","type": "number"},
                        {"name": "PctUrbHi2019Slp20Cat", "type": "number"},{"name": "PctUrbHi2019Slp20Ws","type": "number"},
                        {"name": "PctUrbLo2019Slp20Cat", "type": "number"},{"name": "PctUrbLo2019Slp20Ws","type": "number"},
                        {"name": "PctUrbMd2019Slp20Cat", "type": "number"},{"name": "PctUrbMd2019Slp20Ws","type": "number"},
                        {"name": "PctUrbOp2019Slp20Cat", "type": "number"},{"name": "PctUrbOp2019Slp20Ws","type": "number"},
                        {"name": "PctWdWet2019Slp20Cat", "type": "number"},{"name": "PctWdWet2019Slp20Ws","type": "number"}]}

table_params = {"name": "NLCD2019RipBuf100",
            "metrics":[{"name": "pctbl2019", "display_name": "Bedrock and Similar Earthen Material Percentage 2019"},
                      {"name": "pctconif2019", "display_name": "Evergreeen Forest Percentage 2019"},
                      {"name": "pctcrop2019", "display_name": "Row Crop Percentage 2019"},
                      {"name": "pctdecid2019", "display_name": "Deciduous Forest Percentage 2019"},
                      {"name": "pctgrs2019", "display_name": "Grassland/Herbaceous Percentage 2019"},
                      {"name": "pcthay2019", "display_name": "Pasture/Hay Percentage 2019"},
                      {"name": "pcthbwet2019", "display_name": "Herbaceous Wetland Percentage 2019"},
                      {"name": "pctice2019", "display_name": "Ice/Snow Cover Percentage 2019"},
                      {"name": "pctmxfst2019", "display_name": "Mixed Deciduous/Evergreen Forest"},
                      {"name": "pctow2019", "display_name": "Open Water Percentage 2019"},
                      {"name": "pctshrb2019", "display_name": "Shrub/Scrub Percentage 2019"},
                      {"name": "pcturbhi2019", "display_name": "Developed, High Intensity Land Use"},
                      {"name": "pcturblo2019", "display_name": "Developed, Low Intensity Land Use"},
                      {"name": "pcturbmd2019", "display_name": "Developed, Medium Intensity Land Use"},
                      {"name": "pcturbop2019", "display_name": "Developed, Open Space Land Use Percentage"},
                      {"name": "pctwdwet2019", "display_name": "Woody Wetland Percentage 2019"}],
            "columns": [{"name": "CatPctFullRp100", "type": "number"},{"name": "WsPctFullRp100", "type": "number"},
                        {"name": "PctBl2019CatRp100", "type": "number"},{"name": "PctBl2019WsRp100","type": "number"},
                        {"name": "PctConif2019CatRp100", "type": "number"},{"name": "PctConif2019WsRp100","type": "number"},
                        {"name": "PctCrop2019CatRp100", "type": "number"},{"name": "PctCrop2019WsRp100","type": "number"},
                        {"name": "PctDecid2019CatRp100", "type": "number"},{"name": "PctDecid2019WsRp100","type": "number"},
                        {"name": "PctGrs2019CatRp100", "type": "number"},{"name": "PctGrs2019WsRp100","type": "number"},
                        {"name": "PctHay2019CatRp100", "type": "number"},{"name": "PctHay2019WsRp100","type": "number"},
                        {"name": "PctHbWet2019CatRp100", "type": "number"},{"name": "PctHbWet2019WsRp100","type": "number"},
                        {"name": "PctIce2019CatRp100", "type": "number"},{"name": "PctIce2019WsRp100","type": "number"},
                        {"name": "PctMxFst2019CatRp100", "type": "number"},{"name": "PctMxFst2019WsRp100","type": "number"},
                        {"name": "PctOw2019CatRp100", "type": "number"},{"name": "PctOw2019WsRp100","type": "number"},
                        {"name": "PctShrb2019CatRp100", "type": "number"},{"name": "PctShrb2019WsRp100","type": "number"},
                        {"name": "PctUrbHi2019CatRp100", "type": "number"},{"name": "PctUrbHi2019WsRp100","type": "number"},
                        {"name": "PctUrbLo2019CatRp100", "type": "number"},{"name": "PctUrbLo2019WsRp100","type": "number"},
                        {"name": "PctUrbMd2019CatRp100", "type": "number"},{"name": "PctUrbMd2019WsRp100","type": "number"},
                        {"name": "PctUrbOp2019CatRp100", "type": "number"},{"name": "PctUrbOp2019WsRp100","type": "number"},
                        {"name": "PctWdWet2019CatRp100", "type": "number"},{"name": "PctWdWet2019WsRp100","type": "number"}]}

