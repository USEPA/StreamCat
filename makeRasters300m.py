# -*- coding: utf-8 -*-
"""
Created on Fri May  3 11:18:05 2019
Purpose: Create National map of StreamCat metrics
@author: mweber
"""
import sys, arcpy
from arcpy.sa import *
arcpy.CheckOutExtension("spatial")
arcpy.env.overwriteOutput = True
from raster_function import catcsv2raster2
import pandas as pd
import numpy as np
import glob

streamcat_dir = 'L:/Priv/CORFiles/Geospatial_Library_Projects/StreamCat/FTP_Staging/StreamCat/HydroRegions'
template_dir = 'L:/Priv/CORFiles/Geospatial_Library_Projects/SSWR1.1B/PredictionVisualizations/RasterTemplate/'
inTemplate = template_dir + 'comid300m_clp.tif'
out_dir = 'H:/WorkingData/'

# Read in all the final tables for a given metric for each hydroregion and combine into one pandas dataframe
all_files = glob.glob(streamcat_dir + "/Compton*.csv")
li = []
for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)

frame = pd.concat(li, axis=0, ignore_index=True)

# specify metrics to map  
wi_names = ['Phos_Ag_BalanceWs', 'Phos_Crop_UptakeWs',  'Phos_FertWs',   'Phos_ManureWs']

for i in wi_names:
    print i
    Value = i
    outName = i
    outRas = out_dir + 'CI_' + outName + '_300m.tif'
    catcsv2raster2(frame, Value, inTemplate, outRas, dtype='Float')






