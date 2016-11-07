#!/usr/bin/env python

# Script to call StreamCat functions script and run allocation and
# accumulation of landscape metrics to NHDPlus catchments.  Assumes
# landscape rasters in desired projection with appropriate
# pre-processing to deal with any reclassing of values or recoding
# of NA, and directories of NHDPlusV2 data installed in standard
# directory format.
#          __                                       __
#    _____/ /_________  ____  ____ ___  _________ _/ /_ 
#   / ___/ __/ ___/ _ \/ __ `/ __ `__ \/ ___/ __ `/ __/
#  (__  ) /_/ /  /  __/ /_/ / / / / / / /__/ /_/ / /_ 
# /____/\__/_/   \___/\__,_/_/ /_/ /_/\___/\__,_/\__/ 
#
# Authors:  Marc Weber<weber.marc@epa.gov>,
#           Ryan Hill<hill.ryan@epa.gov>,
#           Darren Thornbrugh<thornbrugh.darren@epa.gov>,
#           Rick Debbout<debbout.rick@epa.gov>,
#           and Tad Larsen<laresn.tad@epa.gov>
#
# Date: November 29, 2015
#
# NOTE: run script from command line passing directory and name of this script
# and then directory and name of the control table to use like:
# > Python "F:\Watershed Integrity Spatial Prediction\Scripts\StreamCat.py"
# L:\Priv\CORFiles\Geospatial_Library\Data\Project\SSWR1.1B\ControlTables\ControlTable_StreamCat.csv
# --------------------------------------------------------

import sys
import os
import pandas as pd
import numpy as np
# Load table used in function argument
ctl = pd.read_csv(sys.argv[1])
#ctl = pd.read_csv('D:/Projects/StreamCat/ControlTable_StreamCat.csv')

# Import system modules
from datetime import datetime as dt
import geopandas as gpd
sys.path.append(ctl.DirectoryLocations.values[5])  # sys.path.append('D:/Projects/Scipts')
from StreamCat_functions import createAccumTable, appendConnectors, createCatStats, interVPU, PointInPoly, makeNumpyVectors, makeVPUdict
#####################################################################################################################
# Populate variables from control table
ingrid_dir = ctl.DirectoryLocations.values[0]
NHD_dir = ctl.DirectoryLocations.values[1]
out_dir = ctl.DirectoryLocations.values[2]
numpy_dir = '%s/StreamCat_npy' % NHD_dir
interVPU_dir = ctl.DirectoryLocations.values[3]
#####################################################################################################################
totTime = dt.now()
interVPUtbl = pd.read_csv(interVPU_dir)  # Load Inter_VPU table
if not os.path.exists('%s/StreamCat_npy' % NHD_dir):
    os.mkdir('%s/StreamCat_npy' % NHD_dir)    
if not os.path.exists('%s/StreamCat_npy/zoneInputs.npy' % NHD_dir):
    inputs = makeVPUdict(NHD_dir)
else:
    inputs = np.load('%s/StreamCat_npy/zoneInputs.npy' % NHD_dir).item()
if not os.path.exists('%s/children' % numpy_dir):
    makeNumpyVectors(numpy_dir, interVPUtbl, inputs, NHD_dir)
    
for line in range(len(ctl.values)):  # loop through each FullTableName in control table
    if ctl.run[line] == 1:   # check 'run' field from the table, if 1 run, if not, skip
        # break
        print 'running ' + str(ctl.FullTableName[line])
        accum_type = ctl.accum_type[line]             # Load metric specific variables
        RPU = int(ctl.by_RPU[line])
        mask = ctl.use_mask[line]
        appendMetric = ctl.AppendMetric[line]
        if appendMetric == 'none':
            appendMetric = '' 
        if mask == 1:
            mask_dir = ctl.DirectoryLocations.values[7]
        elif mask == 2:
            mask_dir = ctl.DirectoryLocations.values[9]
        elif mask ==3:
            mask_dir = ctl.DirectoryLocations.values[10]
        else:
            mask_dir = ''
        LandscapeLayer = '%s/%s' % (ingrid_dir, ctl.LandscapeLayer[line])  # ingrid = 'D:/Projects/lakesAnalysis/MetricData/' + 'mines_rpBuf100.shp'
        if not os.path.exists(LandscapeLayer) and RPU == 1:  # this is currently a placeholder for scripting the select by location process to get masked point file
            print "This shouldn't happen yet"
            # make masked tables for points, Used QGIS 'point sampling tool' to make the rpBuf100 files
        FullTableName = ctl.FullTableName[line]
        summaryfield = None
        if type(ctl.summaryfield[line]) == str:
            summaryfield = ctl.summaryfield[line].split(';')
        if accum_type == 'Point':  # Load in point geopandas table and Pct_Full table 
            if mask == 0:
                pct_full_file = ctl.DirectoryLocations.values[4]
            if mask == 1:
                pct_full_file = ctl.DirectoryLocations.values[8]
            pct_full = pd.read_csv(pct_full_file)
            points = gpd.GeoDataFrame.from_file(LandscapeLayer)
        if not os.path.exists(out_dir):
            os.mkdir(out_dir)
        Connector = "%s/%s_connectors.csv" % (out_dir, FullTableName)  # File string to store InterVPUs needed for adjustments
        catTime = dt.now()
        for zone in inputs:
            if not os.path.exists(out_dir + '/' + FullTableName + '_' + zone + '.csv'):
                hydroregion = inputs[zone]
                if not accum_type == 'Point':
                    if len(mask_dir) > 1:
                        inZoneData = '%s/%s.tif' % (mask_dir, zone)
                    else:
                        inZoneData = NHD_dir + '/NHDPlus%s/NHDPlus%s/NHDPlusCatchment/cat' % (hydroregion, zone)  # NHD_dir = 'C:/Users/Rdebbout/temp/NHDPlusV21' hydroregion = 'MS' zone = '10U' hydroregion = 'SR' zone = '09'
                    cat = createCatStats(accum_type, LandscapeLayer, inZoneData, out_dir, zone, RPU, mask_dir, NHD_dir, hydroregion, appendMetric)
                if accum_type == 'Point':
                    inZoneData = NHD_dir + '/NHDPlus%s/NHDPlus%s/NHDPlusCatchment/Catchment.shp' % (hydroregion, zone)
                    cat = PointInPoly(points, zone, inZoneData, pct_full, mask_dir, appendMetric, summaryfield)
                cat.to_csv('%s/%s_%s.csv' % (out_dir, FullTableName, zone), index=False)
                in2accum = len(cat.columns)
        print 'Cat Results Complete in : ' + str(dt.now()-catTime)     
        try:
            in2accum
        except NameError:
            in2accum = len(pd.read_csv('%s/%s_%s.csv' % (out_dir, FullTableName, zone)).columns)
        accumTime = dt.now()
        for zone in inputs:
            cat = pd.read_csv(out_dir + '/' + FullTableName + '_' + zone + '.csv')
            in2accum = len(cat.columns)
            if len(cat.columns) == in2accum:
                if zone in interVPUtbl.ToZone.values:
                    cat = appendConnectors(cat, Connector, zone, interVPUtbl)
                up = createAccumTable(cat, numpy_dir, zone, tbl_type='UpCat')
                ws = createAccumTable(cat, numpy_dir, zone, tbl_type='Ws')
                if zone in interVPUtbl.ToZone.values:
                    cat = pd.read_csv(out_dir + '/' + FullTableName + '_' + zone + '.csv')
                if zone in interVPUtbl.FromZone.values:
                    interVPU(ws, cat.columns[1:], accum_type, zone, Connector, interVPUtbl.copy(), summaryfield)
                upFinal = pd.merge(up, ws, on='COMID')
                final = pd.merge(cat, upFinal, on='COMID')
                final.to_csv(out_dir + '/' + FullTableName + '_' + zone + '.csv', index=False)
        print 'Accumulation Results Complete in : ' + str(dt.now()-accumTime)
print "total elapsed time " + str(dt.now()-totTime)

