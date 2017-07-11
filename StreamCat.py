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
# > Python "J:/GitProjects/StreamCat/StreamCat.py"
# L:\Priv\CORFiles\Geospatial_Library\Data\Project\SSWR1.1B\ControlTables\ControlTable_StreamCat.csv
# --------------------------------------------------------

import sys
import os
import pandas as pd
import numpy as np
# Load table used in function argument
ctl = pd.read_csv(sys.argv[1]).set_index('f_d_Title')
#ctl = pd.read_csv(r'L:\Priv\CORFiles\Geospatial_Library\Data\Project\SSWR1.1B\ControlTables\ControlTable_StreamCat_RD.csv').set_index('f_d_Title')
#ctl = pd.read_csv(r'D:\Projects\ControlTables_SSWR1.1B\ControlTable_StreamCat_RD.csv').set_index('f_d_Title')
# Import system modules
from datetime import datetime as dt
import geopandas as gpd
dls = 'DirectoryLocations'
sys.path.append(ctl.ix['StreamCat_repo'][dls])  # sys.path.append('D:/Projects/Scipts')
from StreamCat_functions import Accumulation, appendConnectors, createCatStats, interVPU, PointInPoly, makeNumpyVectors, NHD_Dict
#####################################################################################################################
# Populate variables from control table
igd = ctl.ix['ingrid_dir'][dls]
nhd = ctl.ix['NHD_dir'][dls]
out = ctl.ix['out_dir'][dls]
npy = '%s/StreamCat_npy' % nhd
#####################################################################################################################

totTime = dt.now()
# Load Inter_VPU table
interVPUtbl = pd.read_csv("%s/InterVPU.csv" % ctl.ix['StreamCat_repo'][dls])
    
inputs = NHD_Dict(nhd)

if not os.path.exists('%s/children' % npy):
    makeNumpyVectors(npy, interVPUtbl, inputs, nhd)
    
for line in range(len(ctl.values)):  # loop through each FullTableName in control table
    if ctl.run[line] == 1:   # check 'run' field from the table, if 1 run, if not, skip
        # break
        print 'running ' + str(ctl.FullTableName[line])
        # Load metric specific variables
        accum_type = ctl.accum_type[line]
        RPU = int(ctl.by_RPU[line])
        mask = ctl.use_mask[line]
        apm = ctl.AppendMetric[line]
        if apm == 'none':
            apm = '' 
        if mask == 1:
            mask_dir = ctl.ix['mask_dir_RP100'][dls]
        elif mask == 2:
            mask_dir = ctl.ix['mask_dir_Slp20'][dls]
        elif mask ==3:
            mask_dir = ctl.ix['mask_dir_Slp10'][dls]
        else:
            mask_dir = ''
        LL = '%s/%s' % (igd, ctl.LandscapeLayer[line])
        ftn = ctl.FullTableName[line]
        summaryfield = None
        if type(ctl.summaryfield[line]) == str:
            summaryfield = ctl.summaryfield[line].split(';')
        if accum_type == 'Point':  # Load in point geopandas table and Pct_Full table 
            if mask == 0:
                pct_full_file = ctl.ix['pct_full_file'][dls]
            if mask == 1:
                pct_full_file = ctl.ix['pct_full_file_RP100'][dls]
            pct_full = pd.read_csv(pct_full_file)
            points = gpd.GeoDataFrame.from_file(LL)
        if not os.path.exists(out + '/DBF_stash'):
            os.mkdir(out + '/DBF_stash')
        Connector = "%s/%s_connectors.csv" % (out, ftn)  # File string to store InterVPUs needed for adjustments
        catTime = dt.now()
        for zone in inputs:
            if not os.path.exists('%s/%s_%s.csv' % (out, ftn, zone)):
                hydroregion = inputs[zone]
                pre = '%s/NHDPlus%s/NHDPlus%s' % (nhd, hydroregion, zone)
                if not accum_type == 'Point':
                    if len(mask_dir) > 1:
                        izd = '%s/%s.tif' % (mask_dir, zone)
                    else:
                        izd = '%s/NHDPlusCatchment/cat' % (pre)
                    cat = createCatStats(accum_type, LL, izd, out, zone, RPU, 
                                         mask_dir, nhd, hydroregion, 
                                         apm)
                if accum_type == 'Point':
                    izd = '%s/NHDPlusCatchment/Catchment.shp' % (pre)
                    cat = PointInPoly(points, zone, izd, pct_full, mask_dir, 
                                      apm, summaryfield)
                cat.to_csv('%s/%s_%s.csv' % (out, ftn, zone), index=False)
                in2accum = len(cat.columns)
        print 'Cat Results Complete in : ' + str(dt.now()-catTime)     
        try: 
            #if in2accum not defined...Cat process done,but error thrown in accum
            in2accum
        except NameError:
            # get number of columns to test if accumulation needs to happen
            in2accum = len(pd.read_csv('%s/%s_%s.csv' % (out, 
                                                         ftn, 
                                                         zone)).columns)
        accumTime = dt.now()
        for zone in inputs:
            cat = pd.read_csv('%s/%s_%s.csv' % (out, ftn, zone))
            in2accum = len(cat.columns)
            if len(cat.columns) == in2accum:
                if zone in interVPUtbl.ToZone.values:
                    cat = appendConnectors(cat, Connector, zone, interVPUtbl)    
                accum = np.load('%s/bastards/accum_%s.npz' % (npy ,zone))
                up = Accumulation(cat, accum['comids'], 
                                       accum['lengths'], 
                                       accum['upstream'], 
                                       'UpCat')
                accum = np.load('%s/children/accum_%s.npz' % (npy ,zone))
                ws = Accumulation(cat, accum['comids'], 
                                       accum['lengths'], 
                                       accum['upstream'], 
                                       'Ws')
                if zone in interVPUtbl.ToZone.values:
                    cat = pd.read_csv('%s/%s_%s.csv' % (out, ftn, zone))
                if zone in interVPUtbl.FromZone.values:
                    interVPU(ws, cat.columns[1:], accum_type, zone, 
                             Connector, interVPUtbl.copy(), summaryfield)
                upFinal = pd.merge(up, ws, on='COMID')
                final = pd.merge(cat, upFinal, on='COMID')
                final.to_csv('%s/%s_%s.csv' % (out, ftn, zone), index=False)
        print 'Accumulation Results Complete in : ' + str(dt.now()-accumTime)
print "total elapsed time " + str(dt.now()-totTime)

