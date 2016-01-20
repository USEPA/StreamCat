# Script to call StreamCat functions script and run allocation and accumulation of landscape metrics to
# NHDPlus catchments.  Assumes landscape rasters in desired projection with appropriate pre-processing to deal with
# any reclassing of values or recoding of NA, and directories of NHDPlusV2 data installed in standard directory format
# Authors: Marc Weber<weber.marc@epa.gov>, Ryan Hill<hill.ryan@epa.gov>,
#          Darren Thornbrugh<thornbrugh.darren@epa.gov>, Rick Debbout<debbout.rick@epa.gov>, 
#          and Tad Larsen<laresn.tad@epa.gov>
# Date: November 30, 2015
# ArcGIS 10.2.1, Python 2.7

import sys, os
import pandas as pd 
# Load table used in function argument
ctl = pd.read_csv(sys.argv[1])

# Import system modules
from collections import  OrderedDict
from datetime import datetime as dt
import arcpy
import geopandas as gpd
sys.path.append(ctl.DirectoryLocations.values[6])
from StreamCat_functions import createAccumTable, appendConnectors, createCatStats, interVPU, dbf2DF, PointInPoly
#####################################################################################################################
# Populate variables from control table
ingrid_dir =  ctl.DirectoryLocations.values[0]
NHD_dir = ctl.DirectoryLocations.values[1]
out_dir = ctl.DirectoryLocations.values[2]
numpy_dir = ctl.DirectoryLocations.values[3]
interVPU_dir = ctl.DirectoryLocations.values[4]
pct_full_file = ctl.DirectoryLocations.values[5]
repo = ctl.DirectoryLocations.values[6] 
#####################################################################################################################
inputs = OrderedDict([('10U','MS'),('10L','MS'),('07','MS'),('11','MS'),('06','MS'),('05','MS'),('08','MS'),\
                      ('01','NE'),('02','MA'),('03N','SA'),('03S','SA'),('03W','SA'),('04','GL'),('09','SR'),\
                      ('12','TX'),('13','RG'),('14','CO'),('15','CO'),('16','GB'),('17','PN'),('18','CA')])
totTime = dt.now()
for line in ctl.values: # loop through each landscape_var in control table 
    if line[-1] == 1:   # check 'run' field from the table, if 1 run, if not, skip
        print 'running ' + str(line[2:])
        accum_type = line[3]             # Load metric specific variables
        ingrid = ingrid_dir + line[4]
        mask_layer = ''
        landscape_var = line[2]
        if type(line[5]) == str:
            summaryfield = line[5].split(',')
        else:
            summaryfield = None    
        interVPUtbl = pd.read_csv(interVPU_dir)  # Load Inter_VPU table
        if accum_type == 'Count': # Load in point geopandas table and Pct_Full table 
            pct_full = dbf2DF(pct_full_file)[['FEATUREID','PCT_FULL']]
            pct_names = ['COMID','CatPctFull']
            pct_full['PCT_FULL'] = pct_full['PCT_FULL']*100
            pct_full.columns = pct_names
            points  = gpd.GeoDataFrame.from_file(ingrid) 
        if not os.path.exists(out_dir):
        	os.mkdir(out_dir)
        Connector = "%s/%s_connectors.csv"%(out_dir,landscape_var) # File string to store InterVPUs needed for adjustments
        catTime = dt.now()
        for zone in inputs:
            if not os.path.exists(out_dir + '/' + landscape_var + '_' + zone + '.csv'):
                hydroregion = inputs[zone]          
                inZoneData = NHD_dir + '/NHDPlus%s/NHDPlus%s/NHDPlusCatchment/cat'%(hydroregion, zone)       
                if len(mask_layer) > 1:
                    arcpy.env.mask = mask_layer + "_" + zone + '.tif'
                if not accum_type == 'Count':
                    inZoneData = NHD_dir + '/NHDPlus%s/NHDPlus%s/NHDPlusCatchment/cat'%(hydroregion, zone)
                    cat = createCatStats(accum_type, ingrid, inZoneData, out_dir, zone)
                if accum_type == 'Count':
                    inZoneData = NHD_dir + '/NHDPlus%s/NHDPlus%s/NHDPlusCatchment/Catchment.shp'%(hydroregion, zone)
                    cat = PointInPoly(points,inZoneData,pct_full,summaryfield)
                cat.to_csv(out_dir + '/' + landscape_var + '_' + zone + '.csv', index=False)
                in2accum = len(cat.columns)
        print 'Cat Results Complete in : ' + str(dt.now()-catTime) 
        accumTime = dt.now()
        for zone in inputs:           
            cat = pd.read_csv(out_dir + '/' + landscape_var + '_' + zone + '.csv')
            if len(cat.columns) == in2accum:
                if zone in interVPUtbl.ToZone.values:
                    cat = appendConnectors(cat, Connector, zone, interVPUtbl)
                up = createAccumTable(cat, numpy_dir, zone, tbl_type = 'UpCat')
                ws = createAccumTable(cat, numpy_dir, zone, tbl_type = 'Ws')
                if zone in interVPUtbl.ToZone.values:
                    cat = pd.read_csv(out_dir + '/' + landscape_var + '_' + zone + '.csv')
                if zone in interVPUtbl.FromZone.values: 
                    interVPU(ws, cat.columns[1:], accum_type, zone, Connector, interVPUtbl.copy(),summaryfield)			        
                upFinal = pd.merge(up,ws,on='COMID')
                final = pd.merge(cat,upFinal,on='COMID') 
                final.to_csv(out_dir + '/' + landscape_var + '_' + zone + '.csv', index=False)
        print 'Accumulation Results Complete in : ' + str(dt.now()-accumTime)
print "total elapsed time " + str(dt.now()-totTime)

