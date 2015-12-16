# Script to call StreamCat functions script and run allocation and accumulation of landscape metrics to
# NHDPlus catchments.  Assumes landscape rasters in desired projection with appropriate pre-processing to deal with
# any reclassing of values or recoding of NA, and directories of NHDPlusV2 data installed in standard directory format
# Authors: Marc Weber<weber.marc@epa.gov>, Ryan Hill<hill.ryan@epa.gov>,
#          Darren Thornbrugh<thornbrugh.darren@epa.gov>, Rick Debbout<debbout.rick@epa.gov>, 
#          and Tad Larsen<laresn.tad@epa.gov>
# Date: November 30, 2015
# ArcGIS 10.2.1, Python 2.7

# Import system modules
import sys, os
import pandas as pd
from collections import  OrderedDict
from datetime import datetime as dt
import arcpy
import geopandas as gpd
#sys.path.append('C:/Users/Rdebbout/Scipts')
sys.path.append('F:/Watershed Integrity Spatial Prediction/Scripts')
from StreamCat_functions import createAccumTable, appendConnectors, arcpySettings, makeOutTable, createCatStats, interVPUfix2, dbf2DF, PointInPoly
#####################################################################################################################
inputs = OrderedDict([('10U','MS'),('10L','MS'),('07','MS'),('11','MS'),('06','MS'),('05','MS'),('08','MS'),\
                      ('01','NE'),('02','MA'),('03N','SA'),('03S','SA'),('03W','SA'),('04','GL'),('09','SR'),\
                      ('12','TX'),('13','RG'),('14','CO'),('15','CO'),('16','GB'),('17','PN'),('18','CA')])
#  Set parameters here
accum_type = 'Continuous'
ingrid = 'L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/LandscapeRasters/QAComplete/popden2010_v2.tif'
summaryfield = ['NIDStorM3', 'NrmStorM3']
mask_layer = ''#L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/PhaseTwo/LandscapeRasters/QAcomplete/WaterMask/Mosaics/RipBuf100
NHD_dir = "H:/NHDPlusV21"
out_dir = 'J:/Watershed Integrity Spatial Prediction/AccumulationResults'
landscape_var = 'PopDen'
numpy_dir = 'J:/Watershed Integrity Spatial Prediction/npStreamCat'
interVPU_dir = "L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/InterVPUtable/InterVPU.csv"
pct_full_file = 'L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/Q/BorderCatchmentPCT_FULL/catFINAL_Clip.dbf'
#####################################################################################################################
startTime = dt.now()
# Load Inter_VPU table
interVPUtbl = pd.read_csv(interVPU_dir)
if accum_type != 'Count':
    arcpySettings()
if accum_type == 'Count':
    print 'Load Pct_Full Table'
    pct_full = dbf2DF(pct_full_file)[['FEATUREID','PCT_FULL']]
    pct_names = ['COMID','CatPctFull']
    pct_full['PCT_FULL'] = pct_full['PCT_FULL']*100
    pct_full.columns = pct_names
    print 'Load Point table'
    points  = gpd.GeoDataFrame.from_file(ingrid) 
if not os.path.exists(out_dir):
	os.mkdir(out_dir)
Connector = "%s/%s_connectors.csv"%(out_dir,landscape_var)
for zone in inputs:
    print zone
    if not os.path.exists(out_dir + '/' + landscape_var + '_' + zone + '.csv'):
        hydroregion = inputs[zone]          
        inZoneData = NHD_dir + '/NHDPlus%s/NHDPlus%s/NHDPlusCatchment/cat'%(hydroregion, zone)       
        outTable = makeOutTable(ingrid, out_dir, zone)
        if len(mask_layer) > 1:
            arcpy.env.mask = mask_layer + "_" + zone + '.tif'
        if not accum_type == 'Count':
            inZoneData = NHD_dir + '/NHDPlus%s/NHDPlus%s/NHDPlusCatchment/cat'%(hydroregion, zone)
            cat = createCatStats(accum_type, outTable, ingrid, inZoneData)
        if accum_type == 'Count':
            inZoneData = NHD_dir + '/NHDPlus%s/NHDPlus%s/NHDPlusCatchment/Catchment.shp '%(hydroregion, zone)
            cat = PointInPoly(points,inZoneData,pct_full,summaryfield)
        cat.to_csv(out_dir + '/' + landscape_var + '_' + zone + '.csv', index=False)
print 'CatResults Complete'       
for zone in inputs:
    print zone           
    cat = pd.read_csv(out_dir + '/' + landscape_var + '_' + zone + '.csv')
    if zone in interVPUtbl.ToZone.values:
        cat = appendConnectors(cat, Connector, zone, interVPUtbl)
    up = createAccumTable(cat, numpy_dir, zone, tbl_type = 'UpCat')
    ws = createAccumTable(cat, numpy_dir, zone, tbl_type = 'Ws')
    if zone in interVPUtbl.ToZone.values:
        cat = pd.read_csv(out_dir + '/' + landscape_var + '_' + zone + '.csv')
    if zone in interVPUtbl.FromZone.values: 
        print 'get interVPUs'
        interVPUfix2(ws, cat.columns[1:], accum_type, zone, Connector, interVPUtbl.copy())			        
    upFinal = pd.merge(up,ws,on='COMID')
    final = pd.merge(cat,upFinal,on='COMID') 
    final.to_csv(out_dir + '/' + landscape_var + '_' + zone + '.csv', index=False)  
print "total elapsed time " + str(dt.now()-startTime)
