#!/usr/bin/env python
'''
 Script to call StreamCat functions script and run allocation and
 accumulation of landscape metrics to NHDPlus catchments.  Assumes
 landscape rasters in desired projection with appropriate
 pre-processing to deal with any reclassing of values or recoding
 of NA, and directories of NHDPlusV2 data installed in standard
 directory format.
          __                                       __
    _____/ /_________  ____  ____ ___  _________ _/ /_ 
   / ___/ __/ ___/ _ \/ __ `/ __ `__ \/ ___/ __ `/ __/
  (__  ) /_/ /  /  __/ /_/ / / / / / / /__/ /_/ / /_ 
 /____/\__/_/   \___/\__,_/_/ /_/ /_/\___/\__,_/\__/ 

 Authors:  Marc Weber<weber.marc@epa.gov>,
           Ryan Hill<hill.ryan@epa.gov>,
           Darren Thornbrugh<thornbrugh.darren@epa.gov>,
           Rick Debbout<debbout.rick@epa.gov>,
           and Tad Larsen<laresn.tad@epa.gov>

 Date: November 29, 2015

 NOTE: Navigate to the directory and run script:
 > Python StreamCat.py 
 --------------------------------------------------------
'''
#TODO: remake npz files for bastards only, don't retain comids w/o upstream!
#TODO: calculate WS metrics by table math, not in numpy accumulation process
#TODO: create function like findUpstreamNPY for up and ws

import os
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime as dt
from stream_cat_config import (LYR_DIR, NHD_DIR, OUT_DIR,
                               mask_dir_RP100, mask_dir_Slp10, mask_dir_Slp20,
                               pct_full_file, pct_full_file_RP100)
from StreamCat_functions import (Accumulation, appendConnectors, createCatStats,
                                  interVPU, makeNumpyVectors, nhd_dict,
                                  PointInPoly)

# Load table of layers to be run...
ctl = pd.read_csv('ControlTable_StreamCat.csv')

if not os.path.exists('accum_npy'):
    os.mkdir('accum_npy')

INPUTS = (np.load('accum_npy/vpu_inputs.npy').item() # load the stored dict
            if os.path.exists('accum_npy/vpu_inputs.npy')
            else nhd_dict(NHD_DIR)) # create the dict and store it

totTime = dt.now()

# Load table of inter vpu connections
inter_vpu = pd.read_csv("InterVPU.csv")
    

if not os.path.exists('accum_npy'): # TODO: work out children OR bastards only
    makeNumpyVectors(inter_vpu, INPUTS, NHD_DIR)
    
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
            mask_dir = mask_dir_RP100
	elif mask == 2:
            mask_dir = mask_dir_Slp10	
        elif mask ==3:
            mask_dir = mask_dir_Slp20	
        else:
            mask_dir = ''
        LL = '%s/%s' % (LYR_DIR, ctl.LandscapeLayer[line])
        ftn = ctl.FullTableName[line]
        summaryfield = None
        if type(ctl.summaryfield[line]) == str:
            summaryfield = ctl.summaryfield[line].split(';')
        if accum_type == 'Point':  # Load in point geopandas table and Pct_Full table 
            if mask == 0: # TODO: script to create this pct_full_file
                pct_full_file = pct_full_file
            if mask == 1: # TODO: script to create point in buffer for processing?
                pct_full_file = pct_full_file_RP100
            pct_full = pd.read_csv(pct_full_file)
            points = gpd.GeoDataFrame.from_file(LL)
        if not os.path.exists(OUT_DIR + '/DBF_stash'):
            os.mkdir(OUT_DIR + '/DBF_stash')
        Connector = "%s/%s_connectors.csv" % (OUT_DIR, ftn)  # File string to store InterVPUs needed for adjustments
        catTime = dt.now()
        for zone in INPUTS:
            if not os.path.exists('%s/%s_%s.csv' % (OUT_DIR, ftn, zone)):
                hydroregion = INPUTS[zone]
                pre = '%s/NHDPlus%s/NHDPlus%s' % (NHD_DIR, hydroregion, zone)
                if not accum_type == 'Point':
                    if len(mask_dir) > 1:
                        izd = '%s/%s.tif' % (mask_dir, zone)
                    else:
                        izd = '%s/NHDPlusCatchment/cat' % (pre)
                    cat = createCatStats(accum_type, LL, izd, OUT_DIR, zone, RPU, 
                                         mask_dir, NHD_DIR, hydroregion, 
                                         apm)
                if accum_type == 'Point':
                    izd = '%s/NHDPlusCatchment/Catchment.shp' % (pre)
                    cat = PointInPoly(points, zone, izd, pct_full, mask_dir, 
                                      apm, summaryfield)
                cat.to_csv('%s/%s_%s.csv' % (OUT_DIR, ftn, zone), index=False)
                in2accum = len(cat.columns)
        print 'Cat Results Complete in : ' + str(dt.now()-catTime)     
        try: 
            #if in2accum not defined...Cat process done,but error thrown in accum
            in2accum
        except NameError:
            # get number of columns to test if accumulation needs to happen
            in2accum = len(pd.read_csv('%s/%s_%s.csv' % (OUT_DIR, 
                                                         ftn, 
                                                         zone)).columns)
        accumTime = dt.now()
        for zone in INPUTS:
            cat = pd.read_csv('%s/%s_%s.csv' % (OUT_DIR, ftn, zone))
            in2accum = len(cat.columns)
            if len(cat.columns) == in2accum:
                if zone in inter_vpu.ToZone.values:
                    cat = appendConnectors(cat, Connector, zone, inter_vpu)    
                accum = np.load('accum_npy/bastards/accum_%s.npz' % zone)
                up = Accumulation(cat, accum['comids'], 
                                       accum['lengths'], 
                                       accum['upstream'], 
                                       'UpCat')
                accum = np.load('accum_npy/children/accum_%s.npz' % zone)
                ws = Accumulation(cat, accum['comids'], 
                                       accum['lengths'], 
                                       accum['upstream'], 
                                       'Ws')
                if zone in inter_vpu.ToZone.values:
                    cat = pd.read_csv('%s/%s_%s.csv' % (OUT_DIR, ftn, zone))
                if zone in inter_vpu.FromZone.values:
                    interVPU(ws, cat.columns[1:], accum_type, zone, 
                             Connector, inter_vpu.copy(), summaryfield)
                upFinal = pd.merge(up, ws, on='COMID')
                final = pd.merge(cat, upFinal, on='COMID')
                final.to_csv('%s/%s_%s.csv' % (OUT_DIR, ftn, zone), index=False)
        print 'Accumulation Results Complete in : ' + str(dt.now()-accumTime)
print "total elapsed time " + str(dt.now()-totTime)



#ctl = pd.read_csv(r'L:\Priv\CORFiles\Geospatial_Library\Data\Project\SSWR1.1B\ControlTables\ControlTable_StreamCat_RD.csv').set_index('f_d_Title')
#ctl = pd.read_csv(r'D:\Projects\ControlTables_SSWR1.1B\ControlTable_StreamCat_RD.csv').set_index('f_d_Title')
#####################################################################################################################

def Accumulation_new(tbl, comids, lengths, upstream, tbl_type, icol='COMID'):   
    coms = tbl[icol].values  # get array of comids
    indices = swapper(coms, upstream)  # Get indices that will be used to map values
    #del upstream
    out = pd.DataFrame(index=comids)
    for col in tbl.columns[1:]:
        print col
        col_data = tbl[col].values
        x = 0
        for comid, length in zip(comids, lengths):
            upstream_idxs = indices[x: x + length]
            if 'PctFull' in col:
                area = tbl.ix[:, 1].values # is index the best way to get thie area column???
                # using nan_to_num in weighted average function to treat NA's as zeros when summing            
                accum_val = np.ma.average(col_data[upstream_idxs],
                                          weights=area[upstream_idxs])
            else:
                accum_val = np.nansum(col_data[upstream_idxs])
            out.loc[comid,col] = accum_val
            x = x + length
    return out
