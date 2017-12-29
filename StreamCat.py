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
#TODO:
#TODO:
#TODO:

import os
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime as dt
from stream_cat_config import LYR_DIR, NHD_DIR, OUT_DIR, INPUTS
from StreamCat_functions import (Accumulation, appendConnectors, #cat_stats,
                                 interVPU, PointInPoly, makeNumpyVectors)

# Load table of layers to be run...
ctl = pd.read_csv('ControlTable_StreamCat.csv')

if not os.path.exists('accum_npy'):
    os.mkdir('accum_npy')

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
            mask_dir = ctl.ix['mask_dir_RP100'][dls]
        elif mask == 2:
            mask_dir = ctl.ix['mask_dir_Slp20'][dls]
        elif mask ==3:
            mask_dir = ctl.ix['mask_dir_Slp10'][dls]
        else:
            mask_dir = ''
        LL = '%s/%s' % (LYR_DIR, ctl.LandscapeLayer[line])
        ftn = ctl.FullTableName[line]
        summaryfield = None
        if type(ctl.summaryfield[line]) == str:
            summaryfield = ctl.summaryfield[line].split(';')
        if accum_type == 'Point':  # Load in point geopandas table and Pct_Full table 
            if mask == 0: # TODO: script to create this pct_full_file
                pct_full_file = ctl.ix['pct_full_file'][dls]
            if mask == 1: # TODO: script to create point in buffer for processing?
                pct_full_file = ctl.ix['pct_full_file_RP100'][dls]
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
                    cat = cat_stats(accum_type, LL, izd, OUT_DIR, zone, RPU, 
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

import numpy as np
import pandas as pd
from StreamCat_functions import dbf2DF

def swapper(coms, upstream):

    bsort = np.argsort(coms)
    apos = np.searchsorted(coms[bsort], upstream)
    indices = bsort[apos]
    return indices


def Accumulation(tbl, comids, lengths, upstream, tbl_type, icol='COMID'):

    coms = tbl[icol].values  # get array of comids

    indices = swapper(coms, upstream)  #Get indices that will be used to map values
    del upstream  # a and indices are big - clean up to minimize RAM
    cols = tbl.columns[1:]  #Get column names that will be accumulated
    z = np.zeros(comids.shape)  #Make empty vector for placing values
    #TODO: Do we need to use np.arrays??
    outT = np.zeros((len(comids), len(tbl.columns)))  #Make empty array for placing final values
    outT[:,0] = comids  #Define first column as comids
    #Loop and accumulate values
    for k in range(0,len(cols)):
        # TODO: col in cols???
        
        col = cols[k]
        c = np.array(tbl[col]) # tbl[col].fillna(0) keep out zeros where no data!
        d = c[indices] #Make final vector from desired data (c)
        if 'PctFull' in col:
            area = np.array(tbl.ix[:, 1])
            ar = area[indices]
            x = 0
            for i in range(0, len(lengths)):
                # using nan_to_num in average function to treat NA's as zeros when summing
                z[i] = np.ma.average(np.nan_to_num(d[x:x + lengths[i]]), weights=ar[x:x + lengths[i]])
                x = x + lengths[i]
        else:
            x = 0
            for i in range(0, len(lengths)):
                z[i] = np.nansum(d[x:x + lengths[i]])
                x = x + lengths[i]
                
                
                
        outT[:,k+1] = z  #TODO: add to DF here, outDF[col] = z 
    outT = outT[np.in1d(outT[:,0], coms),:]  #TODO: retains appended COMIDs from InterVPU 
                                            # clearer to do with pandas here
    outDF = pd.DataFrame(outT)
    
    
    # below can be moved outside of this function except the nan line below
    if tbl_type == 'Ws':
        outDF.columns = np.append(icol, map(lambda x : x.replace('Cat', 'Ws'),cols.values))
    if tbl_type == 'UpCat':
        outDF.columns = np.append(icol, 'Up' + cols.values)
    for name in outDF.columns:
        if 'AreaSqKm' in name:
            areaName = name
    outDF.loc[(outDF[areaName] == 0), outDF.columns[2:]] = np.nan  # identifies that there is no area in catchment mask, then NA values across the table   
    return outDF


npy = 'D:/NHDPlusV21/StreamCat_npy/bastards'
accum = np.load('%s/accum_06.npz' % npy)
accum = np.load('./accum_npy/accum_18.npz')
accum.files
comids = accum['comids']
lengths = accum['lengths']
upstream = accum['upstream']

print len(comids), len(upstream)

h = 'L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat/Allocation_and_Accumulation'

tbl = pd.read_csv('%s/Clay_18.csv' % h)[['COMID',
                                         'CatAreaSqKm',
                                         'CatCount',
                                         'CatSum',
                                         'CatPctFull']]
tbl.columns.tolist()


check = pd.read_csv('%s/Clay_18.csv' % h)

inputs = np.load('D:/NHDPlusV21/StreamCat_npy/zoneInputs.npy').item()
for zone in inputs:
    t = pd.read_csv('%s/Clay_%s.csv' % (h,zone))
    accum = np.load('%s/accum_%s.npz' % (npy,zone))
    print len(accum['comids']), len(t.COMID.values)
    assert len(accum['comids']) == len(t.COMID.values)
    
   ##############################################
   # TESTING #
   #coming into this func  
   [col.replace('Cat','') for col in tbl.columns]
   
def Accumulation(tbl, comids, lengths, upstream, tbl_type, icol='COMID'):    
    coms = tbl[icol].values  # get array of comids
    indices = swapper(coms, upstream)  # Get indices that will be used to map values
    del upstream
    out = pd.DataFrame(index=comids)
    for col in tbl.columns[1:]:    
        cat = tbl[col].values[indices] # catchment values
        accumulated = np.zeros(comids.shape) # array to calculate th3e accumulated values
        x = 0
        area = tbl.ix[:, 1].values
        ar = area[indices]
        for i in range(len(lengths)):
            length = lengths[i]
            if 'PctFull' in col:
                # using nan_to_num in average function to treat NA's as zeros when summing            
                accumulated[i] = np.ma.average(np.nan_to_num(cat[x:x + length]),
                                               weights=ar[x:x + length])
            else:
                accumulated[i] = np.nansum(cat[x:x + length])
            x = x + length
        out[col] = accumulated       
            
            

outT = outT[np.in1d(outT[:,0], coms),:]  #TODO: retains appended COMIDs from InterVPU 
                                        # clearer to do with pandas here
check2 = check.ix[check.COMID.isin(out.index)]
check2.sort_values('COMID', inplace=True)   
out.sort_index(inplace=True)
check2[['COMID','UpCatAreaSqKm', 'UpCatCount', 'UpCatSum', 'UpCatPctFull']].head(10)
check[['COMID','WsAreaSqKm', 'WsCount', 'WsSum', 'WsPctFull']].tail(10)
out.head(10)
uppy = out.copy()

################################################################################
    ## V2  ##### iterate indices for idxs, and use zip to iterate all rows
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

#C:\Users\Rdebbout\AppData\Local\Continuum\Anaconda2\envs\nusc\lib\site-packages\numpy\ma\extras.py:553: RuntimeWarning: invalid value encountered in double_scalars
#  avg = np.multiply(a, wgt, dtype=result_dtype).sum(axis)/scl
lengths[0]
comids[0]
out.ix[out.CatCount.isnull()]
rmn = add.index.difference(out.index)
no_up = pd.DataFrame({'CatAreaSqKm':[0]*len(rmn),
                      'CatCount':[np.nan]*len(rmn),
                      'CatSum':[np.nan]*len(rmn),
                      'CatPctFull':[np.nan]*len(rmn)},
                        index=rmn)
for i in tet.index:
    final.ix[i] == tet.ix[i]

tbl['CatCount'].values[indices[1000880 : 1004325]].sum()
len(lengths[lengths==0]) # proof that we should remove upstream COMIDs from the npz files!!
tot, zeroes = 0,0 
for zone in inputs:
    print zone
    accum = np.load('%s/accum_%s.npz' % (npy,zone))
    lengths = accum['lengths']
    tot += len(accum['comids'])
    zeroes += len(lengths[lengths==0])
    print len(accum['comids']), len(lengths[lengths==0])
    
count = 0    
for i in y:
    if not i > 0:
        print i, count
    count+=1
        
# comids, lengths, upstream

col_data = tbl[col]

upstream_idxs = indices[x: x + length]
col_data[upstream_idxs]

for q in tbl.columns:
    break
    tbl.loc[(tbl[q] == np.nan)]

outDF.loc[(outDF[areaName] == 0), outDF.columns[2:]]


ps.Se


zeroes / float(tot)

a,b,c = accum['comids'], accum['lengths'], accum['upstream']


type(accum.files[0])

57641 in upstream_idxs
np.where(comids==comid)[0]
57641 in np.concatenate(upstream_idxs, np.where(comids==comid)[0])

np.nonzero(comids == comid)

np.insert(upstream_idxs,0, np.where(comids==comid)[0][0])
np.append(upstream_idxs,np.where(comids==comid)[0][0])
np.append(upstream_idxs,8)

upstream_idxs.shape

type(np.where(comids==comid)[0][0])

a, = np.where(comids==comid)

a, = np.ix_(comids==comid)

np.flatnonzero(comids == comid)[0]

comids.index

for comid, idx, length in zip(comids,range(len(comids)), lengths):
    print idx, comid, length
    break

inter_tbl = pd.read_csv('InterVPU.csv')
interVPUtbl = pd.read_csv('InterVPU.csv')

for zone in INPUTS:
    print zone
    hydroregion = INPUTS[zone]
    pre = '%s/NHDPlus%s/NHDPlus%s' % (NHD_DIR, hydroregion, zone)
        
        
UpStreamComs = UpCOMs        
        
     0:00:00.284000
     
count = 0
f = defaultdict(list)

j = []
for k,v in UpStreamComs.iteritems():
    if len(v) == 0 :
        j.append(k)
        break
        f[k].append([x for x in v if x in all_comids])

flow.ix[flow.TOCOMID == k]  
      
cats = dbf2DF('%s/NHDPlusCatchment/Catchment.dbf' % pre)        
        
keep = tbl.ix[~tbl.COMID.isin(out.index)].set_index('COMID')
        
52943 + 89332       
140835 - 89332      
52289 + 89332
out.ix[out.CatCount.isnull()]


len(tbl.ix[tbl.COMID.isin(comids)].COMID.values)
#filter out keys!!
87892