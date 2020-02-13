# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 15:29:55 2017

@author: RHill04
"""

import geopandas as gpd
import pandas as pd
import numpy as np
import time
import os
import arcpy
from shapely.ops import unary_union
sys.path.append('F:/GitProjects/StreamCat')
from StreamCat_functions import dbf2DF
import matplotlib.pyplot as plt

def watershed_onnet(focal_com, uids, lengths, up, basins_shp, agg_ws_shp):        
    try: 
        #focal_uid = uids_trns[np.in1d(comids_trns, focal_com)]
        focal_uid = focal_com
        l = np.asscalar(lengths[np.in1d(uids, focal_uid)])
        start = np.sum(lengths[:np.asscalar(np.where(np.in1d(uids, focal_uid))[0])])
        uplist = up[start:start+l]
    except: 
        uplist = [focal_uid]
    tmp_basin = basins_shp.loc[basins_shp['FEATUREID'].isin(uplist)] 
    tmp_basin.is_copy = False
    tmp_basin['COMID'] = int(focal_com)
    tmp_basin = tmp_basin[['geometry', 'COMID']]
    tmp_intervpu = agg_ws_shp.loc[agg_ws_shp['COMID'].isin(uplist)]
    tmp_intervpu.is_copy = False
    tmp_intervpu['COMID'] = int(focal_com)
    tmp_basin = tmp_basin.append(tmp_intervpu)
    #tmp_basin['geometry'] = tmp_basin.buffer(0)
#    start_time2 = time.time()
#    tmp_basin = tmp_basin.dissolve(by='COMID')
#    print("--- %s seconds ---" % (time.time() - start_time2))  
    return tmp_basin


def WBCOMID_COMID(nhd_dir, hydro, vpu, nhdcats):    
    pre = "%s/NHDPlus%s/NHDPlus%s" % (nhd_dir, hydro, vpu)
    fl = dbf2DF("%s/NHDSnapshot/Hydrography/NHDFlowline.dbf"%(pre))[['COMID', 'WBAREACOMI']]
    cat = gpd.read_file('%s/NHDPlusCatchment/Catchment.shp'%(pre)).drop(['GRIDCODE', 'SOURCEFC'], axis=1)
    cat.columns = cat.columns[:-1].str.upper().tolist() + ['geometry']                         
    vaa = dbf2DF('%s/NHDPlusAttributes/PlusFlowlineVAA.dbf'%(pre))[['COMID','HYDROSEQ']]
    # merge 
    df = pd.merge(cat.drop('geometry', axis=1),fl,left_on='FEATUREID',
                       right_on='COMID',how='inner')
    df = df[df['WBAREACOMI']!=0]
    df = pd.merge(df,vaa, on='COMID',how='left')
    df = df.loc[df.groupby('WBAREACOMI')['HYDROSEQ'].idxmin()]
    # initialize containers for on-net lakes                   
    df = df[['WBAREACOMI','COMID']]
    df.columns = ['WBAREACOMID','COMID']
    return df          
                
#Define directories
nhd_dir = 'H:/NHDPlusV21/'
np_dir = 'L:/Priv/CORFiles/Geospatial_Library_Projects/StreamCat/numpy_files/children/'
#Read in  COMIDs of interest of select certain COMIDs 
#lakes_df = pd.read_csv(nla17_dir + '/NLA17_LandscapeData.csv')
#lakes = np.array(lakes_df.COMID).astype(int)
coms = np.array(19268286).astype(int)


#Read in on-network numpy files
tmp_np = np.load(ws_dir + 'onNetFramework.npz')
on_uids = tmp_np['uids']
#on_lengths = tmp_np['lengths']
#on_up = tmp_np['upstream']
on_vpus = tmp_np['vpus']
on_uids_trns = tmp_np['on_uids_trns']
on_comids_trns = tmp_np['on_comids_trns']
vectunit = tmp_np['vectunit']
hydreg = tmp_np['hydreg']
del tmp_np
#Create on-net lake watersheds
onnet = lakes[np.in1d(lakes, on_comids_trns)]
onuids = on_uids_trns[np.in1d(on_comids_trns, onnet)]
onuids = on_uids[np.in1d(on_uids, onuids)]
vpus = on_vpus[np.in1d(on_uids, onuids)]

vpu_list = np.unique(vpus)
intervpu = gpd.read_file(ws_dir + 'interVPUs.shp')

i=0
for vpu in vpu_list:
    print vpu
    hydro = hydreg[np.in1d(vectunit,vpu)]
    onuids_vpu = onuids[np.in1d(vpus, vpu)]
#    onnet_vpu = onnet[np.in1d(vpus, vpu)]
    nhdcats = gpd.read_file(nhd_dir + '/NHDPlus' + hydro[0] + '/NHDPlus' + vpu + '/NHDPlusCatchment/Catchment.shp')
    tmp_np = np.load(np_dir + 'accum_' + vpu + '.npz')
    #nhdcats['dummy'] = 1 
    for lake in onuids_vpu:
        print lake
        start_time2 = time.time()
        if i==0:
            out_ws = watershed_onnet(lake, tmp_np['comids'], tmp_np['lengths'], tmp_np['upstream'], nhdcats, intervpu)
            out_ws = out_ws.to_crs(epsg=5070)
            out_ws['geometry'] = out_ws.buffer(0.01)
            out_ws = out_ws.dissolve(by='COMID')
            out_ws['COMID'] = out_ws.index
#            out_ws['SITE_ID'] = lakes_df.loc[lakes_df.index[i],'SITE_ID']
        else:
            temp_ws = watershed_onnet(lake, tmp_np['comids'], tmp_np['lengths'], tmp_np['upstream'], nhdcats, intervpu)
            temp_ws = temp_ws.to_crs(epsg=5070)
            temp_ws['COMID'] = lake
            temp_ws['geometry'] = temp_ws.buffer(0.01)
            temp_ws = temp_ws.dissolve(by='COMID')
            temp_ws['COMID'] = temp_ws.index
#            temp_ws['SITE_ID'] = lakes_df.loc[lakes_df.index[i],'SITE_ID']
            out_ws = out_ws.append(temp_ws, ignore_index=True)
        i+=1
        print("--- %s seconds ---" % (time.time() - start_time2)) 
        #world.to_file(filename=temp_shp,driver='ESRI Shapefile',crs_wkt=prj)
        print '------------------------------------'

tmp = int(on_comids_trns[np.in1d(on_uids_trns, comid)])        
out_ws['WBCOMID'] = on_comids_trns[np.in1d(on_uids_trns, out_ws['COMID'])]
for vals in out_ws['COMID']:
    tmp = int(on_comids_trns[np.in1d(on_uids_trns, vals)])
    out_ws.loc[(out_ws['COMID']==vals),'WBCOMID'] = tmp
out_ws[['WBCOMID']] = out_ws[['WBCOMID']].astype(int)
out_ws[['COMID']] = out_ws[['COMID']].astype(int)
out_ws.columns = ['CAT_COMID','geometry', 'COMID']
out_ws = out_ws[['COMID','CAT_COMID','geometry']]
# Add SITE_ID
lakes_df = lakes_df[['COMID','SITE_ID']]
out_ws = out_ws.merge(lakes_df, how='left')
out_ws = out_ws[['SITE_ID','COMID','CAT_COMID','geometry']]
out_ws.to_file('L:/Priv/CORFiles/Geospatial_Library_Projects/NLA/NLA2017LandscapeMetrics/On_Network_Lakes/OnNetLakes.shp', driver = 'ESRI Shapefile')

#Off-Net Lakes
#Read in off-network numpy files
off_np = np.load('L:/Priv/CORFiles/Geospatial_Library_Projects/LakeCat/Watersheds_Framework/offNetFramework.npz')
#Create off-net lake watersheds
offnet = lakes[np.in1d(lakes, off_np['off_comids_trns'])]
basins = gpd.read_file(ws_dir + '/allBasins.shp')
i=0
for lake in offnet:
    print lake
    if i==0:
        out_ws = watershed_offnet(lake, off_np['uids'], off_np['lengths'], off_np['upstream'], off_np['off_uids_trns'], 
                          off_np['off_comids_trns'], basins)
        out_ws = out_ws.to_crs(epsg=5070)
        out_ws['geometry'] = out_ws.buffer(0.01)
        out_ws = out_ws.dissolve(by='COMID')
        out_ws['COMID'] = out_ws.index
#            out_ws['SITE_ID'] = lakes_df.loc[lakes_df.index[i],'SITE_ID']
    else:
        temp_ws = watershed_offnet(lake, off_np['uids'], off_np['lengths'], off_np['upstream'], off_np['off_uids_trns'], 
                          off_np['off_comids_trns'], basins)
        temp_ws = temp_ws.to_crs(epsg=5070)
        temp_ws['COMID'] = lake
        temp_ws['geometry'] = temp_ws.buffer(0.01)
        temp_ws = temp_ws.dissolve(by='COMID')
        temp_ws['COMID'] = temp_ws.index
#            temp_ws['SITE_ID'] = lakes_df.loc[lakes_df.index[i],'SITE_ID']
        out_ws = out_ws.append(temp_ws, ignore_index=True)
    i+=1
    print("--- %s seconds ---" % (time.time() - start_time2)) 
    #world.to_file(filename=temp_shp,driver='ESRI Shapefile',crs_wkt=prj)
    print '------------------------------------'

out_ws = out_ws[['COMID','geometry']]  
out_ws = out_ws.merge(lakes_df, how='left') 
out_ws = out_ws[['SITE_ID','COMID','geometry']]     
out_ws.to_file('L:/Priv/CORFiles/Geospatial_Library_Projects/NLA/NLA2017LandscapeMetrics/Off_Network_Lakes/Off_Network_InNHD_NLA17Lakes.shp', driver = 'ESRI Shapefile')
off_net_inNHD = out_ws
# Combind on and off network lakes with standard columns
off_net = gpd.read_file('L:/Priv/CORFiles/Geospatial_Library_Projects/NLA/NLA2017LandscapeMetrics/Off_Network_Lakes/Off_Network_NLA17Lakes.shp')
on_net = gpd.read_file('L:/Priv/CORFiles/Geospatial_Library_Projects/NLA/NLA2017LandscapeMetrics/On_Network_Lakes/OnNetLakes.shp')

off_net = off_net[['comid','geometry']]
off_net.columns = ['COMID','geometry']
off_net = off_net.merge(lakes_df, how='left')
off_net[['COMID']] = off_net[['COMID']].astype(int)
off_net = off_net[['SITE_ID','COMID','geometry']]
on_net = on_net[['SITE_ID','COMID','geometry']]

lake_wats = on_net.append(off_net, ignore_index=True)

# and then the InNHD off-network
lake_wats = lake_wats.append(off_net_inNHD, ignore_index=True)
lake_wats.to_file('L:/Priv/CORFiles/Geospatial_Library_Projects/NLA/NLA2017LandscapeMetrics/NLA17_Watersheds.shp', driver = 'ESRI Shapefile')
