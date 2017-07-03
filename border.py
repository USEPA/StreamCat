# -*- coding: utf-8 -*-
"""
Created on Tue May 30 14:05:09 2017

Make the PctFull for Point landscape layers. Finds what percentage of zones
exist inside the CONUS border. Isolates polys that cross the border and then
performs area calculations and returns the proportion of each zone.

@author: Rdebbout
"""

import os
import pandas as pd
import geopandas as gpd
from geopandas.tools import sjoin

# trimmed VPU dict
# only holds VPUs on border
inputs = {'10U': 'MS',
          '07' : 'MS',
          '01' : 'NE',
          '17' : 'PN',
          '15' : 'CO',
          '13' : 'RG',
          '12' : 'TX',
          '09' : 'SR',
          '02' : 'MA',
          '08' : 'MS',
          '04' : 'GL',
          '03W' : 'SA',
          '03S' : 'SA',
          '03N' : 'SA',
          '18' : 'CA'}

def dissolveStates(f, nm):
    '''
    Arguments
    ---------
    f        : filename of state shapefile
    nm       : name of the column that identifies state names
    '''
    sts = gpd.read_file(f)
    nin = ['United States Virgin Islands',
            'Commonwealth of the Northern Mariana Islands',
            'Guam',
            'Alaska',
            'American Samoa',
            'Puerto Rico',
            'Hawaii']
    sts = sts.drop(sts.ix[sts[nm].isin(nin)].index)
    sts['dissolve'] = 1
    conus = sts.dissolve(by='dissolve')
    conus = conus[[nm,'geometry']]
    conus.ix[conus.index[0]][nm] = 'CONUS'
    return conus

def brdrPctFull(zns, brdr, ncol, acol='AreaSqKM'):
    '''
    Arguments
    ---------
    zns      : geoDF of basin polygons
    brdr     : geoDF of CONUS polygon
    ncol     : name of the column that uniquely identifies zns polygons    
    acol     : name of column that holds area (sq. KM)
    '''
    # move poly to albers, need to stay in this CRS to cal. area later
    if brdr.crs != zns.crs:
        brdr.to_crs(zns.crs,inplace=True)
    touch = sjoin(zns,brdr,op='within')
    nwin = zns.ix[~zns[ncol].isin(touch[ncol])].copy()
    if len(nwin) == 0:
        return pd.DataFrame()    
    tot = pd.DataFrame()
    for idx, row in nwin.iterrows():
        p = gpd.GeoDataFrame({ncol: [row[ncol]],
                          acol: [row[acol]]},
                          geometry=[row.geometry],
                          crs=nwin.crs)
        clip = gpd.overlay(brdr, p, how='intersection')
        if len(clip) == 0:
            p['CatPctFull'] = 0
            tot = pd.concat([tot,p.set_index(ncol)[['CatPctFull']]])
        else:
            out = clip.dissolve(by=ncol)
            out['Area_CONUS'] = out.geometry.area * 1e-6    
            out['CatPctFull'] = (out['Area_CONUS'] / out[acol]) * 100
            tot = pd.concat([tot,out[['CatPctFull']]])
    assert len(tot) == len(nwin)
    return tot

def makeBrdrPctFile(b_file, z_file, b_field, z_field):
    states = dissolveStates(b_file, b_field)
    if z_file[-4:] == '.shp':
        cats = gpd.read_file(z_file)
        final = brdrPctFull(cats,states,'UID')
    else:
        final = pd.DataFrame()
        for zone in inputs:
            hr = inputs[zone]
            pre = "%s/NHDPlus%s/NHDPlus%s" % (z_file, hr, zone)
            cats = gpd.read_file('%s/NHDPlusCatchment/Catchment.shp'%(pre))
            cats.to_crs({'init':'epsg:5070'},inplace=True)
            out = brdrPctFull(cats,states, z_field)
            final = pd.concat([final,out])
        if final.index.names[0] != 'COMID':
            final.index.names = ['COMID']
    return final
        
if __name__ == '__main__':
    
    us_file = 'L:/Priv/CORFiles/Geospatial_Library/Data/RESOURCE/POLITICAL/BOUNDARIES/NATIONAL/TIGER_2010_State_Boundaries.shp'
#    lake_basins = 'D:/Projects/Frame_NULL/shps/allBasins.shp'
    temp_dir = 'D:/Projects/Frame_NULL/border'
    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)
    nhd = 'D:/NHDPlusV21'
    print 'Making border PctFull csv'
    # LakeCat
    #csv = makeBrdrPctFile(us_file, lake_basins, 'NAME10', 'UID')
    # StreamCat
    csv = makeBrdrPctFile(us_file, nhd, 'NAME10', 'FEATUREID') 
    csv.to_csv('%s/pct_full.csv' % here)