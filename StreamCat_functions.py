# Functions for standardizing landscape rasters, allocating landscape metrics to NHDPlusV2
# catchments, accumulating metrics for upstream catchments, and writing final landscape metric tables
# Authors: Marc Weber<weber.marc@epa.gov>, Ryan Hill<hill.ryan@epa.gov>,
#          Darren Thornbrugh<thornbrugh.darren@epa.gov>, Rick Debbout<debbout.rick@epa.gov>, 
#          and Tad Larsen<laresn.tad@epa.gov>
# Date: October 2015
# ArcGIS 10.2.1, Python 2.7

# load modules
import os
import arcpy
from arcpy.sa import *
arcpy.CheckOutExtension("Spatial")
import pysal as ps
import time, os, sys, string, math
import numpy as np
import numpy.lib.recfunctions as rfn
import pandas as pd
from datetime import datetime as dt
from collections import deque, defaultdict, OrderedDict
import struct, decimal 
import itertools as it
from osgeo import gdal, osr
from gdalconst import *
import rasterio
from rasterio import transform
os.environ['GDAL_DATA'] = 'C:/Users/mweber/AppData/Local/Continuum/Anaconda/pkgs/libgdal-1.11.2-2/Library/data'
from rasterio.warp import calculate_default_transform, reproject, RESAMPLING
import geopandas as gpd
from geopandas.tools import sjoin

#####################################################################################################################
def dbf2DF(dbfile, upper=True): 
    '''
    __author__ = "Ryan Hill <hill.ryan@epa.gov>"    
                 "Marc Weber <weber.marc@epa.gov>"    
    Reads and converts a dbf file to a pandas data frame using pysal.
    
    Arguments
    ---------
    dbfile           : a dbase (.dbf) file
    '''
    db = ps.open(dbfile)
    cols = {col: db.by_col(col) for col in db.header}
    db.close() #Close dbf 
    pandasDF = pd.DataFrame(cols)
    if upper == True:
        pandasDF.columns = map(str.upper, pandasDF.columns)
    return pandasDF
#####################################################################################################################
def UpcomDict(hydroregion, zone, NHD_dir, interVPUtbl): 
    '''
    __author__ = "Ryan Hill <hill.ryan@epa.gov>"    
                 "Marc Weber <weber.marc@epa.gov>"    
    Converts a dbf file to a pandas data frame using pysal.
    
    Arguments
    ---------
    hydroregion           : an NHDPlusV2 hydroregion acronnym, i.e. 'MS', 'PN', 'GB'
    zone                  : an NHDPlusV2 VPU number, i.e. 10, 16, 17
    NHD_dir               : the directory contining NHDPlus data at the top level
    interVPUtbl           : the table that holds the inter-VPU connections to manage connections and anomalies in the NHD
    '''
    #Returns UpCOMs dictionary for accumulation process 
    #Provide either path to from-to tables or completed from-to table
    flowtable= NHD_dir + "/NHDPlus%s/NHDPlus%s/NHDPlusAttributes/PlusFlow.dbf"%(hydroregion, zone)
    flow = dbf2DF(flowtable)
    flow = flow[['TOCOMID','FROMCOMID']]
    flow  = flow[flow.TOCOMID != 0]
    # check to see if out of zone values have FTYPE = 'Coastline'
    flowlines = NHD_dir + "/NHDPlus%s/NHDPlus%s/NHDSnapshot/Hydrography/NHDFlowline.dbf"%(hydroregion, zone) 
    coast = dbf2DF(flowlines, upper=True)
    coasties = coast.COMID[coast.FTYPE == 'Coastline']
    flow = flow[~flow.FROMCOMID.isin(coasties.values)]
    # remove these FROMCOMIDs from the 'flow' table, there are three COMIDs here that won't get filtered out
    remove = np.delete(np.array(interVPUtbl.removeCOMs),np.where(np.array(interVPUtbl.removeCOMs) == 0))
    flow = flow[~flow.FROMCOMID.isin(remove)]
    #find values that are coming from other zones and remove the ones that aren't in the interVPU table
    others = []
    for chk in np.array(flow.FROMCOMID):
        if chk not in np.array(coast.COMID) and chk != 0:
            others.append(chk)
    for y in others:
        if not y in np.array(interVPUtbl.thruCOMIDs):
            flow = flow.drop(flow[flow.FROMCOMID == y].index) 
    # Now table is ready for processing and the UpCOMs dict can be created                  
    fcom = np.array(flow.FROMCOMID)
    tcom = np.array(flow.TOCOMID)    
    UpCOMs = defaultdict(list) 
    for i in range(0,len(flow),1):   
        FROMCOMID = fcom[i]
        TOCOMID = tcom[i]
        if FROMCOMID == 0:
            UpCOMs[TOCOMID] = []
        else:            
            UpCOMs[TOCOMID].append(FROMCOMID)
    for interLine in interVPUtbl.values:
        if interLine[6] > 0 and interLine[2] == zone: 
            UpCOMs[int(interLine[6])].append(int(interLine[0]))
    return UpCOMs
#####################################################################################################################
def children(token, tree, chkset):
    '''
    __author__ = "Ryan Hill <hill.ryan@epa.gov>"    
                 "Marc Weber <weber.marc@epa.gov>"    
    returns a list of every child
    
    Arguments
    ---------
    token           : an NHDPlusV2 hydroregion acronnym, i.e. 'MS', 'PN', 'GB'
    tree            : an NHDPlusV2 VPU number, i.e. 10, 16, 17
	chkset          : set of all the NHD catchment COMIDs used to remove flowlines with no associated catchment
    '''
    visited = set()
    to_crawl = deque([token])
    while to_crawl:
        current = to_crawl.popleft()
        if current in visited:
            continue
        visited.add(current)
        node_children = set(tree[current])
        to_crawl.extendleft(node_children - visited)
    #visited.remove(token)
    visited = visited.intersection(chkset)
    return list(visited)
#####################################################################################################################
def bastards(token, tree, chkset):
    '''
    __author__ = "Ryan Hill <hill.ryan@epa.gov>"    
                 "Marc Weber <weber.marc@epa.gov>"    
    returns a list of every child w/ out father (key) included
    
    Arguments
    ---------
    token           : an NHDPlusV2 hydroregion acronnym, i.e. 'MS', 'PN', 'GB'
    tree            : an NHDPlusV2 VPU number, i.e. 10, 16, 17
	chkset          : set of all the NHD catchment COMIDs used to remove flowlines with no associated catchment
    '''
    visited = set()
    to_crawl = deque([token])
    while to_crawl:
        current = to_crawl.popleft()
        if current in visited:
            continue
        visited.add(current)
        node_children = set(tree[current])
        to_crawl.extendleft(node_children - visited)
    visited.remove(token)
    visited = visited.intersection(chkset)
    return list(visited)
#####################################################################################################################   
def getRasterInfo(FileName):
    '''
    __author__ =   "Marc Weber <weber.marc@epa.gov>"  
                   "Ryan Hill <hill.ryan@epa.gov>" 
    returns basic raster information for a given raster
    
    Arguments
    ---------
    raster          : a raster file 
    '''
    SourceDS = gdal.Open(FileName, GA_ReadOnly)
    NDV = SourceDS.GetRasterBand(1).GetNoDataValue()
    stats = SourceDS.GetRasterBand(1).GetStatistics(True, True)
    xsize = SourceDS.RasterXSize
    ysize = SourceDS.RasterYSize
    GeoT = SourceDS.GetGeoTransform()
    prj = SourceDS.GetProjection()
    Projection = osr.SpatialReference(wkt=prj)
    Proj_projcs = Projection.GetAttrValue('projcs')
#    if Proj_projcs == None:
#        Proj_projcs = 'Not Projected'
    Proj_geogcs = Projection.GetAttrValue('geogcs')
    DataType = SourceDS.GetRasterBand(1).DataType
    DataType = gdal.GetDataTypeName(DataType)
    return (NDV, stats, xsize, ysize, GeoT, Proj_projcs, Proj_geogcs, DataType)
#####################################################################################################################
def Reclass(inras, outras, inval, outval, NDV):
    '''
    __author__ =   "Marc Weber <weber.marc@epa.gov>"  
                   "Ryan Hill <hill.ryan@epa.gov>" 
    reclass a set of values in a raster to another value
    
    Arguments
    ---------
    inras           : an input raster file 
    outras          : an output raster file 
    inval           : value to convert 
    outval          : value to convert to
    NDV             : no data value
    '''
    with rasterio.drivers():
        with rasterio.open(inras) as src:
            kwargs = src.meta
            kwargs.update(
                driver='GTiff',
                count=1,
                compress='lzw',
                nodata=NDV,
                bigtiff='YES' # Output will be larger than 4GB
            )
            
            windows = src.block_windows(1)
    
            with rasterio.open(outras, 'w', **kwargs) as dst:
                for idx, window in windows:
                    src_data = src.read(1, window=window) 
                    # Convert a value
                    src_data = np.where(src_data == inval, outval, src_data)
                    dst_data = src_data
                    dst.write_band(1, dst_data, window=window)
 #####################################################################################################################
def Multiply(inras, outras, val, RastType):
    '''
    __author__ =   "Marc Weber <weber.marc@epa.gov>"  
                   “Ryan Hill<hill.ryan@epa.gov>"
    Multiplies a raster by a given value and returns raster in a specified data type - ideas from https://sgillies.net/page3.html
    
    Arguments
    ---------
    inras           : an input raster file 
    outras          : an output raster file 
    val             : a value to muliply raster by 
    RastType        : the data type of the raster, i.e. 'float32', 'uint8' 
    '''
    with rasterio.drivers():
        with rasterio.open(inras, masked=True) as src:
            kwargs = src.meta.copy()    
            kwargs.update(
                driver='GTiff',
                count=1,
                compress='lzw',
#                dtype=rasterio.uint16
#                nodata=222
            )
            
            windows = src.block_windows(1)
            
            with rasterio.open(outras, 'w', **kwargs) as dst:
                for idx, window in windows:
                    src_data = src.read(1, window=window, masked=True) 
 
                    #Where scr_data eq original nodata make new nodata value. All other data keep same.
                    src_data = np.ma.masked_array(src_data * val, mask=src.meta['nodata']) 
                    dst_data = np.where(src_data == src.meta['nodata'], kwargs['nodata'], src_data).astype(rasterio.float32)
#                    src_data = np.where(src_data == kwargs['nodata'], 0, prG)
#                    dst_data = (src_data * val).astype(rasterio.uint16)
                    # Convert a value
#                    kwargs.update(
#                        dtype=RastType
#                    )
#                    profile = dst.profile
#                    profile.update(
#                        dtype=RastType)
                    dst.write_band(1, dst_data, window=window)
#                    dst.write(dst_data.astype(rasterio.uint8), 1)   
#####################################################################################################################                
def Project(inras, outras, dst_crs, template_raster, nodata):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
                  "Ryan Hill <hill.ryan@epa.gov>"
    reprojects and resamples a raster using rasterio
    
    Arguments
    ---------
    inras           : an input raster with full path name
    outras          : an output raster with full path name
    outproj         : projection to apply to output raster in EPSG format, i.e. EPSG:5070
    resamp          : resampling method to use - either nearest or bilinear
    '''
    with rasterio.open(inras) as src:
        with rasterio.open(template_raster) as tmp:
            affine, width, height = calculate_default_transform(src.crs, dst_crs, src.width, src.height, *tmp.bounds)
            kwargs = src.meta.copy()
            kwargs.update({
                'crs': dst_crs,
                'transform': affine,
                'affine': affine,
                'width': width,
                'height': height,
                'driver': 'GTiff'
            })
            
            with rasterio.open(outras, 'w', **kwargs) as dst:
                reproject(
                    source=rasterio.band(src, 1),
                    destination=rasterio.band(dst, 1),
                    src_transform=src.affine,
                    src_crs=src.crs,
                    src_nodata=nodata,
                    dst_transform=affine,
                    dst_crs=dst_crs,
                    )
#####################################################################################################################
def Resample(inras, outras, resamp_type, resamp_res):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
                  "Ryan Hill <hill.ryan@epa.gov>"
    Resamples a raster using rasterio
    
    Arguments
    ---------
    inras           : an input raster with full path name
    outras          : an output raster with full path name
    resamp_type     : resampling method to use - either nearest or bilinear
    resamp_res      : resolution to apply to output raster
    '''
    with rasterio.open(inras) as src:       
        affine, width, height = calculate_default_transform(src.crs, src.crs, src.width, 
                                                            src.height, *src.bounds, resolution = resamp_res)
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': src.crs,
            'transform': affine,
            'affine': affine,
            'width': width,
            'height': height,
            'driver': 'GTiff'
        })
        with rasterio.open(outras, 'w', **kwargs) as dst:
            if resamp_type=='bilinear':
                reproject(
                    source=rasterio.band(src, 1),
                    destination=rasterio.band(dst, 1),
                    src_transform=src.affine,
                    src_crs=src.crs,
                    dst_transform=src.affine,
                    dst_crs=dst_crs,
                    resampling=RESAMPLING.bilinear,
                    compress='lzw'
                    )
            elif resamp_type=='nearest':
                reproject(
                    source=rasterio.band(src, 1),
                    destination=rasterio.band(dst, 1),
                    src_transform=affine,
                    src_crs=src.crs,
                    dst_transform=affine,
                    dst_crs=src.crs,
                    resampling=RESAMPLING.nearest,
                    compress='lzw'
                    )
#####################################################################################################################
def ProjectResamp(inras, outras, template_raster, resamp_type):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
                  "Ryan Hill <hill.ryan@epa.gov>"
    reprojects a raster using rasterio
	
	
	    Arguments
    ---------
    inras           : an input raster with full path name
    outras          : an output raster with full path name
    outproj         : projection to apply to output raster in EPSG format, i.e. EPSG:5070
    resamp          : resampling method to use - either nearest or bilinear
    '''
    with rasterio.open(inras) as src:
        with rasterio.open(template_raster) as dst:        
            affine, width, height = calculate_default_transform(src.crs, dst.crs, src.width, src.height, *src.bounds)
            kwargs = src.meta.copy()
            kwargs.update({
                'crs': dst.crs,
                'transform': affine,
                'affine': affine,
                'width': width,
                'height': height,
                'driver': 'GTiff'
            })
            with rasterio.open(outras, 'w', **kwargs) as dst:
                if resamp_type=='bilinear':
                    reproject(
                        source=rasterio.band(src, 1),
                        destination=rasterio.band(dst, 1),
                        src_transform=src.affine,
                        src_crs=src.crs,
                        dst_transform=transform.from_origin(affine[2],affine[5],dist.transform[0],dst.transform[0]),
                        dst_crs=dst_crs,
                        resampling=RESAMPLING.bilinear
                        )
                elif resamp_type=='nearest':
                    reproject(
                        source=rasterio.band(src, 1),
                        destination=rasterio.band(dst, 1),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform.from_origin(dst.transform[0],dst.transform[3],dst.transform[1],dst.transform[1]),
                        dst_crs=dst.crs,
                        resampling=RESAMPLING.nearest
                        )    
#####################################################################################################################
def PointInPoly(points,inZoneData, pct_full, summaryfield=None):   
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
                  "Rick Debbout <debbout.rick@epa.gov>"
    Gets the point count of a spatial points feature for every polygon in a spatial polygons feature
    
    Arguments
    ---------
    points        : input points geographic features (any format readable by fiona)
    InZoneData    : input polygons geographic features (any format readable by fiona)
    pct_full      : table that links COMIDs to pct_full, determined from catchments that are  not within the US Census border
    summaryfield  : a list of the field/s in points feature to use for getting summary stats in polygons
    '''   
    polys = gpd.GeoDataFrame.from_file(inZoneData)        
    points = points.to_crs(polys.crs)
    points2 = points.ix[~points.duplicated(['LATITUDE','LONGITUDE'])]
    point_poly_join = sjoin(points2, polys, how="left", op="within")   
    grouped = point_poly_join.groupby('FEATUREID')    
    point_poly_count = grouped[points2.columns[0]].count()
    final = polys.join(point_poly_count,on='FEATUREID', how='left')
    final = final[['FEATUREID','AreaSqKM',points2.columns[0]]].fillna(0)
    cols = ['COMID','AreaSqKm','Count']
    if not summaryfield == None:
        point_poly_dups = sjoin(points, polys, how="left", op="within")   
        grouped2 = point_poly_dups.groupby('FEATUREID')
        for x in summaryfield:
            point_poly_stats = grouped2[x].sum()
            final = final.join(point_poly_stats,on='FEATUREID', how='left').fillna(0)
            cols.append(x)
    final.columns = cols
    final = pd.merge(final,pct_full, on='COMID', how='left')
    final.CatPctFull = final.CatPctFull.fillna(100)
    return final    
#####################################################################################################################
def interVPUfix(Accumulation, accum_type, zone, allocMet_dir, Connector, interVPUtbl):
    '''
    __author__ = "Rick Debbout <debbout.rick@epa.gov>"     
    Loads watershed values for given COMIDs to be appended to catResults table for accumulation.
    
    Arguments
    ---------
    Accumulation          : location of Accumulation file
    accum_type            : type metric to be accumulated, i.e. 'Categorical', 'Continuous', 'Count'
    zone                  : an NHDPlusV2 VPU number, i.e. 10, 16, 17
    allocMet_dir          : string of the directory containing catResults tables including the landscape variable
    Connector             : Location of the connector file
    InterVPUtbl           : table of interVPU exchanges
    '''
    tbl = pd.read_csv(Accumulation).set_index('COMID')
    tbl = tbl[tbl.index.isin(interVPUtbl.thruCOMIDs.values)].copy()    
    throughVPUs = loadInterVPUs(tbl.copy(), accum_type)
    del tbl
    interVPUtbl = interVPUtbl.ix[interVPUtbl.FromZone.values == zone]
    if [x for x in interVPUtbl.toCOMIDs if x > 0]:
           interAlloc = '%s%s.csv'%(allocMet_dir,interVPUtbl.ToZone.values[0].zfill(2)) 
           tbl = pd.read_csv(interAlloc).set_index('COMID')
           toVPUs = tbl[tbl.index.isin([x for x in interVPUtbl.toCOMIDs if x > 0])].copy()        
    for interLine in interVPUtbl.values:
        if interLine[4] > 0:
            AdjustCOMs(toVPUs,int(interLine[4]),int(interLine[0]),accum_type,throughVPUs)           
        if interLine[3] > 0:
            AdjustCOMs(throughVPUs,int(interLine[3]),int(interLine[0]),accum_type,None)
        if interLine[5] > 0:
            throughVPUs = throughVPUs.drop(int(interLine[5]))
    if [x for x in interVPUtbl.toCOMIDs if x > 0]:
        con = pd.read_csv(Connector).set_index('COMID') 
        con.columns = map(str, con.columns)
        toVPUs = toVPUs.append(con)	
        toVPUs.to_csv(Connector)
    if os.path.exists(Connector):
        con = pd.read_csv(Connector).set_index('COMID') 
        con.columns = map(str, con.columns)
        throughVPUs = throughVPUs.append(con)
    throughVPUs.to_csv(Connector)
#####################################################################################################################
def interVPUfix2(tbl, cols, accum_type, zone, Connector, interVPUtbl):
    '''
    __author__ = "Rick Debbout <debbout.rick@epa.gov>"     
    Loads watershed values for given COMIDs to be appended to catResults table for accumulation.
    
    Arguments
    ---------
    Accumulation          : location of Accumulation file
    accum_type            : type metric to be accumulated, i.e. 'Categorical', 'Continuous', 'Count'
    zone                  : an NHDPlusV2 VPU number, i.e. 10, 16, 17
    Connector             : Location of the connector file
    InterVPUtbl           : table of interVPU exchanges
    '''
    throughVPUs = tbl[tbl.COMID.isin(interVPUtbl.thruCOMIDs.values)].set_index('COMID').copy()    
    interVPUtbl = interVPUtbl.ix[interVPUtbl.FromZone.values == zone]
    throughVPUs.columns = cols
    if [x for x in interVPUtbl.toCOMIDs if x > 0]:
           interAlloc = '%s_%s.csv'%(Connector.split('_')[0],interVPUtbl.ToZone.values[0]) 
           tbl = pd.read_csv(interAlloc).set_index('COMID')
           toVPUs = tbl[tbl.index.isin([x for x in interVPUtbl.toCOMIDs if x > 0])].copy()        
    for interLine in interVPUtbl.values:
        if interLine[4] > 0:
            AdjustCOMs(toVPUs,int(interLine[4]),int(interLine[0]),accum_type,throughVPUs)           
        if interLine[3] > 0:
            AdjustCOMs(throughVPUs,int(interLine[3]),int(interLine[0]),accum_type,None)
        if interLine[5] > 0:
            throughVPUs = throughVPUs.drop(int(interLine[5]))
    if [x for x in interVPUtbl.toCOMIDs if x > 0]:
        con = pd.read_csv(Connector).set_index('COMID') 
        con.columns = map(str, con.columns)
        toVPUs = toVPUs.append(con)	
        toVPUs.to_csv(Connector)
    if os.path.exists(Connector):
        con = pd.read_csv(Connector).set_index('COMID') 
        con.columns = map(str, con.columns)
        throughVPUs = throughVPUs.append(con)
    throughVPUs.to_csv(Connector)
#####################################################################################################################
def AdjustCOMs(tbl, comid1, comid2, accum, tbl2 = None):
    '''
    __author__ = "Rick Debbout <debbout.rick@epa.gov>"     
    Adjusts values for COMIDs where values from one need to be subtracted from another
    
    Arguments
    ---------
    tbl                   : table containing watershed values 
    accum_type            : type metric to be accumulated, i.e. 'Categorical', 'Continuous', 'Count'
    zone                  : an NHDPlusV2 VPU number, i.e. 10, 16, 17
    allocMet_dir          : string of the directory containing catResults tables including the landscape variable
    Connector             : Location of the connector file
    InterVPUtbl           : table of interVPU exchanges
    '''
    if tbl2 is None:
        tbl2 = tbl.copy()
    if accum == 'Count':
        tbl.ix[comid1,'CatAreaSqKm'] = tbl.ix[comid1,'CatAreaSqKm'] - tbl2.ix[comid2,'CatAreaSqKm']
        tbl.ix[comid1,'CatCount'] = tbl.ix[comid1,'CatCount'] - tbl2.ix[comid2,'CatCount']
        tbl.ix[comid1,'CatMean'] = tbl.ix[comid1,'CatCount'] / tbl2.ix[comid2,'CatAreaSqKm']
    if accum == 'Continuous':
        for att in ['CatAreaSqKm','CatCount','CatSum']:
            tbl.ix[comid1,att] = tbl.ix[comid1,att]- tbl2.ix[comid2,att]
    if accum == 'Categorical':
        for idx in tbl.columns[:-1]:
            tbl.ix[comid1,idx] = tbl.ix[comid1,idx] - tbl2.ix[comid2,idx]
#####################################################################################################################
def loadInterVPUs(tbl, accum):
    '''
    __author__ = "Rick Debbout <debbout.rick@epa.gov>"     
    Uses the 'Cat' and 'UpCat' columns to caluculate watershed values and returns those values in 'Cat' columns 
	so they can be appended to 'CatResult' tables in other zones before accumulation.
    
    Arguments
    ---------
    tbl                   : table containing watershed values 
    accum                 : type metric to be accumulated, i.e. 'Categorical', 'Continuous', 'Count'
    '''
    if accum == 'Continuous':     
        tbl['WsArea'] = tbl.CatAreaSqKm + tbl.UpCatAreaSqKm
        #tbl['WsMean'] = ((tbl.CatSum + tbl.UpCatSum)/(tbl.CatCount + tbl.UpCatCount)).fillna(0)
        tbl['WsPctFull'] = ((((tbl.CatAreaSqKm * (tbl.CatPctFull/100)) + (tbl.UpCatAreaSqKm * (tbl.UpCatPctFull/100)))/ (tbl.CatAreaSqKm + tbl.UpCatAreaSqKm))*100)
        tbl['WsCount'] = tbl.CatCount + tbl.UpCatCount
        tbl['WsSum'] = tbl.CatSum + tbl.UpCatSum      
        result = tbl.iloc[:,[8,9,10,11]]
        result.columns = ['CatAreaSqKm','CatPctFull','CatCount','CatSum']
        return result
    if accum == 'Categorical':
        catch_full_index = (len(tbl.columns))/2
        result = tbl.copy()
        names = tbl.columns[:catch_full_index]
        result['WsArea'] = result.CatAreaSqKm + result.UpCatAreaSqKm
        for i in range(1, catch_full_index-1):
            result.insert((len(result.columns)),tbl.columns[i] + 'Ws', tbl.ix[:,i] + tbl.ix[:,(catch_full_index+i)])
        result = result.ix[:,len(tbl.columns):]
        result.insert(len(result.columns),'WsPctFull', np.round((((result.ix[:,1:].sum(axis=1)*1e-06)/result.WsArea)*100)), 2)
        result.columns = names
        return result
    if accum == 'Count':
        tbl['WsArea'] = tbl.CatAreaSqKm + tbl.UpCatAreaSqKm
        tbl['WsCount'] = tbl.CatCount + tbl.UpCatCount
        tbl['WsMean'] = tbl['WsCount']/tbl['WsArea']
        tbl['WsPctFull'] = (((tbl.CatAreaSqKm * tbl.CatPctFull) + (tbl.UpCatAreaSqKm * tbl.UpCatPctFull))/tbl.WsArea)
        result = tbl.ix[:,[8,9,10,11]]
        result.columns = ['CatAreaSqKm','CatCount','CatMean','CatPctFull']
        return result
#####################################################################################################################	
def Accumulation(arr, COMIDs, lengths, upStream, tbl_type):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
                  "Ryan Hill <hill.ryan@epa.gov>"
    Uses the 'Cat' and 'UpCat' columns to caluculate watershed values and returns those values in 'Cat' columns 
	so they can be appended to 'CatResult' tables in other zones before accumulation.
    
    Arguments
    ---------
    arr                   : table containing watershed values 
    COMIDs                : numpy array of all zones COMIDs 
    lengths               : numpy array with lengths of upstream COMIDs
    upstream                : numpy array of all upstream arrays for each COMID
    tbl_type              : type metric to be accumulated, i.e. 'Categorical', 'Continuous', 'Count'
    '''
    coms = np.array(arr.COMID) #Read in COMIDs    
    indices = swapper2(coms, upStream) #Get indices that will be used to map values
    del upStream # a and indices are big - clean up to minimize RAM
    cols = arr.columns[1:] #Get column names that will be accumulated
    z = np.zeros(COMIDs.shape) #Make empty vector for placing values
    outT = np.zeros((len(COMIDs), len(arr.columns))) #Make empty array for placing final values
    outT[:,0] = COMIDs #Define first column as comids
    #Loop and accumulate values
    for k in range(0,len(cols)):  
        col = cols[k]    
        c = np.array(arr[col].fillna(0)) 
        d = c[indices] #Make final vector from desired data (c)        
        if col == 'CatPctFull':
            area = np.array(arr.CatAreaSqKm) 
            ar = area[indices]
            x = 0
            for i in range(0, len(lengths)):
                z[i] = np.ma.average(d[x:x+lengths[i]], weights=ar[x:x+lengths[i]])
                x = x+lengths[i]
        else:            
            x = 0
            for i in range(0, len(lengths)):
                z[i] = np.nansum(d[x:x+lengths[i]])    
                x = x+lengths[i]                  
        outT[:,k+1] = np.nan_to_num(z)
    outT = outT[np.in1d(outT[:,0], coms),:] #Remove the extra COMIDs
    outDF = pd.DataFrame(outT)
    if tbl_type == 'Ws':
        outDF.columns = np.append('COMID', map(lambda x : x.replace('Cat','Ws'),cols.values))
    if tbl_type == 'UpCat':
        outDF.columns = np.append('COMID', 'Up'+cols.values)
    return outDF			
#####################################################################################################################
def createCatStats(accum_type, outTable, ingrid, inZoneData):
    startTime = dt.now()
    arcpySettings()
    arcpy.env.snapRaster = inZoneData        	
    if not os.path.exists(outTable):
        if accum_type == 'Categorical':
            arcpy.gp.TabulateArea_sa(inZoneData, 'VALUE', ingrid, "Value", outTable, "30")
        if accum_type == 'Continuous':
            arcpy.gp.ZonalStatisticsAsTable_sa(inZoneData, 'VALUE', ingrid, outTable, "DATA", "ALL")
#    if by_RPU == 'True':
#        hydrodir = '/'.join(inZoneData.split('/')[:-2])
#        for subdirs in os.listdir(hydrodir):
#            if subdirs.count("FdrFac") and not subdirs.count('.txt') and not subdirs.count('.7z'):
#                fdr = "%s/%s/fdr"%(hydrodir, subdirs)
#                arcpy.env.snapRaster = fdr
#                arcpy.env.resamplingMethod = "NEAREST"
#                if len(mask_layer) > 1:	
#                    arcpy.env.mask = mask_layer + '_' + subdirs[-3:] + '.tif'
#                if accum_type == 'Categorical':
#                    arcpy.gp.TabulateArea_sa(inZoneData, 'VALUE', ingrid, "Value", outTable, "30")
#                if accum_type == 'Continuous':
#                    arcpy.gp.ZonalStatisticsAsTable_sa(inZoneData, 'VALUE', ingrid, outTable, "DATA", "ALL")
    table = dbf2DF(outTable)
    if accum_type == 'Continuous':
        table = table[['VALUE','AREA','COUNT','SUM']]
        table = table.rename(columns = {'COUNT':'Count','SUM':'Sum'})
    if accum_type == 'Categorical':
        # Get ALL categorical values from the dbf associated with the raster to retain all values
        # in the raster in every table, even when a given value doesn't exist in a given hydroregion
        AllCols = dbf2DF(ingrid + '.vat.dbf').VALUE.tolist()
        col_list = table.columns.tolist()[1:]
        if len(AllCols) != len(col_list):
            AllCols = ['VALUE_'+str(x) for x in AllCols]
            diff = list(set(AllCols) - set(col_list))
            diff.sort()
            for spot in diff:
                here = AllCols.index(spot) + 1
                table.insert(here,spot,0)
        table['AREA'] = table[col_list].sum(axis=1)
    #nhdTable = dbf2DF(inZoneData[:-3] + 'Catchment.dbf').ix[:,[1,0,2]]
    nhdTable = dbf2DF(inZoneData[:-3] + 'Catchment.dbf').ix[:,['FEATUREID','AREASQKM','GRIDCODE']]
    nhdTable = nhdTable.rename(columns = {'FEATUREID':'COMID','AREASQKM':'AreaSqKm'})
    result = pd.merge(nhdTable, table, how='left', left_on='GRIDCODE', right_on='VALUE')
    result['PctFull'] = (((result.AREA * 1e-6)/result.AreaSqKm)*100)
    result = result.drop(['GRIDCODE','VALUE','AREA'], axis=1).fillna(0)
    cols = result.columns[1:]
    result.columns = np.append('COMID', 'Cat' + cols.values) 
    print "elapsed time " + str(dt.now()-startTime)
    return result
#####################################################################################################################    
def arcpySettings():
    arcpy.CheckOutExtension("spatial")
    arcpy.OverWriteOutput = 1
    arcpy.env.outputCoordinateSystem = "PROJCS['NAD_1983_Albers',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-96.0],PARAMETER['Standard_Parallel_1',29.5],PARAMETER['Standard_Parallel_2',45.5],PARAMETER['Latitude_Of_Origin',23.0],UNIT['Meter',1.0]]"
    arcpy.env.pyramid = "NONE"
    arcpy.env.cellSize = "30"
    arcpy.env.resamplingMethod = "NEAREST"
#####################################################################################################################    
def makeOutTable(ingrid, out_dir, zone):
    if ingrid.count('.tif') or ingrid.count('.img'):
        outTable ="%s/zonalstats_%s%s.dbf"%(out_dir,ingrid.split("/")[-1].split(".")[0],zone)
    else:
        outTable ="%s/zonalstats_%s%s.dbf"%(out_dir,ingrid.split("/")[-1],zone)
    return outTable 
#####################################################################################################################
def appendConnectors(cat, Connector, zone, interVPUtbl):    
    con = pd.read_csv(Connector)
    for comidx in con.COMID.values.astype(int):
        if comidx in cat.COMID.values.astype(int):
            cat = cat.drop(cat[cat.COMID == comidx].index)
    con = con.ix[con.COMID.isin(np.append(interVPUtbl.ix[interVPUtbl.ToZone.values == zone].thruCOMIDs.values,interVPUtbl.ix[interVPUtbl.ToZone.values == zone].toCOMIDs.values[np.nonzero(interVPUtbl.ix[interVPUtbl.ToZone.values == zone].toCOMIDs.values)]))]
    #con = con.ix[con.COMID.isin(np.append(np.array(interVPUtbl.ix[np.array(interVPUtbl.ToZone) == zone].thruCOMIDs),np.array(interVPUtbl.ix[np.array(interVPUtbl.ToZone) == zone].toCOMIDs)[np.nonzero(np.array(interVPUtbl.ix[np.array(interVPUtbl.ToZone) == zone].toCOMIDs))]))]             
    cat = cat.append(con)
    return cat
#####################################################################################################################   
def createAccumTable(table, directory, zone, tbl_type):
    if tbl_type == 'UpCat':
        directory = directory + '/bastards'
    if tbl_type == 'Ws':
        directory = directory + '/children'
    COMIDs = np.load(directory + '/comids' + zone + '.npy')
    lengths= np.load(directory + '/lengths' + zone + '.npy')
    upStream = np.load(directory + '/upStream' + zone + '.npy')
    add = Accumulation(table, COMIDs, lengths, upStream, tbl_type)
    return add
#####################################################################################################################
def swapper2(coms, upStream):
    #Run numpy query to replace COMID raster with desired values:
    bsort = np.argsort(coms) 
    apos = np.searchsorted(coms[bsort], upStream) 
    indices = bsort[apos] 
    return indices
