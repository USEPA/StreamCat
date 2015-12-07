# Functions for standardizing landscape rasters, allocating landscape metrics to NHDPlusV2
# catchments, accumulating metrics for upstream catchments, and writing final landscape metric tables
# Authors: Marc Weber<weber.marc@epa.gov>, Ryan Hill<hill.ryan@epa.gov>,
#          Darren Thornbrugh<thornbrugh.darren@epa.gov>, Rick Debbout<debbout.rick@epa.gov>, 
#          and Tad Larsen<laresn.tad@epa.gov>
# Date: October 2015
# Python 2.7

# load modules
import os
import pysal as ps
import time, os, sys, string, math, arcpy
import numpy as np
import numpy.lib.recfunctions as rfn
import pandas as pd
from datetime import datetime
from collections import deque, defaultdict, OrderedDict
import struct, decimal 
import itertools as it
from osgeo import gdal, osr
from gdalconst import *
from arcpy.sa import *
import rasterio
from rasterio import transform
os.environ['GDAL_DATA'] = 'C:/Users/mweber/AppData/Local/Continuum/Anaconda/pkgs/libgdal-1.11.2-2/Library/data'
from rasterio.warp import calculate_default_transform, reproject, RESAMPLING
import geopandas as gpd
from geopandas import GeoDataFrame, read_file
from geopandas.tools import sjoin
arcpy.CheckOutExtension("Spatial")

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
    return (NDV, xsize, ysize, GeoT, Proj_projcs, Proj_geogcs, DataType)
#####################################################################################################################
def Reclass(inras, outras, inval, outval, comparison, NDV):
    '''
    __author__ =   "Marc Weber <weber.marc@epa.gov>"  
                   "Ryan Hill <hill.ryan@epa.gov>" 
    returns basic raster information for a given raster
    
    Arguments
    ---------
    raster          : a raster file 
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
                    if comparison == 'eq':
                        src_data = np.where(src_data == inval, outval, src_data)
                    if comparison == 'lt':
                        src_data = np.where(src_data < inval, outval, src_data)
                    if comparison == 'gt':
                        src_data = np.where(src_data > inval, outval, src_data)
                    if comparison == 'lte':
                        src_data = np.where(src_data <= inval, outval, src_data)
                    if comparison == 'gte':
                        src_data = np.where(src_data >= inval, outval, src_data)
                    dst_data = src_data
                    dst.write_band(1, dst_data, window=window)
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
def Resample(inras, outras, template_raster, resamp_type):
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
        with rasterio.open(template_raster) as dst:        
            affine, width, height = calculate_default_transform(dst.crs, dst.crs, dst.width, 
                                                                dst.height, *dst.bounds)
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
                        dst_transform=src.affine,
                        dst_crs=dst_crs,
                        resampling=RESAMPLING.bilinear,
                        dtype='uint16',
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
                        dtype='uint16',
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
def PointInPolyCount(points, polys, groupingID, countfield=None, summaryfield=None):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
    Gets the point count of a spatial points feature for every polyton in a spatial polygons feature
    
    Arguments
    ---------
    InPoints         : input points geographic features (any format readable by fiona)
    InPolys          : input polygons geographic features (any format readable by fiona)
    groupingID       : attribute field in polygon features used for grouping of point results
    countfield       : field in points feature to use for getting count in polygons
    summaryfield     : field in points feature to use for getting summary stats in polygons
    '''
    try:
        gp_points  = gpd.GeoDataFrame.from_file(points) 
        gp_polys = gpd.GeoDataFrame.from_file(polys)
        if gp_points.crs==gp_polys.crs:
            if countfield!=None:
                point_poly_join = sjoin(gp_points, gp_polys, how="right", op="within")
                grouped = point_poly_join.groupby(groupingID)
                point_poly_count = grouped[countfield].count()
                return point_poly_count
    except:
        print 'features are not in same crs!'     
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
def continAccum(arr, COMIDs, lengths, upStream, tbl_type):
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
	COMIDs                : numpy array of all upstream arrays for each COMID
	tbl_type              : type metric to be accumulated, i.e. 'Categorical', 'Continuous', 'Count'
    '''
    coms = np.array(arr.COMID) #Read in COMIDs  !Remove key COMID from upstream b4 passing to swapper if tbl-type =='UpCat'
    #if tbl_type == 'UpCat':
    
    jcoms = np.setdiff1d(COMIDs,coms) #Get a list of the junk coms 
    indices = swapper(coms, upStream,jcoms) #Get indices that will be used to map values
    del upStream # a and indices are big - clean up to minimize RAM
    cols = arr.columns[1:] #Get column names that will be accumulated
    z = np.zeros(COMIDs.shape) #Make empty vector for placing values
    outT = np.zeros((len(COMIDs), len(arr.columns))) #Make empty array for placing final values
    outT[:,0] = COMIDs #Define first column as comids
    #Loop and accumulate values
    for k in range(0,len(cols)):  
        col = cols[k]    
        c = np.array(arr[col].fillna(0)) 
        c = np.append(c, np.zeros(jcoms.size))
        d = c[indices] #Make final vector from desired data (c)        
        if col == 'CatPctFull':
            area = np.array(arr.CatAreaSqKm) 
            area = np.append(area, np.zeros(jcoms.size))
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