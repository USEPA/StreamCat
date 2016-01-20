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
import numpy as np
import pandas as pd
from datetime import datetime as dt
from collections import deque, defaultdict
from osgeo import gdal, osr
from gdalconst import *
import rasterio
from rasterio import transform
os.environ['GDAL_DATA'] = 'C:/Users/mweber/AppData/Local/Continuum/Anaconda/pkgs/libgdal-1.11.2-2/Library/data'
from rasterio.warp import calculate_default_transform, reproject, RESAMPLING
import geopandas as gpd
from geopandas.tools import sjoin
import fiona

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
def Reclass(inras, outras, inval, outval, out_dtype=None):
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
    out_dtype       : the data type of the raster, i.e. 'float32', 'uint8' (string) 
    '''
    with rasterio.drivers():
        with rasterio.open(inras) as src:
            #Set dtype and nodata values
            if out_dtype is None: #If no dtype defined, use input dtype                
                nd = src.meta['nodata']
                dt = src.meta['dtype']
            else:
                try:
                    nd = eval('np.iinfo(np.'+out_dtype+').max')
                except:
                    nd = eval('np.finfo(np.'+out_dtype+').max')  
                #exec 'nd = np.iinfo(np.'+out_dtype+').max' 
                dt = out_dtype 
            kwargs = src.meta.copy()
            kwargs.update(
                driver='GTiff',
                count=1,
                compress='lzw',
                nodata=nd,
                bigtiff='YES' # Output will be larger than 4GB
            )
            
            windows = src.block_windows(1)
    
            with rasterio.open(outras, 'w', **kwargs) as dst:
                for idx, window in windows:
                    src_data = src.read(1, window=window) 
                    # Convert a value
                    if np.isnan(outval).any():
                        src_data = np.where(src_data != inval, src_data, kwargs['nodata']).astype(dt)
                    else:
                        src_data = np.where(src_data == inval, outval, src_data).astype(dt)
#                    src_data = np.where(src_data == inval, outval, src_data)
                    dst_data = src_data
                    dst.write_band(1, dst_data, window=window)
#####################################################################################################################
def rasterMath(inras, outras, expression=None, out_dtype=None):     
    '''
    __author__ =   "Marc Weber <weber.marc@epa.gov>"  
                   "Ryan Hill<hill.ryan@epa.gov>"
    Applies arithmetic operation to a raster by a given value and returns raster in a specified data type - 
    ideas from https://sgillies.net/page3.html
    
    Arguments
    ---------
    inras           : an input raster file (string)
    outras          : an output raster file (string)    
    expression      : string of mathematical expression to be used that includes the input raster
                      as variable. If no expression provided, raster is copied. Function can be 
                      used to change dtype of original raster.
                      Example: 
                      inras = 'C:/some_locat_raster.tif'
                      expression = 'log(' + inras + '+1)' or inras + ' * 100'   
    out_dtype       : the data type of the raster, i.e. 'float32', 'uint8' (string)    
    '''
    
    expression = expression.replace(inras, 'src_data')    
         
    with rasterio.drivers():
        with rasterio.open(inras) as src:
            #Set dtype and nodata values
            if out_dtype is None: #If no dtype defined, use input dtype                
                nd = src.meta['nodata']
                dt = src.meta['dtype']
            else:
                try:
                    nd = eval('np.iinfo(np.'+out_dtype+').max')
                except:
                    nd = eval('np.finfo(np.'+out_dtype+').max')  
                #exec 'nd = np.iinfo(np.'+out_dtype+').max' 
                dt = out_dtype        
            kwargs = src.meta.copy()    
            kwargs.update(
                driver='GTiff',
                count=1,
                compress='lzw',
                dtype = dt,
                nodata = nd
            )
            
            windows = src.block_windows(1)
            
            with rasterio.open(outras, 'w', **kwargs) as dst:
                for idx, window in windows:
                    src_data = src.read(1, window=window) 
                        #Where src not eq to orig nodata, multiply by val, else set to new nodata. Set dtype
                    if expression == None:
                            #No expression produces copy of original raster (can use new data type)
                        dst_data = np.where(src_data != src.meta['nodata'], src_data, kwargs['nodata']).astype(dt)
                    else:
                        dst_data = np.where(src_data != src.meta['nodata'], eval(expression), kwargs['nodata']).astype(dt)
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
def ShapefileProject(InShp, OutShp, CRS):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
    reprojects a shapefile with Fiona
    
    Arguments
    ---------
    InShp           : an input shapefile as a string, i.e. 'C:/Temp/outshape.shp'
    OutShp          : an output shapefile as a string, i.e. 'C:/Temp/outshape.shp'
    CRS             : the output CRS in Fiona format
    '''
    # Open a file for reading   
    with fiona.open(InShp, 'r') as source:
        sink_schema = source.schema.copy()
        sink_schema['geometry'] = 'Point'
    
        # Open an output file, using the same format driver and passing the desired
        # coordinate reference system
        with fiona.open(
                OutShp, 'w',
                crs=CRS, driver=source.driver, schema=sink_schema,
                ) as sink:
                    for f in source:
                        # Write the record out.
                        sink.write(f)
    
        # The sink's contents are flushed to disk and the file is closed
        # when its ``with`` block ends. This effectively executes
        # ``sink.flush(); sink.close()``.
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
def ProjectResamp(inras, outras, out_proj, resamp_type, out_res):
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
    with rasterio.drivers():
        with rasterio.open(inras) as src:        
            affine, width, height = calculate_default_transform(src.crs, out_proj, src.width, src.height, *src.bounds)
            kwargs = src.meta.copy()
            kwargs.update({
                'crs': out_proj,
                'transform': affine,
                'affine': affine,
                'width': width,
                'height': height,
                'driver': 'GTiff'
            })
            
            windows = src.block_windows(1)
            
            with rasterio.open(outras, 'w', **kwargs) as dst:
                for idx, window in windows:
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
def PointInPoly(points, inZoneData, pct_full, summaryfield=None):   
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
                  "Rick Debbout <debbout.rick@epa.gov>"
    Returns either the count of spatial points feature in every polygon in a spatial polygons feature or the summary of 
    an attribute field for all the points in every polygon of a spatial polygons feature
    
    Arguments
    ---------
    points        : input points geographic features as a GeoPandas GeoDataFrame
    InZoneData    : input polygon shapefile as a string, i.e. 'C:/Temp/outshape.shp'
    pct_full      : table that links COMIDs to pct_full, determined from catchments that are  not within the US Census border
    summaryfield  : a list of the field/s in points feature to use for getting summary stats in polygons
    '''  
    #startTime = dt.now()
    polys = gpd.GeoDataFrame.from_file(inZoneData)     
    points = points.to_crs(polys.crs)
    # Get list of lat/long fields in the table
    latlon = [s for s in points.columns if any(xs in s.upper() for xs in ['LONGIT','LATIT'])]
    # Remove duplicate points for 'Count' 
    points2 = points.ix[~points.duplicated(latlon)]
    point_poly_join = sjoin(points2, polys, how="left", op="within")
    # Create group of all points in catchment
    grouped = point_poly_join.groupby('FEATUREID')    
    point_poly_count = grouped[points2.columns[0]].count()
    # Join Count column on to NHDCatchments table and keep only 'COMID','CatAreaSqKm','CatCount'
    final = polys.join(point_poly_count,on='FEATUREID', how='left')
    final = final[['FEATUREID','AreaSqKM',points2.columns[0]]].fillna(0)
    cols = ['COMID','CatAreaSqKm','CatCount']
    if not summaryfield == None: # Summarize fields in list with gpd table including duplicates
        point_poly_dups = sjoin(points, polys, how="left", op="within")   
        grouped2 = point_poly_dups.groupby('FEATUREID')
        for x in summaryfield: # Sum the field in summary field list for each catchment
            point_poly_stats = grouped2[x].sum()
            final = final.join(point_poly_stats,on='FEATUREID', how='left').fillna(0)
            cols.append('Cat' + x)
    final.columns = cols
    # Merge final table with Pct_Full table based on COMID and fill NA's with 0
    final = pd.merge(final,pct_full, on='COMID', how='left')
    final.CatPctFull = final.CatPctFull.fillna(100)
    #print "elapsed time " + str(dt.now()-startTime)
    return final   
#####################################################################################################################
def interVPU(tbl, cols, accum_type, zone, Connector, interVPUtbl, summaryfield):
    '''
    __author__ = "Rick Debbout <debbout.rick@epa.gov>"     
    Loads watershed values for given COMIDs to be appended to catResults table for accumulation.
    
    Arguments
    ---------
    tbl                   : Watershed Results table
    cols                  : list of columns from Cat Results table needed to append back onto Cat Results tables
    accum_type            : type metric to be accumulated, i.e. 'Categorical', 'Continuous', 'Count'
    zone                  : an NHDPlusV2 VPU number, i.e. 10, 16, 17
    Connector             : Location of the connector file
    InterVPUtbl           : table of interVPU exchanges
    summaryfield          : list of fields to summarize, only used when accum_type is 'Count'
    '''
    # Create subset of the tbl with a COMID in interVPUtbl
    throughVPUs = tbl[tbl.COMID.isin(interVPUtbl.thruCOMIDs.values)].set_index('COMID').copy()  
    # Create subset of InterVPUtbl that identifies the zone we are working on
    interVPUtbl = interVPUtbl.ix[interVPUtbl.FromZone.values == zone]
    throughVPUs.columns = cols
    # COMIDs in the toCOMID column need to swap values with COMIDs in other zones, those COMIDS are then sotred in toVPUS
    if any(interVPUtbl.toCOMIDs.values > 0): # [x for x in interVPUtbl.toCOMIDs if x > 0]
           interAlloc = '%s_%s.csv'%(Connector[:Connector.find('_connectors')],interVPUtbl.ToZone.values[0])
           tbl = pd.read_csv(interAlloc).set_index('COMID')
           toVPUs = tbl[tbl.index.isin([x for x in interVPUtbl.toCOMIDs if x > 0])].copy()        
    for interLine in interVPUtbl.values:
    # Loop through sub-setted interVPUtbl to make adjustments to COMIDS listed in the table 
        if interLine[4] > 0:
            AdjustCOMs(toVPUs,int(interLine[4]),int(interLine[0]),accum_type,throughVPUs,summaryfield)           
        if interLine[3] > 0:
            AdjustCOMs(throughVPUs,int(interLine[3]),int(interLine[0]),accum_type,None,summaryfield)
        if interLine[5] > 0:
            throughVPUs = throughVPUs.drop(int(interLine[5]))
    if any(interVPUtbl.toCOMIDs.values > 0): # if COMIDs came from other zone append to Connector table
    #!!!! This format assumes that the Connector table has already been made by the time it gets to these COMIDs!!!!!                
        con = pd.read_csv(Connector).set_index('COMID') 
        con.columns = map(str, con.columns)
        toVPUs = toVPUs.append(con)	
        toVPUs.to_csv(Connector)
    if os.path.exists(Connector): # if Connector already exists, read it in and append
        con = pd.read_csv(Connector).set_index('COMID') 
        con.columns = map(str, con.columns)
        throughVPUs = throughVPUs.append(con)
    throughVPUs.to_csv(Connector)
#####################################################################################################################
def AdjustCOMs(tbl, comid1, comid2, accum, tbl2 = None, summaryfield=None):
    '''
    __author__ = "Rick Debbout <debbout.rick@epa.gov>"     
    Adjusts values for COMIDs where values from one need to be subtracted from another.  
    Depending on the type of accum, subtracts values for each column in the table other than COMID and Pct_Full
    
    Arguments
    ---------
    tbl                   : throughVPU table from InterVPU function 
    comid1                : COMID which will be adjusted
    comid2                : COMID whose values will be subtracted from comid1
    accum                 : type metric to be accumulated, i.e. 'Categorical', 'Continuous', 'Count'
    tbl2                  : toVPU table from InterVPU function in the case where a COMID comes from a different zone
    summaryfield          : list of fields to summarize, only used when accum_type is 'Count'
    '''
    if tbl2 is None:
        tbl2 = tbl.copy()
    if accum == 'Count':
        tbl.ix[comid1,'CatAreaSqKm'] = tbl.ix[comid1,'CatAreaSqKm'] - tbl2.ix[comid2,'CatAreaSqKm']
        tbl.ix[comid1,'CatCount'] = tbl.ix[comid1,'CatCount'] - tbl2.ix[comid2,'CatCount']
        if summaryfield != None:
            for field in summaryfield:
                tbl.ix[comid1,'Cat' + field] = tbl.ix[comid1,'Cat' + field] - tbl2.ix[comid2,'Cat' + field]
    if accum == 'Continuous':
        for att in ['CatAreaSqKm','CatCount','CatSum']:
            tbl.ix[comid1,att] = tbl.ix[comid1,att]- tbl2.ix[comid2,att]
    if accum == 'Categorical':
        for idx in tbl.columns[:-1]:
            tbl.ix[comid1,idx] = tbl.ix[comid1,idx] - tbl2.ix[comid2,idx]
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
    upstream              : numpy array of all upstream arrays for each COMID
    tbl_type              : type metric to be accumulated, i.e. 'Categorical', 'Continuous', 'Count'
    '''
    coms = np.array(arr.COMID) #Read in COMIDs    
    indices = swapper(coms, upStream) #Get indices that will be used to map values
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
def createCatStats(accum_type, ingrid, inZoneData, out_dir, zone):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
                  "Ryan Hill <hill.ryan@epa.gov>"
    Uses the arcpy tools to perform ZonalStatisticsAsTable or TabulateArea based on accum_type and then formats 
    the results into a Catchment Results table
    
    Arguments
    ---------
    accum_type            : type metric to be accumulated, i.e. 'Categorical', 'Continuous', 'Count' 
    ingrid                : string to the landscape raster being summarized
    inZoneData            : string to the NHD catchment grid 
    out_dir               : string to directory where output is being stored
    zone                  : string of an NHDPlusV2 VPU zone, i.e. 10L, 16, 17
    '''
    #startTime = dt.now()
    arcpy.CheckOutExtension("spatial")
    arcpy.OverWriteOutput = 1
    arcpy.env.outputCoordinateSystem = "PROJCS['NAD_1983_Albers',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-96.0],PARAMETER['Standard_Parallel_1',29.5],PARAMETER['Standard_Parallel_2',45.5],PARAMETER['Latitude_Of_Origin',23.0],UNIT['Meter',1.0]]"
    arcpy.env.pyramid = "NONE"
    arcpy.env.cellSize = "30"
    arcpy.env.resamplingMethod = "NEAREST"
    arcpy.env.snapRaster = inZoneData
    if ingrid.count('.tif') or ingrid.count('.img'):
        outTable ="%s/zonalstats_%s%s.dbf"%(out_dir,ingrid.split("/")[-1].split(".")[0],zone)
    else:
        outTable ="%s/zonalstats_%s%s.dbf"%(out_dir,ingrid.split("/")[-1],zone)        	
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
        col_list = table.columns.tolist()
        col_list.sort()
        col_list.sort(key=len)         # table.columns
        table = table[col_list]
        if len(AllCols) != len(col_list[1:]):
            AllCols = ['VALUE_'+str(x) for x in AllCols]
            diff = list(set(AllCols) - set(col_list[1:]))
            diff.sort()
            diff.sort(key=len)
            for spot in diff:
                here = AllCols.index(spot) + 1
                table.insert(here,spot,0)
        table['AREA'] = table[table.columns.tolist()[1:]].sum(axis=1)
    #nhdTable = dbf2DF(inZoneData[:-3] + 'Catchment.dbf').ix[:,[1,0,2]]
    nhdTable = dbf2DF(inZoneData[:-3] + 'Catchment.dbf').ix[:,['FEATUREID','AREASQKM','GRIDCODE']]
    nhdTable = nhdTable.rename(columns = {'FEATUREID':'COMID','AREASQKM':'AreaSqKm'})
    result = pd.merge(nhdTable, table, how='left', left_on='GRIDCODE', right_on='VALUE')
    result['PctFull'] = (((result.AREA * 1e-6)/result.AreaSqKm)*100)
    result = result.drop(['GRIDCODE','VALUE','AREA'], axis=1).fillna(0)
    cols = result.columns[1:]
    result.columns = np.append('COMID', 'Cat' + cols.values) 
    #print "elapsed time " + str(dt.now()-startTime)
    return result
#####################################################################################################################    
def appendConnectors(cat, Connector, zone, interVPUtbl):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
                  "Ryan Hill <hill.ryan@epa.gov>"
    Appends the connector file of inter VPU COMIDS to the cat table before going into accumulation process
    
    Arguments
    ---------
    cat                   : Results table of catchment summarizations
    Connector             : string to file holding the table of inter VPU COMIDs
    zone                  : string of an NHDPlusV2 VPU zone, i.e. 10L, 16, 17
    interVPUtbl           : table of interVPU adjustments
    '''    
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
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
                  "Ryan Hill <hill.ryan@epa.gov>"
    Uses the 'Cat' and 'UpCat' columns to caluculate watershed values and returns those values in 'Cat' columns 
	so they can be appended to 'CatResult' tables in other zones before accumulation.
    
    Arguments
    ---------
    table                 : table containing watershed values 
    directory             : numpy array of all zones COMIDs 
    zone                  : numpy array with lengths of upstream COMIDs
    tbl_type              : type metric to be accumulated, i.e. 'Categorical', 'Continuous', 'Count'
    '''
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
def swapper(coms, upStream):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
                  "Ryan Hill <hill.ryan@epa.gov>"
    Uses the 'Cat' and 'UpCat' columns to caluculate watershed values and returns those values in 'Cat' columns 
	so they can be appended to 'CatResult' tables in other zones before accumulation.
    
    Arguments
    ---------
    coms                  : numpy array of all zones COMIDs 
    upstream              : numpy array of all upstream arrays for each COMID
    '''
    #Run numpy query to replace COMID raster with desired values:
    bsort = np.argsort(coms) 
    apos = np.searchsorted(coms[bsort], upStream) 
    indices = bsort[apos] 
    return indices
