# Functions for standardizing landscape rasters, allocating landscape metrics to NHDPlusV2
# catchments, accumulating metrics for upstream catchments, and writing final landscape metric tables
# Authors: Marc Weber<weber.marc@epa.gov>, Ryan Hill<hill.ryan@epa.gov>,
#          Darren Thornbrugh<thornbrugh.darren@epa.gov>, Rick Debbout<debbout.rick@epa.gov>,
#          and Tad Larsen<laresn.tad@epa.gov>
#          __                                       __
#    _____/ /_________  ____  ____ ___  _________ _/ /_ 
#   / ___/ __/ ___/ _ \/ __ `/ __ `__ \/ ___/ __ `/ __/
#  (__  ) /_/ /  /  __/ /_/ / / / / / / /__/ /_/ / /_ 
# /____/\__/_/   \___/\__,_/_/ /_/ /_/\___/\__,_/\__/ 
#
# Date: October 2015

# load modules
import os, sys
import arcpy
from arcpy.sa import TabulateArea, ZonalStatisticsAsTable
import pysal as ps
import numpy as np
import pandas as pd
import itertools
import decimal
import struct
import datetime
from datetime import datetime as dt
from collections import deque, defaultdict, OrderedDict
from osgeo import gdal, osr, ogr
from gdalconst import *
import rasterio
from rasterio import transform
#os.environ['GDAL_DATA'] = 'C:/Users/mweber/AppData/Local/Continuum/Anaconda/pkgs/libgdal-1.11.2-2/Library/data'
if rasterio.__version__[0] == '0':
    from rasterio.warp import calculate_default_transform, reproject, RESAMPLING
if rasterio.__version__[0] == '1':
    from rasterio.warp import calculate_default_transform, reproject, Resampling
import geopandas as gpd
from geopandas.tools import sjoin
import fiona

##############################################################################


class LicenseError(Exception):
    pass
##############################################################################

def dbfreader(f):
    """Returns an iterator over records in a Xbase DBF file.

    The first row returned contains the field names.
    The second row contains field specs: (type, size, decimal places).
    Subsequent rows contain the data records.
    If a record is marked as deleted, it is skipped.

    File should be opened for binary reads.

    """
    # See DBF format spec at:
    #     http://www.pgts.com.au/download/public/xbase.htm#DBF_STRUCT

    numrec, lenheader = struct.unpack('<xxxxLH22x', f.read(32))    
    numfields = (lenheader - 33) // 32

    fields = []
    for fieldno in xrange(numfields):
        name, typ, size, deci = struct.unpack('<11sc4xBB14x', f.read(32))
        name = name.replace('\0', '')       # eliminate NULs from string   
        fields.append((name, typ, size, deci))
    yield [field[0] for field in fields]
    yield [tuple(field[1:]) for field in fields]

    terminator = f.read(1)
    assert terminator == '\r'

    fields.insert(0, ('DeletionFlag', 'C', 1, 0))
    fmt = ''.join(['%ds' % fieldinfo[2] for fieldinfo in fields])
    fmtsiz = struct.calcsize(fmt)
    for i in xrange(numrec):
        record = struct.unpack(fmt, f.read(fmtsiz))
        if record[0] != ' ':
            continue                        # deleted record
        result = []
        for (name, typ, size, deci), value in itertools.izip(fields, record):
            if name == 'DeletionFlag':
                continue
            if typ == "N":
                value = value.replace('\0', '').lstrip()
                if value == '':
                    value = 0
                elif deci:
                    value = decimal.Decimal(value)
                else:
                    value = int(value)
            elif typ == 'C':
                value = value.rstrip()                                   
            elif typ == 'D':
                try:
                    y, m, d = int(value[:4]), int(value[4:6]), int(value[6:8])
                    value = datetime.date(y, m, d)
                except:
                    value = None
            elif typ == 'L':
                value = (value in 'YyTt' and 'T') or (value in 'NnFf' and 'F') or '?'
            elif typ == 'F':
                value = float(value)
            result.append(value)
        yield result
        
def dbf2DF(f, upper=True):
    data = list(dbfreader(open(f, 'rb')))
    if upper == False:    
        return pd.DataFrame(data[2:], columns=data[0])
    else:
        return pd.DataFrame(data[2:], columns=map(str.upper,data[0]))    
    
#def dbf2DF(dbfile, upper=True):
#    '''
#    __author__ = "Ryan Hill <hill.ryan@epa.gov>"
#                 "Marc Weber <weber.marc@epa.gov>"
#    Reads and converts a dbf file to a pandas data frame using pysal.
#
#    Arguments
#    ---------
#    dbfile           : a dbase (.dbf) file
#    '''
#    db = ps.open(dbfile)
#    cols = {col: db.by_col(col) for col in db.header}
#    db.close()  #Close dbf 
#    pandasDF = pd.DataFrame(cols)
#    if upper == True:
#        pandasDF.columns = pandasDF.columns.str.upper() 
#    return pandasDF
##############################################################################


def UpcomDict(nhd, interVPUtbl, zone):
    '''
    __author__ = "Ryan Hill <hill.ryan@epa.gov>"
                 "Marc Weber <weber.marc@epa.gov>"

    Creates a dictionary of all catchment connections in a major NHDPlus basin.
    For example, the function combines all from-to typology tables in the Mississippi
    Basin if 'MS' is provided as 'hydroregion' argument.

    Arguments
    ---------
    nhd             : the directory contining NHDPlus data
    interVPUtbl     : the table that holds the inter-VPU connections to manage connections and anomalies in the NHD
    '''
    #Returns UpCOMs dictionary for accumulation process 
    #Provide either path to from-to tables or completed from-to table
    flow = dbf2DF("%s/NHDPlusAttributes/PlusFlow.dbf" % (nhd))[['TOCOMID',
                                                                'FROMCOMID']]
    flow  = flow[flow.TOCOMID != 0]
    # check to see if out of zone values have FTYPE = 'Coastline'
    fls = dbf2DF("%s/NHDSnapshot/Hydrography/NHDFlowline.dbf" % (nhd))
    coastfl = fls.COMID[fls.FTYPE == 'Coastline']
    flow = flow[~flow.FROMCOMID.isin(coastfl.values)]
    # remove these FROMCOMIDs from the 'flow' table, there are three COMIDs here
    # that won't get filtered out
    remove = interVPUtbl.removeCOMs.values[interVPUtbl.removeCOMs.values!=0]
    flow = flow[~flow.FROMCOMID.isin(remove)]
    # find values that are coming from other zones and remove the ones that 
    # aren't in the interVPU table
    out = np.nonzero(np.setdiff1d(flow.FROMCOMID.values,fls.COMID.values))
    flow = flow[~flow.FROMCOMID.isin(
                np.setdiff1d(out, interVPUtbl.thruCOMIDs.values))]        
    # Now table is ready for processing and the UpCOMs dict can be created             
    fcom,tcom = flow.FROMCOMID.values,flow.TOCOMID.values
    UpCOMs = defaultdict(list)
    for i in range(0, len(flow), 1):
        FROMCOMID = fcom[i]
        if FROMCOMID == 0:
            UpCOMs[tcom[i]] = []
        else:
            UpCOMs[tcom[i]].append(FROMCOMID)
    # add IDs from UpCOMadd column if working in ToZone
    for interLine in interVPUtbl.values:
        if interLine[6] > 0 and interLine[2] == zone:
            UpCOMs[int(interLine[6])].append(int(interLine[0]))    
    return UpCOMs
##############################################################################




##############################################################################


def children(token, tree, chkset=None):
    '''
    __author__ = "Ryan Hill <hill.ryan@epa.gov>"
                 "Marc Weber <weber.marc@epa.gov>"
    returns a list of every child

    Arguments
    ---------
    token           : a single COMID
    tree            : Full dictionary of list of upstream COMIDs for each COMID in the zone
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
    if chkset != None:
        visited = visited.intersection(chkset)
    return list(visited)
##############################################################################


def bastards(token, tree, chkset=None):
    '''
    __author__ = "Ryan Hill <hill.ryan@epa.gov>"
                 "Marc Weber <weber.marc@epa.gov>"
    returns a list of every child w/ out father (key) included

    Arguments
    ---------
    token           : a single COMID
    tree            : Full dictionary of list of upstream COMIDs for each COMID in the zone
    chkset          : set of all the NHD catchment COMIDs, used to remove flowlines with no associated catchment
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
    if chkset != None:
        visited = visited.intersection(chkset)
    return list(visited)
##############################################################################


def getRasterInfo(FileName):
    '''
    __author__ =   "Marc Weber <weber.marc@epa.gov>"
                   "Ryan Hill <hill.ryan@epa.gov>"
    returns basic raster information for a given raster

    Arguments
    ---------
    FileName        : a raster file
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
##############################################################################


def GetRasterValueAtPoints(rasterfile, shapefile, fieldname):
    '''
    __author__ =   "Marc Weber <weber.marc@epa.gov>"
    returns raster values at points in a point shapefile
    assumes same projection in shapefile and raster file
    Arguments
    ---------
    rasterfile        : a raster file with full pathname and extension
    shapefile         : a shapefile with full pathname and extension
    fieldname         : field name in the shapefile to identify values
    '''
    src_ds=gdal.Open(rasterfile)
    no_data = src_ds.GetRasterBand(1).GetNoDataValue()
    gt=src_ds.GetGeoTransform()
    rb=src_ds.GetRasterBand(1)
    df = pd.DataFrame(columns=(fieldname, "RasterVal"))
    i = 0
    ds=ogr.Open(shapefile)
    lyr=ds.GetLayer()
    
    for feat in lyr:
        geom = feat.GetGeometryRef()
        name = feat.GetField(fieldname)
        mx,my=geom.GetX(), geom.GetY()  #coord in map units
    
        #Convert from map to pixel coordinates.
        #Only works for geotransforms with no rotation.
        px = int((mx - gt[0]) / gt[1]) #x pixel
        py = int((my - gt[3]) / gt[5]) #y pixel
    
        intval = rb.ReadAsArray(px,py,1,1)
        if intval == no_data:
            intval = -9999
        df.set_value(i,fieldname,name) 
        df.set_value(i,"RasterVal",float(intval)) 
        i+=1
#        print name
#        print intval[0] #intval is a numpy array, length=1 as we only asked for 1 pixel value
    return df
##############################################################################    

def Reclass(inras, outras, reclass_dict, dtype=None):
    '''
    __author__ =   "Marc Weber <weber.marc@epa.gov>"
                   "Ryan Hill <hill.ryan@epa.gov>"
    reclass a set of values in a raster to another value

    Arguments
    ---------
    inras           : an input raster file
    outras          : an output raster file
    reclass_dict    : dictionary of lookup values read in from lookup csv file
    in_nodata       : Returned no data values from
    out_dtype       : the data type of the raster, i.e. 'float32', 'uint8' (string)
    '''
    
    with rasterio.open(inras) as src:
        #Set dtype and nodata values
        if dtype is None: #If no dtype defined, use input dtype
            nd = src.meta['nodata']
            dtype = src.meta['dtype']
        else:
            try:
                nd = eval('np.iinfo(np.' + dtype + ').max')
            except:
                nd = eval('np.finfo(np.' + dtype + ').max')
            #exec 'nd = np.iinfo(np.'+out_dtype+').max'
        kwargs = src.meta.copy()
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='lzw',
            nodata=nd,
            dtype = dtype,
            bigtiff='YES'  # Output will be larger than 4GB
        )

        windows = src.block_windows(1)

        with rasterio.open(outras, 'w', **kwargs) as dst:
            for idx, window in windows:
                src_data = src.read(1, window=window)
                # Convert values
#                    src_data = np.where(src_data == in_nodata, nd, src_data).astype(dtype)
                for inval,outval in reclass_dict.iteritems():
                    if np.isnan(outval).any():
#                        src_data = np.where(src_data != inval, src_data, kwargs['nodata']).astype(dtype)
                        src_data = np.where(src_data == inval, nd, src_data).astype(dtype)
                    else:
                        src_data = np.where(src_data == inval, outval, src_data).astype(dtype)
#                    src_data = np.where(src_data == inval, outval, src_data)
                dst_data = src_data
                dst.write_band(1, dst_data, window=window)
##############################################################################


def rasterMath(inras, outras, expression=None, out_dtype=None):     
    '''
    __author__ =   "Marc Weber <weber.marc@epa.gov>"  
                   "Ryan Hill<hill.ryan@epa.gov>"
    Applies arithmetic operation to a raster by a given value and returns raster 
    in a specified data type - ideas from https://sgillies.net/page3.html

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
            if out_dtype is None:  #If no dtype defined, use input dtype
                nd = src.meta['nodata']
                dt = src.meta['dtype']
            else:
                try:
                    nd = eval('np.iinfo(np.' + out_dtype + ').max')
                except:
                    nd = eval('np.finfo(np.' + out_dtype + ').max')
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
##############################################################################


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
##############################################################################


def ShapefileProject(InShp, OutShp, CRS):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
    reprojects a shapefile with Fiona

    Arguments
    ---------
    InShp           : an input shapefile as a string, i.e. 'C:/Temp/inshape.shp'
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
##############################################################################


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
##############################################################################


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
                            dst_transform=transform.from_origin(affine[2], affine[5], dist.transform[0], dst.transform[0]),
                            dst_crs=dst_crs,
                            resampling=RESAMPLING.bilinear
                            )
                    elif resamp_type=='nearest':
                        reproject(
                            source=rasterio.band(src, 1),
                            destination=rasterio.band(dst, 1),
                            src_transform=src.transform,
                            src_crs=src.crs,
                            dst_transform=transform.from_origin(dst.transform[0], dst.transform[3], dst.transform[1], dst.transform[1]),
                            dst_crs=dst.crs,
                            resampling=RESAMPLING.nearest
                            )
##############################################################################


def PointInPoly(points, zone, inZoneData, pct_full, mask_dir, appendMetric, summaryfield=None):
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
    polys = gpd.GeoDataFrame.from_file(inZoneData)#.set_index('FEATUREID')
    points = points.to_crs(polys.crs)
    if len(mask_dir) > 1:
        polys = polys.drop('AreaSqKM', axis=1)
        tblRP = dbf2DF('%s/%s.tif.vat.dbf' % (mask_dir, zone))
        tblRP['AreaSqKM'] = (tblRP.COUNT * 900) * 1e-6
        tblRP['AreaSqKM'] = tblRP['AreaSqKM'].fillna(0)
        polys = pd.merge(polys, tblRP, left_on='GRIDCODE', right_on='VALUE', how='left')
        polys.crs = {u'datum': u'NAD83', u'no_defs': True, u'proj': u'longlat'}
        #polys = final[['FEATUREID', 'AreaSqKM', fld]]
    # Get list of lat/long fields in the table
    points['latlon_tuple'] = zip(points.geometry.map(lambda point: point.x),points.geometry.map(lambda point: point.y))
    # Remove duplicate points for 'Count'
    points2 = points.drop_duplicates('latlon_tuple') # points2.head() polys.head() point_poly_join.head()
    try:
        point_poly_join = sjoin(points2, polys, how="left", op="within") # point_poly_join.ix[point_poly_join.FEATUREID > 1]
        fld = 'GRIDCODE'  #next(str(unicode(x)) for x in polys.columns if x != 'geometry')
    except:
        polys['link'] = np.nan
        point_poly_join = polys #gpd.GeoDataFrame( pd.concat( [points2, polys], ignore_index=True) ) 
        fld = 'link'
    # Create group of all points in catchment
    grouped = point_poly_join.groupby('FEATUREID')
    point_poly_count = grouped[fld].count() # point_poly_count.head() next((x for x in points2.columns if x != 'geometry'),None)
    # Join Count column on to NHDCatchments table and keep only 'COMID','CatAreaSqKm','CatCount'
    final = polys.join(point_poly_count, on='FEATUREID', lsuffix='_', how='left')
    final = final[['FEATUREID', 'AreaSqKM', fld]].fillna(0)       
    cols = ['COMID', 'CatAreaSqKm%s' % appendMetric, 'CatCount%s' % appendMetric]
    if not summaryfield == None: # Summarize fields in list with gpd table including duplicates
        point_poly_dups = sjoin(points, polys, how="left", op="within")
        grouped2 = point_poly_dups.groupby('FEATUREID')
        for x in summaryfield: # Sum the field in summary field list for each catchment             
            point_poly_stats = grouped2[x].sum()
            point_poly_stats.name = x
            final = final.join(point_poly_stats, on='FEATUREID', how='left').fillna(0)
            cols.append('Cat' + x + appendMetric)
    final.columns = cols
    # Merge final table with Pct_Full table based on COMID and fill NA's with 0
    final = pd.merge(final, pct_full, on='COMID', how='left')
    if len(mask_dir) > 0:
        if not summaryfield == None:
            final.columns = ['COMID','CatAreaSqKmRp100','CatCountRp100']+ ['Cat'  + y + appendMetric for y in summaryfield] + ['CatPctFullRp100']
        else:
            final.columns = ['COMID','CatAreaSqKmRp100','CatCountRp100','CatPctFullRp100']
    final['CatPctFull%s' % appendMetric] = final['CatPctFull%s' % appendMetric].fillna(100) # final.head() final.ix[final.CatCount == 0]
    #print "elapsed time " + str(dt.now()-startTime)
    for name in final.columns:
        if 'AreaSqKm' in name:
            area = name
    final.loc[(final[area] == 0), final.columns[2:]] = np.nan
    return final
##############################################################################


def rat_to_dict(inraster, old_val, new_val):
    """
    __author__ =  "Matt Gregory <matt.gregory@oregonstate.edu>"
                  "Marc Weber <weber.marc@epa.gov>"

    Given a GDAL raster attribute table, convert to a pandas DataFrame.  Idea from
    Matt Gregory's gist: https://gist.github.com/grovduck/037d815928b2a9fe9516
    Arguments
    ---------
    in_rat      : input raster
    old_val     : current value in raster
    new_val     : lookup value to use to replace current value
    """
    # Open the raster and get a handle on the raster attribute table
    # Assume that we want the first band's RAT
    ds = gdal.Open(inraster)
    rb = ds.GetRasterBand(1)
    rat = rb.GetDefaultRAT()
    # Read in each column from the RAT and convert it to a series infering
    # data type automatically
    s = [pd.Series(rat.ReadAsArray(i), name=rat.GetNameOfCol(i))
         for i in xrange(rat.GetColumnCount())]
    # Convert the RAT to a pandas dataframe
    df = pd.concat(s, axis=1)
    # Close the dataset
    ds = None

    # Write out the lookup dictionary
    reclass_dict = pd.Series(df[new_val].values, index=df[old_val]).to_dict()
    return reclass_dict
##############################################################################


def interVPU(tbl, cols, accum_type, zone, Connector, interVPUtbl, summaryfield):
    '''
    __author__ = "Rick Debbout <debbout.rick@epa.gov>"
    Loads watershed values for given COMIDs to be appended to catResults table for accumulation.

    Arguments
    ---------
    tbl                   : Watershed Results table
    cols                  : list of columns from Cat Results table needed to overwrite onto Connector table
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
    
    # COMIDs in the toCOMID column need to swap values with COMIDs in other zones, those COMIDS are then sorted in toVPUS
    if any(interVPUtbl.toCOMIDs.values > 0): # [x for x in interVPUtbl.toCOMIDs if x > 0]
           interAlloc = '%s_%s.csv' % (Connector[:Connector.find('_connectors')], interVPUtbl.ToZone.values[0])
           tbl = pd.read_csv(interAlloc).set_index('COMID')
           toVPUs = tbl[tbl.index.isin([x for x in interVPUtbl.toCOMIDs if x > 0])].copy()
    for _,row in interVPUtbl.iterrows():
    # Loop through sub-setted interVPUtbl to make adjustments to COMIDS listed in the table
        if row.toCOMIDs > 0:
            AdjustCOMs(toVPUs,int(row.toCOMIDs), int(row.thruCOMIDs), throughVPUs)
        if row.AdjustComs > 0:
            AdjustCOMs(throughVPUs,int(row.AdjustComs), int(row.thruCOMIDs),None)
        if row.DropCOMID > 0:
            throughVPUs = throughVPUs.drop(int(row.DropCOMID))
    if any(interVPUtbl.toCOMIDs.values > 0): # if COMIDs came from other zone append to Connector table

    #!!!! This format assumes that the Connector table has already been made by the time it gets to these COMIDs!!!!!
        con = pd.read_csv(Connector).set_index('COMID') 
        con.columns = map(str, con.columns)
        toVPUs = toVPUs.append(con)
        toVPUs.to_csv(Connector)
    if os.path.exists(Connector):  # if Connector already exists, read it in and append
        con = pd.read_csv(Connector).set_index('COMID')
        con.columns = map(str, con.columns)
        throughVPUs = throughVPUs.append(con)
    throughVPUs.to_csv(Connector)
##############################################################################


def AdjustCOMs(tbl, comid1, comid2, tbl2 = None):  #  ,accum, summaryfield=None
    '''
    __author__ = "Rick Debbout <debbout.rick@epa.gov>"
    Adjusts values for COMIDs where values from one need to be subtracted from another.
    Depending on the type of accum, subtracts values for each column in the table other than COMID and Pct_Full

    Arguments
    ---------
    tbl                   : throughVPU table from InterVPU function
    comid1                : COMID which will be adjusted
    comid2                : COMID whose values will be subtracted from comid1
    tbl2                  : toVPU table from InterVPU function in the case where a COMID comes from a different zone
    '''

    if tbl2 is None:  # might be able to fix this in the arguments...
        tbl2 = tbl.copy()
    for idx in tbl.columns[:-1]:
        tbl.ix[comid1, idx] = tbl.ix[comid1, idx] - tbl2.ix[comid2, idx]
##############################################################################



def Accumulation(arr, COMIDs, lengths, upStream, tbl_type, icol='COMID'):
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
    tbl_type              : string value of table metrics to be returned
    icol                  : column in arr object to index
    '''
    coms = np.array(arr[icol])  #Read in COMIDs
    indices = swapper(coms, upStream)  #Get indices that will be used to map values
    del upStream  # a and indices are big - clean up to minimize RAM
    cols = arr.columns[1:]  #Get column names that will be accumulated
    z = np.zeros(COMIDs.shape)  #Make empty vector for placing values
    outT = np.zeros((len(COMIDs), len(arr.columns)))  #Make empty array for placing final values
    outT[:,0] = COMIDs  #Define first column as comids
    #Loop and accumulate values
    for k in range(0,len(cols)):
        col = cols[k]
        c = np.array(arr[col]) # arr[col].fillna(0) keep out zeros where no data!
        d = c[indices] #Make final vector from desired data (c)
        if 'PctFull' in col:
            area = np.array(arr.ix[:, 1])
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
        outT[:,k+1] = z  #np.nan_to_num() -used to convert to zeros here, now done above in the np.ma.average()
    outT = outT[np.in1d(outT[:,0], coms),:]  #Remove the extra COMIDs
    outDF = pd.DataFrame(outT)
    if tbl_type == 'Ws':
        outDF.columns = np.append(icol, map(lambda x : x.replace('Cat', 'Ws'),cols.values))
    if tbl_type == 'UpCat':
        outDF.columns = np.append(icol, 'Up' + cols.values)
    for name in outDF.columns:
        if 'AreaSqKm' in name:
            areaName = name
    outDF.loc[(outDF[areaName] == 0), outDF.columns[2:]] = np.nan  # identifies that there is no area in catchment mask, then NA values across the table   
    return outDF

##############################################################################


def createCatStats(accum_type, LandscapeLayer, inZoneData, out_dir, zone, by_RPU, mask_dir, NHD_dir, hydroregion, appendMetric):

    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
                  "Ryan Hill <hill.ryan@epa.gov>"
    Uses the arcpy tools to perform ZonalStatisticsAsTable or TabulateArea based on accum_type and then formats
    the results into a Catchment Results table with 'PctFull'Calculated

    Arguments
    ---------
    accum_type            : type metric to be accumulated, i.e. 'Categorical', 'Continuous', 'Count'
    LandscapeLayer        : string of the landscape raster name
    inZoneData            : string to the NHD catchment grid
    out_dir               : string to directory where output is being stored
    zone                  : string of an NHDPlusV2 VPU zone, i.e. 10L, 16, 17
    '''

    try:
        if arcpy.CheckExtension("spatial") == "Available":
            arcpy.CheckOutExtension("spatial")
        else:
            raise LicenseError
        arcpy.env.cellSize = "30"
        arcpy.env.snapRaster = inZoneData
        if by_RPU == 0:
            if LandscapeLayer.count('.tif') or LandscapeLayer.count('.img'):
                outTable ="%s/DBF_stash/zonalstats_%s%s%s.dbf" % (out_dir,LandscapeLayer.split("/")[-1].split(".")[0], appendMetric, zone)
            else:
                outTable ="%s/DBF_stash/zonalstats_%s%s%s.dbf" % (out_dir,LandscapeLayer.split("/")[-1], appendMetric, zone)
            if not os.path.exists(outTable):
                if accum_type == 'Categorical':
                    TabulateArea(inZoneData, 'VALUE', LandscapeLayer, "Value", outTable, "30")
                if accum_type == 'Continuous':
                    ZonalStatisticsAsTable(inZoneData, 'VALUE', LandscapeLayer, outTable, "DATA", "ALL")
            table = dbf2DF(outTable)
        if by_RPU == 1:
            hydrodir = '/'.join(inZoneData.split('/')[:-2]) + '/NEDSnapshot'
            rpuList = []
            for subdirs in os.listdir(hydrodir):
                elev = "%s/%s/elev_cm" % (hydrodir, subdirs)
                rpuList.append(subdirs[-3:])
                print 'working on ' + elev
                outTable = out_dir + "/zonalstats_elev%s.dbf" % (subdirs[-3:])
                if not os.path.exists(outTable):
                    ZonalStatisticsAsTable(inZoneData, 'VALUE', elev, outTable, "DATA", "ALL")
            for rpu in range(len(rpuList)):
                if rpu == 0:
                    table = dbf2DF(out_dir + "/zonalstats_elev%s.dbf" % (rpuList[rpu]))
                else:
                    table = pd.concat([table, dbf2DF(out_dir + "/zonalstats_elev%s.dbf" % (rpuList[rpu]))])
            if len(rpuList) > 1:
                clean = table.groupby('VALUE')['AREA'].nlargest(1).reset_index().rename(columns={0:'AREA', 'level_1': 'index'})
                table = pd.merge(table.reset_index(), clean, on=['VALUE','AREA', 'index'], how ='right').set_index('index')
                table = table.drop_duplicates(subset='VALUE')
        arcpy.CheckInExtension("spatial")
    except LicenseError:
        print("Spatial Analyst license is unavailable")
    except arcpy.ExecuteError:
        print(arcpy.GetMessages(2))

    if len(mask_dir) > 1:
        nhdtbl = dbf2DF('%s/NHDPlus%s/NHDPlus%s/NHDPlusCatchment/Catchment.dbf' % (NHD_dir, hydroregion, zone)).ix[:,['FEATUREID', 'AREASQKM', 'GRIDCODE']]
        tbl = dbf2DF(outTable)
        if accum_type == 'Categorical':
            tbl = chkColumnLength(tbl, LandscapeLayer)               
        tbl2 = dbf2DF('%s/%s.tif.vat.dbf' % (mask_dir, zone)) 
        tbl2 = pd.merge(tbl2, nhdtbl, how='right', left_on='VALUE', right_on='GRIDCODE').fillna(0).drop('VALUE', axis=1)
        result = pd.merge(tbl2, tbl, left_on='GRIDCODE', right_on='VALUE', how='left')             
        if accum_type == 'Continuous':
            result['PctFull%s' % appendMetric] = ((result.COUNT_y / result.COUNT_x) * 100)
            result['AreaSqKm%s' % appendMetric] = (result.COUNT_x * 900) * 1e-6
            result.loc[(result['AreaSqKm%s' % appendMetric] > 0) & (result['SUM'].isnull()), 'PctFull%s' % appendMetric] = 0  # identifies that there is a riparion zone, but no coverage
            result = result[['FEATUREID', 'AreaSqKm%s' % appendMetric, 'COUNT_y', 'SUM', 'PctFull%s' % appendMetric]]
            result.columns = ['COMID', 'AreaSqKm%s' % appendMetric, 'Count%s' % appendMetric, 'Sum%s' % appendMetric, 'PctFull%s' % appendMetric]
        if accum_type == 'Categorical':
            result['TotCount'] = result[tbl.columns.tolist()[1:]].sum(axis=1)
            result['PctFull%s' % appendMetric] = ((result.TotCount/(result.COUNT * 900)) * 100)
            result['AreaSqKm%s' % appendMetric] = (result.COUNT * 900) * 1e-6
            result = result[['FEATUREID','AreaSqKm%s' % appendMetric] + tbl.columns.tolist()[1:] + ['PctFull%s' % appendMetric]]
            result.columns = ['COMID','AreaSqKm%s' % appendMetric] + [lbl + appendMetric for lbl in tbl.columns.tolist()[1:]] + ['PctFull%s' % appendMetric]
    else:
        if accum_type == 'Continuous':
            table = table[['VALUE', 'AREA', 'COUNT', 'SUM']]
            table = table.rename(columns = {'COUNT':'Count', 'SUM':'Sum'})              
        if accum_type == 'Categorical':
            table = chkColumnLength(table,LandscapeLayer)
            table['AREA'] = table[table.columns.tolist()[1:]].sum(axis=1)
        nhdTable = dbf2DF(inZoneData[:-3] + 'Catchment.dbf').ix[:,['FEATUREID', 'AREASQKM', 'GRIDCODE']]
        nhdTable = nhdTable.rename(columns = {'FEATUREID':'COMID', 'AREASQKM':'AreaSqKm'})
        result = pd.merge(nhdTable, table, how='left', left_on='GRIDCODE', right_on='VALUE')
        if LandscapeLayer.split('/')[-1].split('.')[0] == 'rdstcrs':
           slptbl = dbf2DF('%s/NHDPlus%s/NHDPlus%s/NHDPlusAttributes/elevslope.dbf' % (NHD_dir, hydroregion, zone)).ix[:,['COMID', 'SLOPE']]
           slptbl.loc[slptbl['SLOPE'] == -9998.0, 'SLOPE'] = 0           
           result = pd.merge(result, slptbl, on='COMID', how='left')
           result.SLOPE = result.SLOPE.fillna(0)
           result['SlpWtd'] = result['Sum'] * result['SLOPE']
           result = result.drop(['SLOPE'], axis=1)           
        result['PctFull'] = (((result.AREA * 1e-6)/result.AreaSqKm.astype('float'))*100).fillna(0)
        result = result.drop(['GRIDCODE', 'VALUE', 'AREA'], axis=1)
    cols = result.columns[1:]
    result.columns = np.append('COMID', 'Cat' + cols.values)
    return result
##############################################################################


def chkColumnLength(table, LandscapeLayer):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Ryan Hill <hill.ryan@epa.gov>"
    Checks the number of columns returned from zonal stats and adds any of the 
    categorical values that that didn't exist within the zone and fills the 
    column with zeros so that all categories will be represented in the table.

    Arguments
    ---------
    table                 : Results table of catchment summarizations
    LandscapeLayer        : string to file holding the table of inter VPU COMIDs
    '''
    # Get ALL categorical values from the dbf associated with the raster to retain all values
    # in the raster in every table, even when a given value doesn't exist in a given hydroregion
    AllCols = dbf2DF(LandscapeLayer + '.vat.dbf').VALUE.tolist()
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
            table.insert(here, spot ,0)
    return table
##############################################################################        


def appendConnectors(cat, Connector, zone, interVPUtbl):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Ryan Hill <hill.ryan@epa.gov>"
    Appends the connector file of inter VPU COMIDS to the cat table before going into accumulation process

    Arguments
    ---------
    cat                   : Results table of catchment summarization
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

##############################################################################   


def swapper(coms, upStream):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Ryan Hill <hill.ryan@epa.gov>"
    Creates array of indexes for all upstream COMIDs that will be summarized for each local catchment.

    Arguments
    ---------
    coms                  : numpy array of all COMIDs in the zone
    upstream              : numpy array of all upstream COMIDs for each local catchment
    '''
    bsort = np.argsort(coms)
    apos = np.searchsorted(coms[bsort], upStream)
    indices = bsort[apos]
    return indices
##############################################################################


def makeNumpyVectors(d, interVPUtbl, inputs, NHD_dir):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
                  "Ryan Hill <hill.ryan@epa.gov>"
    Uses the NHD tables to create arrays of upstream catchments which are used in the Accumulation function

    Arguments
    ---------
    d             : directory where .npy files will be stored
    interVPUtbl           : table of inter-VPU connections
    inputs                : Ordered Dictionary of Hydroregions and zones from the NHD
    NHD_dir               : directory where NHD is stored
    '''
    if not os.path.exists(d + '/allCatCOMs.npy'):
        print 'Making allFLOWCOMs numpy file'
        chkval = 0
        for reg in inputs:
            print 'All ' + reg
            hydroregion = inputs[reg]
            catchment = "%s/NHDPlus%s/NHDPlus%s/NHDPlusCatchment/Catchment.dbf" % (NHD_dir, hydroregion, reg)
            cats = dbf2DF(catchment).set_index('FEATUREID')
            if chkval == 0:
                AllCOMs = np.array(cats.index)
            else:
                COMIDs = np.array(cats.index)
                AllCOMs = np.concatenate((AllCOMs, COMIDs))
            chkval += 1
        AllCOMs = AllCOMs[np.nonzero(AllCOMs)]
        np.save(d + '/allCatCOMs.npy', AllCOMs)
    cat = np.load(d + '/allCatCOMs.npy')
    cats = set(cat)
    cats.discard(0)
    os.mkdir(d + '/bastards')
    os.mkdir(d + '/children')            
    for zone in inputs:
        if not os.path.exists('%s/bastards/accum_%s.npz' % (d,zone)):
            print zone
            hydroregion = inputs[zone]
            print 'Making UpStreamComs dictionary...'
            start_time = dt.now()
            NHD_pre = '%s/NHDPlus%s/NHDPlus%s' % (NHD_dir, hydroregion, zone)
            UpStreamComs = UpcomDict(NHD_pre, interVPUtbl, zone)
            print("--- %s seconds ---" % (dt.now() - start_time))
            print '....'
            print 'Making numpy bastard vectors...'
            start_time = dt.now()
            tbl_dir = NHD_dir + "/NHDPlus%s/NHDPlus%s/NHDPlusCatchment/Catchment.dbf" % (hydroregion, zone)
            catch = dbf2DF(tbl_dir).set_index('FEATUREID')   
            COMIDs = np.append(np.array(catch.index),np.array(interVPUtbl.ix[np.logical_and((np.array(interVPUtbl.ToZone) == zone),(np.array(interVPUtbl.DropCOMID) == 0))].thruCOMIDs))
            del catch
            if 0 in COMIDs:
                COMIDs = np.delete(COMIDs,np.where(COMIDs == 0))
            if zone == '14':
                cats.remove(17029298) # this problem has been cleaned out of NHDPlus
            a = map(lambda x: bastards(x, UpStreamComs, cats), COMIDs)
            lengths = np.array([len(v) for v in a])
            a = np.int32(np.hstack(np.array(a)))    #Convert to 1d vector
            np.savez_compressed('%s/bastards/accum_%s.npz' % (d,zone), comids=COMIDs,lengths=lengths,upstream=a)
            del a, lengths
            print 'Making numpy children vectors...'
            b = map(lambda x: children(x, UpStreamComs, cats), COMIDs)
            lengths = np.array([len(v) for v in b])
            b = np.int32(np.hstack(np.array(b)))  #Convert to 1d vector
            np.savez_compressed('%s/children/accum_%s.npz' % (d,zone), comids=COMIDs,lengths=lengths,upstream=b)
            print("--- %s seconds ---" % (dt.now() - start_time))
            print '___________________'
##############################################################################
def makeNumpyVectors2(d, interVPUtbl, inputs, NHD_dir):
    '''
    __author__ =  "Marc Weber <weber.marc@epa.gov>" 
                  "Ryan Hill <hill.ryan@epa.gov>"
    Uses the NHD tables to create arrays of upstream catchments which are used in the Accumulation function

    Arguments
    ---------
    d             : directory where .npy files will be stored
    interVPUtbl           : table of inter-VPU connections
    inputs                : Ordered Dictionary of Hydroregions and zones from the NHD
    NHD_dir               : directory where NHD is stored
    '''
    os.mkdir(d + '/bastards')         
    for zone in inputs:
        if not os.path.exists('%s/bastards/accum_%s.npz' % (d,zone)):
            print zone
            hydroregion = inputs[zone]
            print 'Making UpStreamComs dictionary...'
            start_time = dt.now()
            NHD_pre = '%s/NHDPlus%s/NHDPlus%s' % (NHD_dir, hydroregion, zone)
            UpStreamComs = UpcomDict(NHD_pre, interVPUtbl, zone)
            print("--- %s seconds ---" % (dt.now() - start_time))
            print '....'
            print 'Making numpy bastard vectors...'
            start_time = dt.now()
            tbl_dir = NHD_dir + "/NHDPlus%s/NHDPlus%s/NHDSnapshot/Hydrography/NHDFlowline.dbf" % (hydroregion, zone)
            catch = dbf2DF(tbl_dir).set_index('COMID')   
            COMIDs = np.append(np.array(catch.index),np.array(interVPUtbl.ix[np.logical_and((np.array(interVPUtbl.ToZone) == zone),(np.array(interVPUtbl.DropCOMID) == 0))].thruCOMIDs))
            del catch
            if 0 in COMIDs:
                COMIDs = np.delete(COMIDs,np.where(COMIDs == 0))
            a = map(lambda x: bastards(x, UpStreamComs), COMIDs)
            lengths = np.array([len(v) for v in a])
            a = np.int32(np.hstack(np.array(a)))    #Convert to 1d vector
            np.savez_compressed('%s/bastards/accum_%s.npz' % (d,zone), comids=COMIDs,lengths=lengths,upstream=a)
            print("--- %s seconds ---" % (dt.now() - start_time))
            print '___________________'
##############################################################################


def makeVPUdict(directory):
    '''
    __author__ =  "Rick Debbout <debbout.rick@epa.gov>"
    Creates an OrderdDict for looping through regions of the NHD to carry InterVPU 
    connections across VPU zones

    Arguments
    ---------
    directory             : the directory contining NHDPlus data at the top level
    '''
    B = dbf2DF('%s/NHDPlusGlobalData/BoundaryUnit.dbf' % directory)
    B = B.drop(B.ix[B.DRAINAGEID.isin(['HI','CI'])].index, axis=0)
    B = B.ix[B.UNITTYPE == 'VPU'].sort_values('HYDROSEQ',ascending=False)
    inputs = OrderedDict()  # inputs = OrderedDict((k, inputs[k]) for k in order)
    for idx, row in B.iterrows():
        inputs[row.UNITID] = row.DRAINAGEID
        #print 'HydroRegion (value): ' + row.DRAINAGEID + ' in VPU (key): ' + row.UNITID
    np.save('%s/StreamCat_npy/zoneInputs.npy' % directory, inputs)
    return inputs
##############################################################################


def makeRPUdict(directory):
    '''
    __author__ =  "Rick Debbout <debbout.rick@epa.gov>"
    Creates an OrderdDict for looping through regions of the NHD RPU zones

    Arguments
    ---------
    directory             : the directory contining NHDPlus data at the top level
    '''
    B = dbf2DF('%s/NHDPlusGlobalData/BoundaryUnit.dbf' % directory)
    B = B.drop(B.ix[B.DRAINAGEID.isin(['HI','CI'])].index, axis=0)      
    rpuinputs = OrderedDict()
    for idx, row in B.iterrows():
        if row.UNITTYPE == 'RPU':
            hr = row.DRAINAGEID
            rpu = row.UNITID
            for root, dirs, files in os.walk('%s/NHDPlus%s' % (directory, hr)):
                for name in dirs:
                    if rpu in os.path.join(root, name):
                        zone = os.path.join(root, name).split('\\')[-3].replace('NHDPlus','')
                        break
            if not zone in rpuinputs.keys():
                rpuinputs[zone] = []
            print 'RPU: ' + rpu + ' in zone: ' + zone 
            rpuinputs[zone].append(row.UNITID)
    np.save('%s/StreamCat_npy/rpuInputs.npy' % directory, rpuinputs)
    return rpuinputs
##############################################################################

def NHD_Dict(directory, unit='VPU'):
    '''
    __author__ =  "Rick Debbout <debbout.rick@epa.gov>"
    Creates an OrderdDict for looping through regions of the NHD RPU zones

    Arguments
    ---------
    directory             : the directory contining NHDPlus data at the top level
    unit                  : Vector or Raster processing units 'VPU' or 'RPU'
    '''  
    if unit == 'VPU':
        if not os.path.exists('%s/StreamCat_npy' % directory):
            os.mkdir('%s/StreamCat_npy' % directory)    
        if not os.path.exists('%s/StreamCat_npy/zoneInputs.npy' % directory):
            inputs = makeVPUdict(directory)
        else:
            inputs = np.load('%s/StreamCat_npy/zoneInputs.npy' % directory).item() 
    if unit == 'RPU':
        if not os.path.exists('%s/StreamCat_npy' % directory):
            os.mkdir('%s/StreamCat_npy' % directory)    
        if not os.path.exists('%s/StreamCat_npy/rpuInputs.npy' % directory):
            inputs = makeRPUdict(directory)
        else:
            inputs = np.load('%s/StreamCat_npy/rpuInputs.npy' % directory).item() 
    return inputs
          
##############################################################################

    
def findUpstreamNpy(zone, com, numpy_dir):
    '''
    __author__ =  "Rick Debbout <debbout.rick@epa.gov>"
    Finds upstream array of COMIDs for any given catchment COMID

    Arguments
    ---------
    zone                  : string of an NHDPlusV2 VPU zone, i.e. 10L, 16, 17
    com                   : COMID of NHD Catchment, integer
    numpy_dir             : directory where .npy files are stored
    '''
    comids = np.load(numpy_dir + '/comids' + zone + '.npy')
    lengths= np.load(numpy_dir + '/lengths' + zone + '.npy')
    upStream = np.load(numpy_dir + '/upStream' + zone + '.npy')
    itemindex = int(np.where(comids == com)[0])
    n = lengths[:itemindex].sum()
    arrlen = lengths[itemindex]
    return upStream[n:n+arrlen]
