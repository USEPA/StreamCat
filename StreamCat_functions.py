"""       __                                       __
    _____/ /_________  ____  ____ ___  _________ _/ /_
   / ___/ __/ ___/ _ \/ __ `/ __ `__ \/ ___/ __ `/ __/
  (__  ) /_/ /  /  __/ /_/ / / / / / / /__/ /_/ / /_
 /____/\__/_/   \___/\__,_/_/ /_/ /_/\___/\__,_/\__/

 Functions for standardizing landscape rasters, allocating landscape metrics
 to NHDPlusV2 catchments, accumulating metrics for upstream catchments, and
 writing final landscape metric tables

 Authors: Marc Weber<weber.marc@epa.gov>
          Ryan Hill<hill.ryan@epa.gov>
          Darren Thornbrugh<thornbrugh.darren@epa.gov>
          Rick Debbout<debbout.rick@epa.gov>
          Tad Larsen<laresn.tad@epa.gov>

 Date: October 2015
"""

import os
import sys
import time
from collections import OrderedDict, defaultdict, deque
from typing import Generator

import numpy as np
import pandas as pd
import rasterio
#from gdalconst import *
from osgeo import gdal, ogr, osr
from rasterio import transform

if rasterio.__version__[0] == "0":
    from rasterio.warp import RESAMPLING, calculate_default_transform, reproject
if rasterio.__version__[0] == "1":
    from rasterio.warp import calculate_default_transform, reproject, Resampling

import fiona
import geopandas as gpd
from geopandas.tools import sjoin

os.environ["PATH"] += r";C:\Program Files\ArcGIS\Pro\bin"
sys.path.append(r"C:\Program Files\ArcGIS\Pro\Resources\ArcPy")
import arcpy
from arcpy.sa import TabulateArea, ZonalStatisticsAsTable

##############################################################################


class LicenseError(Exception):
    pass


##############################################################################


def UpcomDict(nhd, interVPUtbl, zone):
    """
    __author__ = "Ryan Hill <hill.ryan@epa.gov>"
                 "Marc Weber <weber.marc@epa.gov>"

    Creates a dictionary of all catchment connections in a major NHDPlus basin.
    For example, the function combines all from-to typology tables in the Mississippi
    Basin if 'MS' is provided as 'hydroregion' argument.

    Arguments
    ---------
    nhd             : the directory contining NHDPlus data
    interVPUtbl     : the table that holds the inter-VPU connections to manage connections and anomalies in the NHD
    """
    # Returns UpCOMs dictionary for accumulation process
    # Provide either path to from-to tables or completed from-to table
    flow = dbf2DF(f"{nhd}/NHDPlusAttributes/PlusFlow.dbf")[["TOCOMID", "FROMCOMID"]]
    flow = flow[(flow.TOCOMID != 0) & (flow.FROMCOMID != 0)]
    # check to see if out of zone values have FTYPE = 'Coastline'
    fls = dbf2DF(f"{nhd}/NHDSnapshot/Hydrography/NHDFlowline.dbf")
    coastfl = fls.COMID[fls.FTYPE == "Coastline"]
    flow = flow[~flow.FROMCOMID.isin(coastfl.values)]
    # remove these FROMCOMIDs from the 'flow' table, there are three COMIDs here
    # that won't get filtered out
    remove = interVPUtbl.removeCOMs.values[interVPUtbl.removeCOMs.values != 0]
    flow = flow[~flow.FROMCOMID.isin(remove)]
    # find values that are coming from other zones and remove the ones that
    # aren't in the interVPU table
    out = np.setdiff1d(flow.FROMCOMID.values, fls.COMID.values)
    out = out[np.nonzero(out)]
    flow = flow[~flow.FROMCOMID.isin(np.setdiff1d(out, interVPUtbl.thruCOMIDs.values))]
    # Now table is ready for processing and the UpCOMs dict can be created
    fcom, tcom = flow.FROMCOMID.values, flow.TOCOMID.values
    UpCOMs = defaultdict(list)
    for i in range(0, len(flow), 1):
        from_comid = fcom[i]
        if from_comid == 0:
            continue
        else:
            UpCOMs[tcom[i]].append(from_comid)
    # add IDs from UpCOMadd column if working in ToZone, forces the flowtable connection though not there
    for interLine in interVPUtbl.values:
        if interLine[6] > 0 and interLine[2] == zone:
            UpCOMs[int(interLine[6])].append(int(interLine[0]))
    return UpCOMs


##############################################################################


def children(token, tree, chkset=None):
    """
    __author__ = "Ryan Hill <hill.ryan@epa.gov>"
                 "Marc Weber <weber.marc@epa.gov>"
    returns a list of every child

    Arguments
    ---------
    token           : a single COMID
    tree            : Full dictionary of list of upstream COMIDs for each COMID in the zone
    chkset          : set of all the NHD catchment COMIDs used to remove flowlines with no associated catchment
    """
    visited = set()
    to_crawl = deque([token])
    while to_crawl:
        current = to_crawl.popleft()
        if current in visited:
            continue
        visited.add(current)
        node_children = set(tree[current])
        to_crawl.extendleft(node_children - visited)
    # visited.remove(token)
    if chkset != None:
        visited = visited.intersection(chkset)
    return list(visited)


##############################################################################


def bastards(token, tree):
    """
    __author__ = "Ryan Hill <hill.ryan@epa.gov>"
                 "Marc Weber <weber.marc@epa.gov>"
    returns a list of every child w/ out father (key) included

    Arguments
    ---------
    token           : a single COMID
    tree            : Full dictionary of list of upstream COMIDs for each COMID in the zone
    chkset          : set of all the NHD catchment COMIDs, used to remove flowlines with no associated catchment
    """
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
    return list(visited)


##############################################################################


def getRasterInfo(FileName):
    """
    __author__ =   "Marc Weber <weber.marc@epa.gov>"
                   "Ryan Hill <hill.ryan@epa.gov>"
    returns basic raster information for a given raster

    Arguments
    ---------
    FileName        : a raster file
    """
    SourceDS = gdal.Open(FileName, GA_ReadOnly)
    NDV = SourceDS.GetRasterBand(1).GetNoDataValue()
    stats = SourceDS.GetRasterBand(1).GetStatistics(True, True)
    xsize = SourceDS.RasterXSize
    ysize = SourceDS.RasterYSize
    GeoT = SourceDS.GetGeoTransform()
    prj = SourceDS.GetProjection()
    Projection = osr.SpatialReference(wkt=prj)
    Proj_projcs = Projection.GetAttrValue("projcs")
    #    if Proj_projcs == None:
    #        Proj_projcs = 'Not Projected'
    Proj_geogcs = Projection.GetAttrValue("geogcs")
    DataType = SourceDS.GetRasterBand(1).DataType
    DataType = gdal.GetDataTypeName(DataType)
    return (NDV, stats, xsize, ysize, GeoT, Proj_projcs, Proj_geogcs, DataType)


##############################################################################


def GetRasterValueAtPoints(rasterfile, shapefile, fieldname):
    """
    __author__ =   "Marc Weber <weber.marc@epa.gov>"
    returns raster values at points in a point shapefile
    assumes same projection in shapefile and raster file
    Arguments
    ---------
    rasterfile        : a raster file with full pathname and extension
    shapefile         : a shapefile with full pathname and extension
    fieldname         : field name in the shapefile to identify values
    """
    src_ds = gdal.Open(rasterfile)
    no_data = src_ds.GetRasterBand(1).GetNoDataValue()
    gt = src_ds.GetGeoTransform()
    rb = src_ds.GetRasterBand(1)
    df = pd.DataFrame(columns=(fieldname, "RasterVal"))
    ds = ogr.Open(shapefile)
    lyr = ds.GetLayer()
    data = []
    for feat in lyr:
        geom = feat.GetGeometryRef()
        name = feat.GetField(fieldname)
        mx, my = geom.GetX(), geom.GetY()  # coord in map units

        # Convert from map to pixel coordinates.
        # Only works for geotransforms with no rotation.
        px = int((mx - gt[0]) / gt[1])  # x pixel
        py = int((my - gt[3]) / gt[5])  # y pixel

        intval = rb.ReadAsArray(px, py, 1, 1)
        if intval == no_data:
            intval = -9999
        data.append((name, float(intval)))

    df = pd.DataFrame(data, columns=(fieldname, "RasterVal"))
    return df


##############################################################################


def Reclass(inras, outras, reclass_dict, dtype=None):
    """
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
    """

    with rasterio.open(inras) as src:
        # Set dtype and nodata values
        if dtype is None:  # If no dtype defined, use input dtype
            nd = src.meta["nodata"]
            dtype = src.meta["dtype"]
        else:
            try:
                nd = eval("np.iinfo(np." + dtype + ").max")
            except:
                nd = eval("np.finfo(np." + dtype + ").max")
            # exec 'nd = np.iinfo(np.'+out_dtype+').max'
        kwargs = src.meta.copy()
        kwargs.update(
            driver="GTiff",
            count=1,
            compress="lzw",
            nodata=nd,
            dtype=dtype,
            bigtiff="YES",  # Output will be larger than 4GB
        )

        windows = src.block_windows(1)

        with rasterio.open(outras, "w", **kwargs) as dst:
            for idx, window in windows:
                src_data = src.read(1, window=window)
                # Convert values
                # src_data = np.where(src_data == in_nodata, nd, src_data).astype(dtype)
                for inval, outval in reclass_dict.iteritems():
                    if np.isnan(outval).any():
                        # src_data = np.where(src_data != inval, src_data, kwargs['nodata']).astype(dtype)
                        src_data = np.where(src_data == inval, nd, src_data).astype(
                            dtype
                        )
                    else:
                        src_data = np.where(src_data == inval, outval, src_data).astype(
                            dtype
                        )
                # src_data = np.where(src_data == inval, outval, src_data)
                dst_data = src_data
                dst.write_band(1, dst_data, window=window)


##############################################################################


def rasterMath(inras, outras, expression=None, out_dtype=None):
    """
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
    """
    expression = expression.replace(inras, "src_data")

    with rasterio.drivers():
        with rasterio.open(inras) as src:
            # Set dtype and nodata values
            if out_dtype is None:  # If no dtype defined, use input dtype
                nd = src.meta["nodata"]
                dt = src.meta["dtype"]
            else:
                try:
                    nd = eval("np.iinfo(np." + out_dtype + ").max")
                except:
                    nd = eval("np.finfo(np." + out_dtype + ").max")
                # exec 'nd = np.iinfo(np.'+out_dtype+').max'
                dt = out_dtype
            kwargs = src.meta.copy()
            kwargs.update(driver="GTiff", count=1, compress="lzw", dtype=dt, nodata=nd)

            windows = src.block_windows(1)

            with rasterio.open(outras, "w", **kwargs) as dst:
                for idx, window in windows:
                    src_data = src.read(1, window=window)
                    # Where src not eq to orig nodata, multiply by val, else set to new nodata. Set dtype
                    if expression == None:
                        # No expression produces copy of original raster (can use new data type)
                        dst_data = np.where(
                            src_data != src.meta["nodata"], src_data, kwargs["nodata"]
                        ).astype(dt)
                    else:
                        dst_data = np.where(
                            src_data != src.meta["nodata"],
                            eval(expression),
                            kwargs["nodata"],
                        ).astype(dt)
                    dst.write_band(1, dst_data, window=window)


##############################################################################


def Project(inras, outras, dst_crs, template_raster, nodata):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Ryan Hill <hill.ryan@epa.gov>"
    reprojects and resamples a raster using rasterio

    Arguments
    ---------
    inras           : an input raster with full path name
    outras          : an output raster with full path name
    outproj         : projection to apply to output raster in EPSG format, i.e. EPSG:5070
    resamp          : resampling method to use - either nearest or bilinear
    """
    with rasterio.open(inras) as src:
        with rasterio.open(template_raster) as tmp:
            affine, width, height = calculate_default_transform(
                src.crs, dst_crs, src.width, src.height, *tmp.bounds
            )
            kwargs = src.meta.copy()
            kwargs.update(
                {
                    "crs": dst_crs,
                    "transform": affine,
                    "affine": affine,
                    "width": width,
                    "height": height,
                    "driver": "GTiff",
                }
            )

            with rasterio.open(outras, "w", **kwargs) as dst:
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
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
    reprojects a shapefile with Fiona

    Arguments
    ---------
    InShp           : an input shapefile as a string, i.e. 'C:/Temp/inshape.shp'
    OutShp          : an output shapefile as a string, i.e. 'C:/Temp/outshape.shp'
    CRS             : the output CRS in Fiona format
    """
    # Open a file for reading
    with fiona.open(InShp, "r") as source:
        sink_schema = source.schema.copy()
        sink_schema["geometry"] = "Point"

        # Open an output file, using the same format driver and passing the desired
        # coordinate reference system
        with fiona.open(
            OutShp,
            "w",
            crs=CRS,
            driver=source.driver,
            schema=sink_schema,
        ) as sink:
            for f in source:
                # Write the record out.
                sink.write(f)

        # The sink's contents are flushed to disk and the file is closed
        # when its ``with`` block ends. This effectively executes
        # ``sink.flush(); sink.close()``.


##############################################################################


def Resample(inras, outras, resamp_type, resamp_res):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Ryan Hill <hill.ryan@epa.gov>"
    Resamples a raster using rasterio

    Arguments
    ---------
    inras           : an input raster with full path name
    outras          : an output raster with full path name
    resamp_type     : resampling method to use - either nearest or bilinear
    resamp_res      : resolution to apply to output raster
    """
    with rasterio.open(inras) as src:
        affine, width, height = calculate_default_transform(
            src.crs, src.crs, src.width, src.height, *src.bounds, resolution=resamp_res
        )
        kwargs = src.meta.copy()
        kwargs.update(
            {
                "crs": src.crs,
                "transform": affine,
                "affine": affine,
                "width": width,
                "height": height,
                "driver": "GTiff",
            }
        )
        with rasterio.open(outras, "w", **kwargs) as dst:
            if resamp_type == "bilinear":
                reproject(
                    source=rasterio.band(src, 1),
                    destination=rasterio.band(dst, 1),
                    src_transform=src.affine,
                    src_crs=src.crs,
                    dst_transform=src.affine,
                    dst_crs=dst_crs,
                    resampling=RESAMPLING.bilinear,
                    compress="lzw",
                )
            elif resamp_type == "nearest":
                reproject(
                    source=rasterio.band(src, 1),
                    destination=rasterio.band(dst, 1),
                    src_transform=affine,
                    src_crs=src.crs,
                    dst_transform=affine,
                    dst_crs=src.crs,
                    resampling=RESAMPLING.nearest,
                    compress="lzw",
                )


##############################################################################


def ProjectResamp(inras, outras, out_proj, resamp_type, out_res):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Ryan Hill <hill.ryan@epa.gov>"
    reprojects and resamples a raster using rasterio

    Arguments
    ---------
    inras           : an input raster with full path name
    outras          : an output raster with full path name
    outproj         : projection to apply to output raster in EPSG format, i.e. EPSG:5070
    resamp          : resampling method to use - either nearest or bilinear
    """
    with rasterio.drivers():
        with rasterio.open(inras) as src:
            affine, width, height = calculate_default_transform(
                src.crs, out_proj, src.width, src.height, *src.bounds
            )
            kwargs = src.meta.copy()
            kwargs.update(
                {
                    "crs": out_proj,
                    "transform": affine,
                    "affine": affine,
                    "width": width,
                    "height": height,
                    "driver": "GTiff",
                }
            )

            windows = src.block_windows(1)

            with rasterio.open(outras, "w", **kwargs) as dst:
                for idx, window in windows:
                    if resamp_type == "bilinear":
                        reproject(
                            source=rasterio.band(src, 1),
                            destination=rasterio.band(dst, 1),
                            src_transform=src.affine,
                            src_crs=src.crs,
                            dst_transform=transform.from_origin(
                                affine[2],
                                affine[5],
                                dist.transform[0],
                                dst.transform[0],
                            ),
                            dst_crs=dst_crs,
                            resampling=RESAMPLING.bilinear,
                        )
                    elif resamp_type == "nearest":
                        reproject(
                            source=rasterio.band(src, 1),
                            destination=rasterio.band(dst, 1),
                            src_transform=src.transform,
                            src_crs=src.crs,
                            dst_transform=transform.from_origin(
                                dst.transform[0],
                                dst.transform[3],
                                dst.transform[1],
                                dst.transform[1],
                            ),
                            dst_crs=dst.crs,
                            resampling=RESAMPLING.nearest,
                        )


##############################################################################


def get_raster_value_at_points(
    points, rasterfile, fieldname=None, val_name=None, out_df=False
):
    """
    Find value at point (x,y) for every point in points of the given
    rasterfile.

    Arguments
    ---------
    points: str | gpd.GeoDataFrame | generator
        path to point file, or point GeoDataFrame, or generator of (x,y) tuples
    rasterfile: str
        path to raster
    fieldname: str
        attribute in points that identifies name given to index
    out_df: bool
        return pd.DataFrame of values, index will match `fieldname` if used,
        else the index will be equivalent to `range(len(points))`

    Returns
    ---------
    list | pd.DataFrame
        Values of rasterfile | if `out_df` True, dataframe of values.

    """
    if isinstance(points, str):
        points = gpd.read_file(points)
    if isinstance(points, gpd.GeoDataFrame):
        assert points.geometry.type.all() == "Point"
        if fieldname:
            points.set_index(fieldname)
        points = points.geometry.apply(lambda g: (g.x, g.y))
    if isinstance(points, Generator):
        pass

    with rasterio.open(rasterfile) as src:
        data = [s[0] for s in src.sample(points)]

    if out_df:
        return pd.DataFrame(index=points.index, data={val_name: data})
    else:
        return data


def mask_points(points, mask_dir, INPUTS, nodata_vals=[0, -2147483648.0]):
    """
    Filter points to those that only lie within the mask.

    Arguments
    ---------
    points: gpd.GeoDataFrame
        point GeoDataFrame to be filtered
    mask_dir: str
        path to folder holding masked rasters for every VPU
    INPUTS: collections.OrderedDict
        dictionary of vector processing units and hydroregions from NHDPlusV21
    nodata_vals: list
        values of the raster that exist outside of the mask zone

    Returns
    ---------
    gpd.GeoDataFrame
        filtered points that only lie within the masked areas

    """
    temp = pd.DataFrame(index=points.index)
    for zone, hydroregion in INPUTS.items():
        pts = get_raster_value_at_points(points, f"{mask_dir}/{zone}.tif", out_df=True)
        temp = temp.merge(~pts.isin(nodata_vals), left_index=True, right_index=True)
    xx = temp.sum(axis=1)
    return points.iloc[xx.loc[xx == 1].index]


def PointInPoly(points, vpu, catchments, pct_full, mask_dir, appendMetric, summary):
    """
    Filter points to those that only lie within the mask.

    Arguments
    ---------
    points: gpd.GeoDataFrame
        point GeoDataFrame
    vpu: str
        Vector Processing Unit from NHDPlusV21
    catchments: collections.OrderedDict
        dictionary of vector processing units and hydroregions from NHDPlusV21
    pct_full: pd.DataFrame
        DataFrame with `PCT_FULL` calculated from catchments that
        intersect the US border from TIGER files
    mask_dir: str
        path to folder holding masked rasters for every VPU else empty
    appendMetric: str
        string to be appended to metrics from ControlTable_StreamCat.csv
    summary: list
        strings that identify columns from the attribute table in the points
        GeoDataFrame to be summed in returned DataFrame if `summary` is defined

    Returns
    ---------
    pd.DataFrame
        Table with count of spatial points in every catchment feature
        optionally with the summary of attributes from the points attribute
        table

    """

    polys = gpd.GeoDataFrame.from_file(catchments)
    polys.to_crs(points.crs, inplace=True)
    if mask_dir:
        rat = dbf2DF(f"{mask_dir}/{vpu}.tif.vat.dbf")
        rat["AreaSqKM"] = ((rat.COUNT * 900) * 1e-6).fillna(0)
        polys = pd.merge(
            polys.drop("AreaSqKM", axis=1),
            rat[["VALUE", "AreaSqKM"]],
            left_on="GRIDCODE",
            right_on="VALUE",
            how="left",
        )

    # Get list of lat/long fields in the table
    points["latlon_tuple"] = tuple(
        zip(
            points.geometry.map(lambda point: point.x),
            points.geometry.map(lambda point: point.y),
        )
    )
    # Remove duplicate points for 'Count'
    points2 = points.drop_duplicates("latlon_tuple")
    try:
        point_poly_join = sjoin(points2, polys, how="left", op="within")
        fld = "GRIDCODE"
    except:
        polys["link"] = np.nan
        point_poly_join = polys
        fld = "link"
    # Create group of all points in catchment
    grouped = point_poly_join.groupby("FEATUREID")
    point_poly_count = grouped[fld].count()
    point_poly_count.name = "COUNT"
    # Join Count column on to NHDCatchments table and keep only
    # ['COMID','CatAreaSqKm','CatCount']
    final = polys.join(point_poly_count, on="FEATUREID", lsuffix="_", how="left")
    final = final[["FEATUREID", "AreaSqKM", "COUNT"]].fillna(0)
    cols = ["COMID", f"CatAreaSqKm{appendMetric}", f"CatCount{appendMetric}"]
    if not summary == None:  # Summarize fields including duplicates
        point_poly_dups = sjoin(points, polys, how="left", op="within")
        grouped2 = point_poly_dups.groupby("FEATUREID")
        for x in summary:  # Sum the field in summary field list for each catchment
            point_poly_stats = grouped2[x].sum()
            point_poly_stats.name = x
            final = final.join(point_poly_stats, on="FEATUREID", how="left").fillna(0)
            cols.append("Cat" + x + appendMetric)
    final.columns = cols
    # Merge final table with Pct_Full table based on COMID and fill NA's with 0
    final = pd.merge(final, pct_full, on="COMID", how="left")
    if len(mask_dir) > 0:
        if not summary == None:
            final.columns = (
                ["COMID", "CatAreaSqKmRp100", "CatCountRp100"]
                + ["Cat" + y + appendMetric for y in summary]
                + ["CatPctFullRp100"]
            )
        else:
            final.columns = [
                "COMID",
                "CatAreaSqKmRp100",
                "CatCountRp100",
                "CatPctFullRp100",
            ]
    final[f"CatPctFull{appendMetric}"] = final[f"CatPctFull{appendMetric}"].fillna(100)
    for name in final.columns:
        if "AreaSqKm" in name:
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
    s = [
        pd.Series(rat.ReadAsArray(i), name=rat.GetNameOfCol(i))
        for i in xrange(rat.GetColumnCount())
    ]
    # Convert the RAT to a pandas dataframe
    df = pd.concat(s, axis=1)
    # Close the dataset
    ds = None

    # Write out the lookup dictionary
    reclass_dict = pd.Series(df[new_val].values, index=df[old_val]).to_dict()
    return reclass_dict


##############################################################################


def interVPU(tbl, cols, accum_type, zone, Connector, interVPUtbl):
    """
    Loads watershed values for given COMIDs to be appended to catResults table for accumulation.

    Arguments
    ---------
    tbl                   : Watershed Results table
    cols                  : list of columns from Cat Results table needed to overwrite onto Connector table
    accum_type            : type metric to be accumulated, i.e. 'Categorical', 'Continuous', 'Count'
    zone                  : an NHDPlusV2 VPU number, i.e. 10, 16, 17
    Connector             : Location of the connector file
    InterVPUtbl           : table of interVPU exchanges
    """
    # Create subset of the tbl with a COMID in interVPUtbl
    throughVPUs = (
        tbl[tbl.COMID.isin(interVPUtbl.thruCOMIDs.values)].set_index("COMID").copy()
    )
    # Create subset of InterVPUtbl that identifies the zone we are working on
    interVPUtbl = interVPUtbl.loc[interVPUtbl.FromZone.values == zone]
    throughVPUs.columns = cols

    # COMIDs in the toCOMID column need to swap values with COMIDs in other
    # zones, those COMIDS are then sorted in toVPUS
    if any(interVPUtbl.toCOMIDs.values > 0):
        interAlloc = "%s_%s.csv" % (
            Connector[: Connector.find("_connectors")],
            interVPUtbl.ToZone.values[0],
        )
        tbl = pd.read_csv(interAlloc).set_index("COMID")
        toVPUs = tbl[tbl.index.isin([x for x in interVPUtbl.toCOMIDs if x > 0])].copy()
    for _, row in interVPUtbl.iterrows():
        # Loop through sub-setted interVPUtbl to make adjustments to COMIDS listed in the table
        if row.toCOMIDs > 0:
            AdjustCOMs(toVPUs, int(row.toCOMIDs), int(row.thruCOMIDs), throughVPUs)
        if row.AdjustComs > 0:
            AdjustCOMs(throughVPUs, int(row.AdjustComs), int(row.thruCOMIDs), None)
        if row.DropCOMID > 0:
            throughVPUs = throughVPUs.drop(int(row.DropCOMID))
    if any(interVPUtbl.toCOMIDs.values > 0):
        con = pd.read_csv(Connector).set_index("COMID")
        con.columns = map(str, con.columns)
        toVPUs = pd.concat([toVPUs,con], axis=0, ignore_index=False)
        toVPUs.to_csv(Connector)
    if os.path.exists(Connector):  # if Connector already exists, read it in and append
        con = pd.read_csv(Connector).set_index("COMID")
        con.columns = map(str, con.columns)
        throughVPUs = pd.concat([throughVPUs, con], axis=0, ignore_index=False) 
    throughVPUs.to_csv(Connector)


##############################################################################


def AdjustCOMs(tbl, comid1, comid2, tbl2=None):
    """
    Adjusts values for COMIDs where values from one need to be subtracted from another.
    Depending on the type of accum, subtracts values for each column in the table other than COMID and Pct_Full

    Arguments
    ---------
    tbl                   : throughVPU table from InterVPU function
    comid1                : COMID which will be adjusted
    comid2                : COMID whose values will be subtracted from comid1
    tbl2                  : toVPU table from InterVPU function in the case where a COMID comes from a different zone
    """

    if tbl2 is None:  # might be able to fix this in the arguments
        tbl2 = tbl.copy()
    for idx in tbl.columns[:-1]:
        tbl.loc[comid1, idx] = tbl.loc[comid1, idx] - tbl2.loc[comid2, idx]


##############################################################################


def Accumulation(tbl, comids, lengths, upstream, tbl_type, icol="COMID"):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Ryan Hill <hill.ryan@epa.gov>"
    Uses the 'Cat' and 'UpCat' columns to caluculate watershed values and returns those values in 'Cat' columns
        so they can be appended to 'CatResult' tables in other zones before accumulation.

    Arguments
    ---------
    arr                   : table containing watershed values
    comids                : numpy array of all zones comids
    lengths               : numpy array with lengths of upstream comids
    upstream              : numpy array of all upstream arrays for each COMID
    tbl_type              : string value of table metrics to be returned
    icol                  : column in arr object to index
    """
    # RuntimeWarning: invalid value encountered in double_scalars
    np.seterr(all="ignore")
    coms = tbl[icol].values.astype("int32")  # Read in comids
    indices = swapper(coms, upstream)  # Get indices that will be used to map values
    del upstream  # a and indices are big - clean up to minimize RAM
    cols = tbl.columns[1:]  # Get column names that will be accumulated
    z = np.zeros(comids.shape)  # Make empty vector for placing values
    data = np.zeros((len(comids), len(tbl.columns)))
    data[:, 0] = comids  # Define first column as comids
    accumulated_indexes = np.add.accumulate(lengths)[:-1]
    # Loop and accumulate values
    for index, column in enumerate(cols, 1):
        col_values = tbl[column].values.astype("float")
        all_values = np.split(col_values[indices], accumulated_indexes)
        if tbl_type == "Ws":
            # add identity value to each array for full watershed
            all_values = np.array(
                [np.append(val, col_values[idx]) for idx, val in enumerate(all_values)],
                dtype=object,
            )

            # all_values = [np.append(val, col_values[idx]) for idx, val in enumerate(all_values)]

        if index == 1:
            area = all_values.copy()
        if "PctFull" in column:
            values = [
                np.ma.average(np.nan_to_num(val), weights=w)
                for val, w in zip(all_values, area)
            ]
        elif "MIN" in column or "MAX" in column:
            func = np.max if "MAX" in column else np.min
            # initial is necessary to eval empty upstream arrays
            # these values will be overwritten w/ nan later

            # initial = -999 if "MAX" in column else 999999

            initial = -999999 if "MAX" in column else 999999

            values = np.array([func(val, initial=initial) for val in all_values])
            values[lengths == 0] = col_values[lengths == 0]
        else:
            values = np.array([np.nansum(val) for val in all_values])
        data[:, index] = values
    data = data[np.in1d(data[:, 0], coms), :]  # Remove the extra comids
    outDF = pd.DataFrame(data)
    prefix = "UpCat" if tbl_type == "Up" else "Ws"
    outDF.columns = [icol] + [c.replace("Cat", prefix) for c in cols.tolist()]
    areaName = outDF.columns[outDF.columns.str.contains("Area")][0]
    # identifies that there is no area in catchment mask,
    # then NA values for everything past Area, covers upcats w. no area AND
    # WS w/ no area
    no_area_rows, na_columns = (outDF[areaName] == 0), outDF.columns[2:]
    outDF.loc[no_area_rows, na_columns] = np.nan
    return outDF


##############################################################################


def createCatStats(
    accum_type,
    LandscapeLayer,
    inZoneData,
    out_dir,
    zone,
    by_RPU,
    mask_dir,
    NHD_dir,
    hydroregion,
    appendMetric,
):

    """
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
    """

    try:
        arcpy.env.cellSize = "30"
        arcpy.env.snapRaster = inZoneData
        if by_RPU == 0:
            if LandscapeLayer.count(".tif") or LandscapeLayer.count(".img"):
                outTable = "%s/DBF_stash/zonalstats_%s%s%s.dbf" % (
                    out_dir,
                    LandscapeLayer.split("/")[-1].split(".")[0],
                    appendMetric,
                    zone,
                )
            else:
                outTable = "%s/DBF_stash/zonalstats_%s%s%s.dbf" % (
                    out_dir,
                    LandscapeLayer.split("/")[-1],
                    appendMetric,
                    zone,
                )
            if not os.path.exists(outTable):
                if accum_type == "Categorical":
                    TabulateArea(
                        inZoneData, "VALUE", LandscapeLayer, "Value", outTable, "30"
                    )
                if accum_type == "Continuous":
                    ZonalStatisticsAsTable(
                        inZoneData, "VALUE", LandscapeLayer, outTable, "DATA", "ALL"
                    )
            try:
                table = dbf2DF(outTable)
            except fiona.errors.DriverError as e:
                # arc occassionally doesn't release the file and fails here
                print(e, "\n\n!EXCEPTION CAUGHT! TRYING AGAIN!")
                time.sleep(60)
                table = dbf2DF(outTable)
        if by_RPU == 1:
            hydrodir = "/".join(inZoneData.split("/")[:-2]) + "/NEDSnapshot"
            rpuList = []
            for subdirs in os.listdir(hydrodir):
                elev = "%s/%s/elev_cm" % (hydrodir, subdirs)
                rpuList.append(subdirs[-3:])
                print("working on " + elev)
                outTable = out_dir + "/DBF_stash/zonalstats_elev%s.dbf" % (subdirs[-3:])
                if not os.path.exists(outTable):
                    ZonalStatisticsAsTable(
                        inZoneData, "VALUE", elev, outTable, "DATA", "ALL"
                    )
            for count, rpu in enumerate(rpuList):
                if count == 0:
                    table = dbf2DF(f"{out_dir}/DBF_stash/zonalstats_elev{rpu}.dbf")
                else:
                    table = pd.concat(
                        [
                            table,
                            dbf2DF(f"{out_dir}/DBF_stash/zonalstats_elev{rpu}.dbf"),
                        ]
                    )
            if len(rpuList) > 1:
                table.reset_index(drop=True, inplace=True)
                table = table.loc[table.groupby("VALUE").AREA.idxmax()]
    except LicenseError:
        print("Spatial Analyst license is unavailable")
    except arcpy.ExecuteError:
        print("Failing at the ExecuteError!")
        print(arcpy.GetMessages(2))

    if mask_dir:
        nhdtbl = dbf2DF(
            f"{NHD_dir}/NHDPlus{hydroregion}/NHDPlus{zone}"
            "/NHDPlusCatchment/Catchment.dbf"
        ).loc[:, ["FEATUREID", "AREASQKM", "GRIDCODE"]]
        tbl = dbf2DF(outTable)
        if accum_type == "Categorical":
            tbl = chkColumnLength(tbl, LandscapeLayer)
        # We need to use the raster attribute table here for PctFull & Area
        # TODO: this needs to be considered when making masks!!!
        tbl2 = dbf2DF(f"{mask_dir}/{zone}.tif.vat.dbf")
        tbl2 = (
            pd.merge(tbl2, nhdtbl, how="right", left_on="VALUE", right_on="GRIDCODE")
            .fillna(0)
            .drop("VALUE", axis=1)
        )
        result = pd.merge(tbl2, tbl, left_on="GRIDCODE", right_on="VALUE", how="left")
        if accum_type == "Continuous":
            result["PctFull%s" % appendMetric] = (result.COUNT_y / result.COUNT_x) * 100
            result["AreaSqKm%s" % appendMetric] = (result.COUNT_x * 900) * 1e-6
            result.loc[
                (result["AreaSqKm%s" % appendMetric] > 0) & (result["SUM"].isnull()),
                "PctFull%s" % appendMetric,
            ] = 0  # identifies that there is a riparion zone, but no coverage
            result = result[
                [
                    "FEATUREID",
                    "AreaSqKm%s" % appendMetric,
                    "COUNT_y",
                    "SUM",
                    "PctFull%s" % appendMetric,
                ]
            ]
            result.columns = [
                "COMID",
                "AreaSqKm%s" % appendMetric,
                "Count%s" % appendMetric,
                "Sum%s" % appendMetric,
                "PctFull%s" % appendMetric,
            ]
        if accum_type == "Categorical":
            result["TotCount"] = result[tbl.columns.tolist()[1:]].sum(axis=1)
            result["PctFull%s" % appendMetric] = (
                result.TotCount / (result.COUNT * 900)
            ) * 100
            result["AreaSqKm%s" % appendMetric] = (result.COUNT * 900) * 1e-6
            result = result[
                ["FEATUREID", "AreaSqKm%s" % appendMetric]
                + tbl.columns.tolist()[1:]
                + ["PctFull%s" % appendMetric]
            ]
            result.columns = (
                ["COMID", "AreaSqKm%s" % appendMetric]
                + [lbl + appendMetric for lbl in tbl.columns.tolist()[1:]]
                + ["PctFull%s" % appendMetric]
            )
    else:
        # TODO: `table` here is referenced as `tbl` above -- confusing
        if accum_type == "Continuous":
            if by_RPU == 1:
                table = table[["VALUE", "AREA", "COUNT", "SUM", "MIN", "MAX"]]
            else:
                table = table[["VALUE", "AREA", "COUNT", "SUM"]]
            table = table.rename(columns={"COUNT": "Count", "SUM": "Sum"})
        if accum_type == "Categorical":
            table = chkColumnLength(table, LandscapeLayer)
            table["AREA"] = table[table.columns.tolist()[1:]].sum(axis=1)
        nhdTable = dbf2DF(inZoneData[:-3] + "Catchment.dbf").loc[
            :, ["FEATUREID", "AREASQKM", "GRIDCODE"]
        ]
        nhdTable = nhdTable.rename(
            columns={"FEATUREID": "COMID", "AREASQKM": "AreaSqKm"}
        )
        result = pd.merge(
            nhdTable, table, how="left", left_on="GRIDCODE", right_on="VALUE"
        )
        if LandscapeLayer.split("/")[-1].split(".")[0] == "rdstcrs":
            slptbl = dbf2DF(
                "%s/NHDPlus%s/NHDPlus%s/NHDPlusAttributes/elevslope.dbf"
                % (NHD_dir, hydroregion, zone)
            ).loc[:, ["COMID", "SLOPE"]]
            slptbl.loc[slptbl["SLOPE"] == -9998.0, "SLOPE"] = 0
            result = pd.merge(result, slptbl, on="COMID", how="left")
            result.SLOPE = result.SLOPE.fillna(0)
            result["SlpWtd"] = result["Sum"] * result["SLOPE"]
            result = result.drop(["SLOPE"], axis=1)
        result["PctFull"] = (
            ((result.AREA * 1e-6) / result.AreaSqKm.astype("float")) * 100
        ).fillna(0)
        result = result.drop(["GRIDCODE", "VALUE", "AREA"], axis=1)
    cols = result.columns[1:]
    result.columns = np.append("COMID", "Cat" + cols.values)
    return result  # ALL NAs need to be filled w/ zero here for Accumulation!!


def get_rat_vals(raster):
    """
    Build the raster attribute table and extract distinct values. Assume that
    it has only one band.

    WARNING!: this has only been used with GEOTIFF and ERDAS IMAGINE (IMG)
    formatted rasters, results may vary with other formats.

    Parameters
    ---------
    raster : str
        Absolute path to raster

    Returns
    ---------
    list
        Integer values that exist in the raster with `Opacity > 0`
    """
    ds = gdal.Open(raster)
    rb = ds.GetRasterBand(1)
    rat = rb.GetDefaultRAT()
    df = pd.DataFrame.from_dict(
        {rat.GetNameOfCol(i): rat.ReadAsArray(i) for i in range(rat.GetColumnCount())}
    )
    ds = None
    return df.loc[(df.Opacity > 0) & (df.Histogram > 0)].index.tolist()


def chkColumnLength(table, landscape_layer):
    """
    Checks the number of columns returned from zonal stats and adds any of the
    categorical values that that didn't exist within the zone and fills the
    column with zeros so that all categories will be represented in the table.

    Need ALL categorical values from the dbf associated with the
    landscape_layer to retain all values every table, even when a given value
    doesn't exist in a given VPU (vector processing unit).

    Output from the stats dbf headers follows the form:
        | VALUE | VALUE_11 | VALUE_12 | VALUE_21 | VALUE_22 | VALUE_23 |

    The VALUE w/o any int is the one that holds the COMID.

    TODO: this should really be split out into two parts, the lanscape_layer
    TODO: raster vals only need to be read in once and can then be checked
    TODO: against the output TabulateArea to find if all vals exist, then,
    TODO: if not, we can insert the empty columns where needed.

    Parameters
    ---------
    table : pd.DataFrame
        Results table of catchment summarizations - from arc dbf
    landscape_layer : str
        string to file that statistics are being read from

    Returns
    ---------
    pd.DataFrame
        if any missing VALUEs from landscape_layer else
    """
    rat_file = f"{landscape_layer}.vat.dbf"
    if not os.path.exists(rat_file):
        # build RAT with GDAL
        rat_cols = get_rat_vals(landscape_layer)
    else:
        rat_cols = dbf2DF(rat_file).VALUE.tolist()
    tbl_cols = table.columns.tolist()
    tbl_cols.sort(key=len)  # sort() is done in place on a list -- returns None
    table, val_cols = table[tbl_cols], tbl_cols[1:]
    rat_cols = [f"VALUE_{x}" for x in rat_cols]  # align ints w/ strs
    missing = list(set(rat_cols).difference(set(tbl_cols)))
    if missing:
        missing.sort()
        missing.sort(key=len)
        for col in missing:
            idx = rat_cols.index(col)
            table.insert(idx + 1, col, 0)  # add 1 to shift for VALUE
    return table


def appendConnectors(cat, Connector, zone, interVPUtbl):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Ryan Hill <hill.ryan@epa.gov>"
    Appends the connector file of inter VPU COMIDS to the cat table before going into accumulation process

    Arguments
    ---------
    cat                   : Results table of catchment summarization
    Connector             : string to file holding the table of inter VPU COMIDs
    zone                  : string of an NHDPlusV2 VPU zone, i.e. 10L, 16, 17
    interVPUtbl           : table of interVPU adjustments
    """
    con = pd.read_csv(Connector)
    for comidx in con.COMID.values.astype(int):
        if comidx in cat.COMID.values.astype(int):
            cat = cat.drop(cat[cat.COMID == comidx].index)
    con = con.loc[
        con.COMID.isin(
            np.append(
                interVPUtbl.loc[interVPUtbl.ToZone.values == zone].thruCOMIDs.values,
                interVPUtbl.loc[interVPUtbl.ToZone.values == zone].toCOMIDs.values[
                    np.nonzero(
                        interVPUtbl.loc[
                            interVPUtbl.ToZone.values == zone
                        ].toCOMIDs.values
                    )
                ],
            )
        )
    ]

    cat = pd.concat([cat, con], axis=0, ignore_index=False)   
    return cat.reset_index(drop=True)


##############################################################################


def swapper(coms, upStream):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Ryan Hill <hill.ryan@epa.gov>"
    Creates array of indexes for all upstream COMIDs that will be summarized for each local catchment.

    Arguments
    ---------
    coms                  : numpy array of all COMIDs in the zone
    upstream              : numpy array of all upstream COMIDs for each local catchment
    """
    bsort = np.argsort(coms)
    apos = np.searchsorted(coms[bsort], upStream)
    indices = bsort[apos]
    return indices


##############################################################################
def make_all_cat_comids(nhd, inputs):
    print("Making allFLOWCOMs numpy file, reading zones...", end="", flush=True)
    all_comids = np.array([], dtype=np.int32)
    for zone, hr in inputs.items():
        print(zone, end=", ", flush=True)
        pre = f"{nhd}/NHDPlus{hr}/NHDPlus{zone}"
        cats = dbf2DF(f"{pre}/NHDPlusCatchment/Catchment.dbf")
        all_comids = np.append(all_comids, cats.FEATUREID.values.astype(int))
    np.savez_compressed("./accum_npy/allCatCOMs.npz", all_comids=all_comids)
    print("...done!")
    return set(all_comids)  # RETURN A SET!


def makeNumpyVectors(inter_tbl, nhd):
    """
    Uses the NHD tables to create arrays of upstream catchments which are used
    in the Accumulation function

    Arguments
    ---------
    inter_tbl   : table of inter-VPU connections
    nhd         : directory where NHD is stored
    """
    os.mkdir("accum_npy")
    inputs = nhd_dict(nhd)
    all_comids = make_all_cat_comids(nhd, inputs)
    print("Making numpy files in zone...", end="", flush=True)
    for zone, hr in inputs.items():
        print(zone, end=", ", flush=True)
        pre = f"{nhd}/NHDPlus{hr}/NHDPlus{zone}"
        flow = dbf2DF(f"{pre}/NHDPlusAttributes/PlusFlow.dbf")[["TOCOMID", "FROMCOMID"]]
        flow = flow[(flow.TOCOMID != 0) & (flow.FROMCOMID != 0)]
        fls = dbf2DF(f"{pre}/NHDSnapshot/Hydrography/NHDFlowline.dbf")
        coastfl = fls.COMID[fls.FTYPE == "Coastline"]
        flow = flow[~flow.FROMCOMID.isin(coastfl.values)]
        # remove these FROMCOMIDs from the 'flow' table, there are three COMIDs
        # here that won't get filtered out any other way
        flow = flow[~flow.FROMCOMID.isin(inter_tbl.removeCOMs)]
        # find values that are coming from other zones and remove the ones that
        # aren't in the interVPU table
        out = np.setdiff1d(flow.FROMCOMID.values, fls.COMID.values)
        out = out[
            np.nonzero(out)
        ]  # this should be what combines zones and above^, but we force connections with inter_tbl
        flow = flow[
            ~flow.FROMCOMID.isin(np.setdiff1d(out, inter_tbl.thruCOMIDs.values))
        ]
        # Table is ready for processing and flow connection dict can be created
        flow_dict = defaultdict(list)
        for _, row in flow.iterrows():
            flow_dict[row.TOCOMID].append(row.FROMCOMID)
        # add IDs from UpCOMadd column if working in ToZone, forces the flowtable connection though not there
        for interLine in inter_tbl.values:
            if interLine[6] > 0 and interLine[2] == zone:
                flow_dict[int(interLine[6])].append(int(interLine[0]))
        out_of_vpus = inter_tbl.loc[
            (inter_tbl.ToZone == zone) & (inter_tbl.DropCOMID == 0)
        ].thruCOMIDs.values
        cats = dbf2DF(f"{pre}/NHDPlusCatchment/Catchment.dbf").set_index("FEATUREID")
        comids = cats.index.values
        comids = np.append(comids, out_of_vpus)
        # list of upstream lists, filter comids in all_comids
        ups = [list(all_comids.intersection(bastards(x, flow_dict))) for x in comids]
        lengths = np.array([len(u) for u in ups])
        upstream = np.hstack(ups).astype(np.int32)  # Convert to 1d vector
        assert len(ups) == len(lengths) == len(comids)
        np.savez_compressed(
            f"./accum_npy/accum_{zone}.npz",
            comids=comids,
            lengths=lengths,
            upstream=upstream,
        )


##############################################################################


def nhd_dict(nhd, unit="VPU"):
    """
    __author__ =  "Rick Debbout <debbout.rick@epa.gov>"
    Creates an OrderdDict for looping through regions of the NHD to carry
    InterVPU connections across VPU/RPU zones

    Defaults to VPU

    Note: Hawaii and the Carribean Island zones are removed

    Arguments
    ---------
    nhd             : the directory contining NHDPlus data
    unit            : Vector or Raster processing units 'VPU' or 'RPU'
    """

    inputs = OrderedDict()
    bounds = dbf2DF(f"{nhd}/NHDPlusGlobalData/BoundaryUnit.dbf")
    remove = bounds.loc[bounds.DRAINAGEID.isin(["HI", "CI"])].index
    bounds = bounds.drop(remove, axis=0)
    if unit == "VPU":
        vpu_bounds = bounds.loc[bounds.UNITTYPE == "VPU"].sort_values(
            "HYDROSEQ", ascending=False
        )
        for idx, row in vpu_bounds.iterrows():
            inputs[row.UNITID] = row.DRAINAGEID
        np.save("./accum_npy/vpu_inputs.npy", inputs)
        return inputs

    if unit == "RPU":
        rpu_bounds = bounds.loc[bounds.UNITTYPE == "RPU"]
        for _, row in rpu_bounds.iterrows():
            hr = row.DRAINAGEID
            rpu = row.UNITID
            for root, _, _ in os.walk(f"{nhd}/NHDPlus{hr}"):
                if rpu in root:
                    zone = os.path.split(os.path.split(root)[0])[0][-2:]
            if not zone in inputs.keys():
                inputs[zone] = []
            inputs[zone].append(row.UNITID)
        np.save("./accum_npy/rpu_inputs.npy", inputs)
        return inputs


##############################################################################


def findUpstreamNpy(zone, com, numpy_dir):
    """
    __author__ =  "Rick Debbout <debbout.rick@epa.gov>"
    Finds upstream array of COMIDs for any given catchment COMID

    Arguments
    ---------
    zone                  : string of an NHDPlusV2 VPU zone, i.e. 10L, 16, 17
    com                   : COMID of NHD Catchment, integer
    numpy_dir             : directory where .npy files are stored
    """
    accum = np.load(numpy_dir + "/accum_" + zone + ".npz")
    comids = accum["comids"]
    lengths = accum["lengths"]
    upStream = accum["upstream"]
    itemindex = int(np.where(comids == com)[0])
    n = lengths[:itemindex].sum()
    arrlen = lengths[itemindex]
    return upStream[n : n + arrlen]


def dbf2DF(f, upper=True):
    data = gpd.read_file(f).drop("geometry", axis=1)
    if upper is True:
        data.columns = data.columns.str.upper()
    return data
