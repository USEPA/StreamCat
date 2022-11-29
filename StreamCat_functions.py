"""       __                                       __
    _____/ /_________  ____  ____ ___  _________ _/ /_
   / ___/ __/ ___/ _ \/ __ `/ __ `__ \/ ___/ __ `/ __/
  (__  ) /_/ /  /  __/ /_/ / / / / / / /__/ /_/ / /_
 /____/\__/_/   \___/\__,_/_/ /_/ /_/\___/\__,_/\__/

 Functions for allocating landscape metrics for catchments, accumulating metrics 
 for upstream catchments, andwriting final landscape metric tables

 Authors: Marc Weber<weber.marc@epa.gov>
          Ryan Hill<hill.ryan@epa.gov>
          Rick Debbout<debbout.rick@epa.gov>


 Date: March 2022
"""

import os
import sys
import time
from collections import OrderedDict, defaultdict, deque
from typing import Generator

import numpy as np
import pandas as pd

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

def bastards(token, tree):
    """
    __author__ = "Marc Weber <weber.marc@epa.gov>"
                 "Ryan Hill <hill.ryan@epa.gov>"
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

##############################################################################

def Accumulation(tbl, IDs, lengths, upstream, tbl_type, icol="IDs"):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Ryan Hill <hill.ryan@epa.gov>"
    Uses the 'Cat' and 'UpCat' columns to caluculate watershed values and returns those values in 'Cat' columns
        so they can be appended to 'CatResult' tables in other zones before accumulation.

    Arguments
    ---------
    arr                   : table containing watershed values
    IDs                   : numpy array of all zones IDs
    lengths               : numpy array with lengths of upstream IDs
    upstream              : numpy array of all upstream arrays for each ID
    tbl_type              : string value of table metrics to be returned
    icol                  : column in arr object to index
    """
    # RuntimeWarning: invalid value encountered in double_scalars
    np.seterr(all="ignore")
    IDs = tbl[icol].values.astype('float64')  # Read in IDs
    indices = swapper(IDs, upstream)  # Get indices that will be used to map values
    del upstream  # a and indices are big - clean up to minimize RAM
    cols = tbl.columns[1:]  # Get column names that will be accumulated
    z = np.zeros(IDs.shape)  # Make empty vector for placing values
    data = np.zeros((len(IDs), len(tbl.columns)))
    data[:, 0] = IDs  # Define first column as comids
    accumulated_indexes = np.add.accumulate(lengths)[:-1]
    # Loop and accumulate values
    for index, column in enumerate(cols, 1):
        col_values = tbl[column].values.astype("float")
        all_values = np.split(col_values[indices], accumulated_indexes)
        if tbl_type is "Ws":
            # add identity value to each array for full watershed
            all_values = np.array(
                [np.append(val, col_values[idx]) for idx, val in enumerate(all_values)],
                dtype=object,
            )

            # all_values = [np.append(val, col_values[idx]) for idx, val in enumerate(all_values)]

        if index is 1:
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
    data = data[np.in1d(data[:, 0], IDs), :]  # Remove the extra IDss
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
    nhd_dir):

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
    nhd_dir
    """

    try:
        arcpy.env.cellSize = "30"
        # arcpy.env.snapRaster = inZoneData
        if LandscapeLayer.count(".tif") or LandscapeLayer.count(".img"):
            outTable = "%s/DBF_stash/zonalstats_%s%s.dbf" % (
                out_dir,
                LandscapeLayer.split("/")[-1].split(".")[0],
                zone,
            )
        else:
            outTable = "%s/DBF_stash/zonalstats_%s%s.dbf" % (
                out_dir,
                LandscapeLayer.split("/")[-1],
                zone,
            )
        if not os.path.exists(outTable):
            if accum_type == "Categorical":
                TabulateArea(
                    inZoneData, "OBJECTID", LandscapeLayer, "Value", outTable, "30"
                )
            if accum_type == "Continuous":
                ZonalStatisticsAsTable(
                    inZoneData, "OBJECTID", LandscapeLayer, outTable, "DATA", "ALL"
                )
        try:
            table = dbf2DF(outTable)
        except fiona.errors.DriverError as e:
            # arc occassionally doesn't release the file and fails here
            print(e, "\n\n!EXCEPTION CAUGHT! TRYING AGAIN!")
            time.sleep(4)
            table = dbf2DF(outTable)
            
    except LicenseError:
        print("Spatial Analyst license is unavailable")
    except arcpy.ExecuteError:
        print("Failing at the ExecuteError!")
        print(arcpy.GetMessages(2))

    
    if accum_type == "Continuous":
        table = table[["OBJECTID", "AREA", "COUNT", "SUM"]]
        table = table.rename(columns={"COUNT": "Count", "SUM": "Sum"})
    if accum_type == "Categorical":
        table = chkColumnLength(table, LandscapeLayer)
        table["AREA"] = table[table.columns.tolist()[1:]].sum(axis=1)
    nhdTable = gpd.read_file(nhd_dir +'/NHDPlusHRVFGen' + zone + '_V5.gdb', 
                    driver='FileGDB', 
                    layer='NHDPlusCatchment')
    # calc area sqkm
    nhdTable = nhdTable.to_crs('EPSG:5070')
    nhdTable["AreaSqKm"] = nhdTable['geometry'].area/ 10**6
    nhdTable = nhdTable.rename(
        columns={"Gen_NHDPlusID": "NHDPlusID"})
    # ObjectID is a hidden field in nhdTable so add back in dummy variable
    nhdTable['GridCode'] = np.arange(len(nhdTable))+2
    result = pd.merge(
        nhdTable, table, how="left", left_on="GridCode", right_on="OBJECTID"
    )
    # if LandscapeLayer.split("/")[-1].split(".")[0] == "rdstcrs":
    #     slptbl = dbf2DF(
    #         "%s/NHDPlus%s/NHDPlus%s/NHDPlusAttributes/elevslope.dbf"
    #         % (NHD_dir, hydroregion, zone)
    #     ).loc[:, ["COMID", "SLOPE"]]
    #     slptbl.loc[slptbl["SLOPE"] == -9998.0, "SLOPE"] = 0
    #     result = pd.merge(result, slptbl, on="COMID", how="left")
    #     result.SLOPE = result.SLOPE.fillna(0)
    #     result["SlpWtd"] = result["Sum"] * result["SLOPE"]
    #     result = result.drop(["SLOPE"], axis=1)
    result["PctFull"] = (
        ((result.AREA * 1e-6) / result.AreaSqKm.astype("float")) * 100
    ).fillna(0)
    result = pd.DataFrame(result)
    result = result.drop(["GridCode", "geometry", "SHAPE_Length", "SHAPE_Area","OBJECTID", "Gen_AreaSQKM","AREA"], axis=1)
    cols = result.columns[1:]
    result.columns = np.append("NHDPlusID", "Cat" + cols.values)
    return result  # ALL NAs need to be filled w/ zero here for Accumulation!!




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



##############################################################################


def swapper(IDs, upStream):
    """
    __author__ =  "Marc Weber <weber.marc@epa.gov>"
                  "Ryan Hill <hill.ryan@epa.gov>"
    Creates array of indexes for all upstream COMIDs that will be summarized for each local catchment.

    Arguments
    ---------
    IDs                  : numpy array of all IDss in the zone
    upstream              : numpy array of all upstream IDss for each local catchment
    """
    bsort = np.argsort(IDs)
    apos = np.searchsorted(IDs[bsort], upStream)
    indices = bsort[apos]
    return indices


##############################################################################
def make_all_reg_IDs(nhd, regs):
    print("Making lookup table", end="", flush=True)
    lookup = {}
    for reg in regs:
        print(reg, end=", ", flush=True)
        cats = gpd.read_file(f"{nhd}/NHDPlusHRVFGen{reg}_V5.gdb", driver='FileGDB',
                             layer='NHDPlusCatchment',
                             ignore_fields=["GridCode","SHAPE_Length",
                                            "SHAPE_Area"]).drop("geometry", axis=1)
        cats[['REG']]=reg
        cats['Gen_NHDPlusID'] = cats['Gen_NHDPlusID'].astype('int64')
        cats = pd.Series(cats.REG.values,index=cats.Gen_NHDPlusID).to_dict()
        lookup.update(cats)
    print("...done!")
    return set(lookup) # RETURN A DICT!


def makeNumpyVectors(nhd, numpy_dir, regs):
    """
    Uses the NHD tables to create arrays of upstream catchments which are used
    in the Accumulation function

    Arguments
    ---------
    nhd         : directory where NHD is stored
    numpy_dir   : directory to create zipped numpy arrays
    regs        : NHD regions to process
    """
    os.mkdir(f"{numpy_dir}/accum_npy")
    # inputs = nhd_dict(nhd)
    all_reg_ids = make_all_reg_IDs(nhd, regs)
    print("Making numpy files specified inputs...", end="", flush=True)
    for zone in regs:
        print(zone, end=", ", flush=True)
        pre = f"{nhd}/NHDPlusHRVFGen{zone}_V5.gdb"
        flow = gpd.read_file(f"{pre}", driver="FileGDB", layer="NHDPlusFlow")
        
        flow = flow[(flow.ToNHDPID != 0) & (flow.FromNHDPID != 0)]
        fls = gpd.read_file(f"{pre}", driver="FileGDB", layer="NHDFlowline")
        coastfl = fls.NHDPlusID[fls.FType == 566]
        flow = flow[~flow.ToNHDPID.isin(coastfl.values)]
        out = np.setdiff1d(flow.FromNHDPID.values, fls.NHDPlusID.values)
        out = out[
            np.nonzero(out)
        ]  # this should be what combines zones and above^, but we force connections with inter_tbl
        # flow = flow[
        #     ~flow.FROMCOMID.isin(np.setdiff1d(out, inter_tbl.thruCOMIDs.values))
        # ]
        # Table is ready for processing and flow connection dict can be created
        flow_dict = defaultdict(list)
        for _, row in flow.iterrows():
            flow_dict[row.ToNHDPID].append(row.FromNHDPID)
        # add IDs from UpCOMadd column if working in ToZone, forces the flowtable connection though not there
        # for interLine in inter_tbl.values:
        #     if interLine[6] > 0 and interLine[2] == zone:
        #         flow_dict[int(interLine[6])].append(int(interLine[0]))
        # out_of_vpus = inter_tbl.loc[
        #     (inter_tbl.ToZone == zone) & (inter_tbl.DropCOMID == 0)
        # ].thruCOMIDs.values
        cats = gpd.read_file(f"{pre}",driver="FileGDB", layer="NHDPlusCatchment").set_index("Gen_NHDPlusID").drop("geometry", axis=1)
        IDs = cats.index.values
        # comids = np.append(comids, out_of_vpus)
        # list of upstream lists, filter comids in all_comids
        ups = [list(all_reg_ids.intersection(bastards(x, flow_dict))) for x in IDs]
        lengths = np.array([len(u) for u in ups])
        upstream = np.hstack(ups).astype(np.int32)  # Convert to 1d vector
        assert len(ups) == len(lengths) == len(IDs)
        np.savez_compressed(
            f"{numpy_dir}/accum_npy/accum_{zone}.npz",
            IDs=IDs,
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
