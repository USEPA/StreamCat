# Functions for zonal stats
import pandas as pd 
import numpy as np
import os 
from pathlib import Path 
import arcpy
from arcpy.sa import TabulateArea, ZonalStatisticsAsTable
import gdal
import time 
import fiona
from utils import dbf2df
import rioxarray
import rasterio
from xrspatial.zonal import stats, crosstab

class LicenseError(Exception):
    pass

class ZonalOperations:
    def __init__(self, use_arcpy=False, num_workers=None):
        self.use_arcpy = use_arcpy
        self.num_workers = num_workers
        self.use_dask = False

        if use_arcpy and num_workers:
            self.use_dask = True
    @staticmethod
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


    def chkColumnLength(self, table, landscape_layer):
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
            rat_cols = self.get_rat_vals(landscape_layer)
        else:
            rat_cols = dbf2df(rat_file).VALUE.tolist()
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
    
    def xarrayZonalStatsPrep(self, izd_path, landscape_layer_path):
        """Create Xarray DataArrays from inZoneData and LandscapeLayer files
    
        Args:
            izd_path (str): path to inZoneData dbf
            landscape_layer_path (str): path to landscape layer raster
    
        Returns:
            izd_array (xr.DataArray): DataArray of zone data
            ll_array (xr.DataArray): Windowed read of landscape layer raster as DataArray
        """
    
        # Load first band of in zone data to an xarray DataArray
        # print(use_dask)
        if self.use_dask:
            izd_array = rioxarray.open_rasterio(izd_path, chunks="auto").sel(band=1).drop_vars('band')
        else:
            izd_array = rioxarray.open_rasterio(izd_path).sel(band=1).drop_vars('band')
    
        # Get transform and bounds from in zone data to create window
        transform = izd_array.rio.transform()
        bounds = izd_array.rio.bounds()
        window = rasterio.windows.from_bounds(*bounds, transform)
    
        # Read window of Landscape Layer (band 1) to rasterio array (numpy ndarray)
        # Notes:
        # Used rasterio because windowed reading with rioxarray was not working.
        # Also attempted to use rioxarray and rio.clip & mask however this took 5-6 minutes for the NE region so was a huge slowdown.
        # rasterio window read then DataArray conversion is simplest and fastest and does not require us loading a new GeoDataFrame for a shapely box.
        #with rasterio.open(landscape_layer_path) as src:
            # Open LandscapeLayer raster, window read band 1
            #ll_rio_array = src.read(1, window=window)
        # Convert numpy array to xarray DataArray with x and y as the dimensions to match izd_array
        #ll_array = xr.DataArray(ll_rio_array, dims=['y', 'x'])
    
        # Rioxarray window selection is much faster than rasterio
        ll_array = rioxarray.open_rasterio(landscape_layer_path).sel(band=1).drop_vars('band')
        ll_array = ll_array.rio.isel_window(window, pad=True)
        
        # TODO add check to make sure they are the same size
        if izd_array.shape != ll_array.shape:
            # Determine the target shape (you can choose either one, but here we'll use izd_array's shape)
            target_shape = izd_array.shape
    
            # Resample or pad the ll_array to match the target shape
            if ll_array.shape[0] < target_shape[0]:
                ll_array = ll_array.pad(y=(0, target_shape[0] - ll_array.shape[0]), mode='constant', constant_values=np.nan)
            elif ll_array.shape[0] > target_shape[0]:
                ll_array = ll_array.isel(y=slice(0, target_shape[0]))
    
            if ll_array.shape[1] < target_shape[1]:
                ll_array = ll_array.pad(x=(0, target_shape[1] - ll_array.shape[1]), mode='constant', constant_values=np.nan)
            elif ll_array.shape[1] > target_shape[1]:
                ll_array = ll_array.isel(x=slice(0, target_shape[1]))
    
        # Return the DataArrays to use in xrspatial.zonal.stats, and xrspatial.zonal.crosstab
        if self.use_dask:
            ll_array = ll_array.chunk(izd_array.chunksizes)

        return izd_array, ll_array
    
    def createCatStats(
        self,
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
            if self.use_arcpy:
                arcpy.env.cellSize = "30"
                arcpy.env.snapRaster = inZoneData
                ext = 'dbf'
            else:
                ext = 'csv'
            if by_RPU == 0:
                if LandscapeLayer.count(".tif") or LandscapeLayer.count(".img"):
                    landscape_layer = Path(LandscapeLayer).stem  # / vs. \ agnostic
                    
                    outTable_path = "%s/DBF_stash/zonalstats_%s%s%s.%s" % (
                        out_dir,
                        landscape_layer,
                        appendMetric,
                        zone,
                        ext
                    )
                else:
                    landscape_layer = Path(LandscapeLayer).name  # / vs. \ agnostic
                    outTable_path = "%s/DBF_stash/zonalstats_%s%s%s.%s" % (
                        out_dir,
                        landscape_layer,
                        appendMetric,
                        zone,
                        ext
                    )
                if not os.path.exists(outTable_path):
                    if accum_type == "Categorical":
                        if self.use_arcpy:
                            TabulateArea(
                                inZoneData, "VALUE", LandscapeLayer, "Value", outTable, "30"
                            )
                        else:
                            izd_array, ll_array = self.xarrayZonalStatsPrep(inZoneData, LandscapeLayer)
                            outTable = crosstab(izd_array, ll_array)

                    if accum_type == "Continuous":
                        if self.use_arcpy:
                            ZonalStatisticsAsTable(
                                inZoneData, "VALUE", LandscapeLayer, outTable, "DATA", "ALL"
                            )
                        else:
                            izd_array, ll_array = self.xarrayZonalStatsPrep(inZoneData, LandscapeLayer)
                            outTable = stats(izd_array, ll_array)
                    
                    # Call compute before building up too large of a dask graph if using dask
                    if self.use_dask:
                        outTable = outTable.compute()
                    
                    # Post process pandas dataframe
                    if "Unnamed: 0" in outTable.columns:
                        outTable = outTable.drop("Unnamed: 0", axis=1)
                    outTable = outTable[outTable.zone != -2147483647]
                    outTable = outTable.round(2)

                    # Memeory cleanup
                    del izd_array 
                    del ll_array
                try:
                    if self.use_arcpy:
                        table = dbf2df(outTable)
                    else:
                        outTable.to_csv(outTable_path)
                except fiona.errors.DriverError as e:
                    # arc occassionally doesn't release the file and fails here
                    print(e, "\n\n!EXCEPTION CAUGHT! TRYING AGAIN!")
                    time.sleep(60)
                    table = dbf2df(outTable)
            if by_RPU == 1:
                hydrodir = "/".join(inZoneData.split("/")[:-2]) + "/NEDSnapshot"
                rpuList = []
                for subdirs in os.listdir(hydrodir):
                    elev = "%s/%s/elev_cm" % (hydrodir, subdirs)
                    rpuList.append(subdirs[-3:])
                    print("working on " + elev)
                    outTable = out_dir + "/DBF_stash/zonalstats_elev%s.dbf" % (subdirs[-3:])
                    if not os.path.exists(outTable):
                        if self.use_arcpy:
                            ZonalStatisticsAsTable(
                                inZoneData, "VALUE", elev, outTable, "DATA", "ALL"
                            )
                        else:
                            izd_array, elev_array = self.xarrayZonalStatsPrep(inZoneData, elev)
                            outTable = stats(izd_array, elev_array)
                            if self.use_dask:
                                outTable = outTable.compute()
                                del izd_array
                                del elev_array
                for count, rpu in enumerate(rpuList):
                    if count == 0:
                        table = dbf2df(f"{out_dir}/DBF_stash/zonalstats_elev{rpu}.dbf")
                    else:
                        table = pd.concat(
                            [
                                table,
                                dbf2df(f"{out_dir}/DBF_stash/zonalstats_elev{rpu}.dbf"),
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
            nhdtbl = dbf2df(
                f"{NHD_dir}/NHDPlus{hydroregion}/NHDPlus{zone}"
                "/NHDPlusCatchment/Catchment.dbf"
            ).loc[:, ["FEATUREID", "AREASQKM", "GRIDCODE"]]
            tbl = dbf2df(outTable)
            if accum_type == "Categorical":
                tbl = self.chkColumnLength(tbl, LandscapeLayer)
            # We need to use the raster attribute table here for PctFull & Area
            # TODO: this needs to be considered when making masks!!!
            tbl2 = dbf2df(f"{mask_dir}/{zone}.tif.vat.dbf")
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
                table = self.chkColumnLength(table, LandscapeLayer)
                table["AREA"] = table[table.columns.tolist()[1:]].sum(axis=1)
            nhdTable = dbf2df(inZoneData[:-3] + "Catchment.dbf").loc[
                :, ["FEATUREID", "AREASQKM", "GRIDCODE"]
            ]
            nhdTable = nhdTable.rename(
                columns={"FEATUREID": "COMID", "AREASQKM": "AreaSqKm"}
            )
            result = pd.merge(
                nhdTable, table, how="left", left_on="GRIDCODE", right_on="VALUE"
            )
            if LandscapeLayer.split("/")[-1].split(".")[0] == "rdstcrs":
                slptbl = dbf2df(
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