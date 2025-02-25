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

class LicenseError(Exception):
    pass

class ZonalOperations:
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
            arcpy.env.cellSize = "30"
            arcpy.env.snapRaster = inZoneData
            if by_RPU == 0:
                if LandscapeLayer.count(".tif") or LandscapeLayer.count(".img"):
                    landscape_layer = Path(LandscapeLayer).stem  # / vs. \ agnostic
                    outTable = "%s/DBF_stash/zonalstats_%s%s%s.dbf" % (
                        out_dir,
                        landscape_layer,
                        appendMetric,
                        zone,
                    )
                else:
                    landscape_layer = Path(LandscapeLayer).name  # / vs. \ agnostic
                    outTable = "%s/DBF_stash/zonalstats_%s%s%s.dbf" % (
                        out_dir,
                        landscape_layer,
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
                    table = dbf2df(outTable)
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
                        ZonalStatisticsAsTable(
                            inZoneData, "VALUE", elev, outTable, "DATA", "ALL"
                        )
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