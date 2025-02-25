# Functions related to spatial / vector processing.
import os
import fiona
import numpy as np
import pandas as pd
import geopandas as gpd
from collections import defaultdict, deque
from geopandas.tools import sjoin
import rasterio
from typing import Generator
from utils import dbf2df

class SpatialOperations:
    @staticmethod
    def point_in_poly(points, vpu, catchments, pct_full, mask_dir, append_metric, summary=None):
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
        append_metric: str
            string to be appended to metrics from ControlTable_StreamCat.csv
        summary: list
            strings that identify columns from the attribute table in the points
            GeoDataFrame to be summed in returned DataFrame if `summary` is defined

        Returns
        ---------
        pd.DataFrame
            Table with count of spatial points in every catchment feature
            optionally with the summary of attributes from the points attribute
            table.
        """
        polys = gpd.read_file(catchments)
        polys.to_crs(points.crs, inplace=True)

        if mask_dir:
            rat = pd.read_csv(f"{mask_dir}/{vpu}.tif.vat.dbf")
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
            point_poly_join = sjoin(points2, polys, how="left", predicate="within")
            fld = "GRIDCODE"
        except Exception:
            polys["link"] = None
            point_poly_join = polys
            fld = "link"

        # Create group of all points in catchment
        grouped = point_poly_join.groupby("FEATUREID")
        point_poly_count = grouped[fld].count()
        point_poly_count.name = "COUNT"

        # Join Count column on to NHDCatchments table and keep only relevant columns
        final = polys.join(point_poly_count, on="FEATUREID", lsuffix="_", how="left")
        final = final[["FEATUREID", "AreaSqKM", "COUNT"]].fillna(0)
        cols = ["COMID", f"CatAreaSqKm{append_metric}", f"CatCount{append_metric}"]

        if summary:  # Summarize fields including duplicates
            point_poly_dups = sjoin(points, polys, how="left", predicate="within")
            grouped2 = point_poly_dups.groupby("FEATUREID")
            for x in summary:  # Sum the field in summary field list for each catchment
                point_poly_stats = grouped2[x].sum()
                point_poly_stats.name = x
                final = final.join(point_poly_stats, on="FEATUREID", how="left").fillna(0)
                cols.append(f"Cat{x}{append_metric}")
        final.columns = cols

        # Merge final table with Pct_Full table based on COMID and fill NA's with 0
        final = pd.merge(final, pct_full, on="COMID", how="left")
        if mask_dir:
            if summary:
                final.columns = (
                    ["COMID", "CatAreaSqKmRp100", "CatCountRp100"]
                    + [f"Cat{y}{append_metric}" for y in summary]
                    + ["CatPctFullRp100"]
                )
            else:
                final.columns = [
                    "COMID",
                    "CatAreaSqKmRp100",
                    "CatCountRp100",
                    "CatPctFullRp100",
                ]
        final[f"CatPctFull{append_metric}"] = final[f"CatPctFull{append_metric}"].fillna(100)
        for name in final.columns:
            if "AreaSqKm" in name:
                area = name
        final.loc[(final[area] == 0), final.columns[2:]] = None
        return final

    @staticmethod
    def mask_points(points, mask_dir, inputs, nodata_vals=[0, -2147483648.0]):
        """
        Filter points to those that only lie within the mask.

        Arguments
        ---------
        points: gpd.GeoDataFrame
            point GeoDataFrame to be filtered
        mask_dir: str
            path to folder holding masked rasters for every VPU
        inputs: collections.OrderedDict
            dictionary of vector processing units and hydroregions from NHDPlusV21
        nodata_vals: list
            values of the raster that exist outside of the mask zone

        Returns
        ---------
        gpd.GeoDataFrame
            filtered points that only lie within the masked areas
        """
        temp = pd.DataFrame(index=points.index)
        for zone, hydroregion in inputs.items():
            pts = SpatialOperations.get_raster_value_at_points(
                points, f"{mask_dir}/{zone}.tif", out_df=True
            )
            temp = temp.merge(~pts.isin(nodata_vals), left_index=True, right_index=True)
        xx = temp.sum(axis=1)
        return points.iloc[xx.loc[xx == 1].index]

    @staticmethod
    def get_raster_value_at_points(points, rasterfile, fieldname=None, val_name=None, out_df=False):
        """
        Find value at point (x,y) for every point in points of the given rasterfile.

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

    @staticmethod
    def shapefile_project(in_shp, out_shp, crs):
        """
        Reprojects a shapefile with Fiona.

        Arguments
        ---------
        in_shp: str
            input shapefile as a string, i.e., 'C:/Temp/inshape.shp'
        out_shp: str
            output shapefile as a string, i.e., 'C:/Temp/outshape.shp'
        crs: str
            the output CRS in Fiona format
        """
        with fiona.open(in_shp, "r") as source:
            sink_schema = source.schema.copy()
            sink_schema["geometry"] = "Point"

            with fiona.open(
                out_shp,
                "w",
                crs=crs,
                driver=source.driver,
                schema=sink_schema,
            ) as sink:
                for f in source:
                    sink.write(f)

    @staticmethod
    def upcom_dict(nhd, inter_vpu_tbl, zone):
        """
        Creates a dictionary of all catchment connections in a major NHDPlus basin.

        Arguments
        ---------
        nhd: str
            the directory containing NHDPlus data
        inter_vpu_tbl: pd.DataFrame
            table that holds the inter-VPU connections
        zone: str
            the zone being processed

        Returns
        ---------
        dict
            Dictionary of upstream COMIDs for each COMID in the zone.
        """
        flow = dbf2df(f"{nhd}/NHDPlusAttributes/PlusFlow.dbf")[["TOCOMID", "FROMCOMID"]]
        flow = flow[(flow.TOCOMID != 0) & (flow.FROMCOMID != 0)]
        fls = dbf2df(f"{nhd}/NHDSnapshot/Hydrography/NHDFlowline.dbf")
        coastfl = fls.COMID[fls.FTYPE == "Coastline"]
        flow = flow[~flow.FROMCOMID.isin(coastfl.values)]
        remove = inter_vpu_tbl.removeCOMs.values[inter_vpu_tbl.removeCOMs.values != 0]
        flow = flow[~flow.FROMCOMID.isin(remove)]
        out = np.setdiff1d(flow.FROMCOMID.values, fls.COMID.values)
        out = out[np.nonzero(out)]
        flow = flow[~flow.FROMCOMID.isin(np.setdiff1d(out, inter_vpu_tbl.thruCOMIDs.values))]
        fcom, tcom = flow.FROMCOMID.values, flow.TOCOMID.values
        up_coms = defaultdict(list)
        for i in range(len(flow)):
            from_comid = fcom[i]
            if from_comid == 0:
                continue
            else:
                up_coms[tcom[i]].append(from_comid)
        for inter_line in inter_vpu_tbl.values:
            if inter_line[6] > 0 and inter_line[2] == zone:
                up_coms[int(inter_line[6])].append(int(inter_line[0]))
        return up_coms