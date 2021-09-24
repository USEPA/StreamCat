# -*- coding: utf-8 -*-
"""
Created on Thu Jun 23 11:49:24 2016
                      _           _        _
 _______  _ __   __ _| |      ___| |_ __ _| |_ ___
|_  / _ \| '_ \ / _` | |_____/ __| __/ _` | __/ __|
 / / (_) | | | | (_| | |_____\__ \ || (_| | |_\__ \
/___\___/|_| |_|\__,_|_|     |___/\__\__,_|\__|___/

This script will use the tifs that are generated with the georasters package
below to loop through and perform zonal statistics or tabulate area on each tif
in the given directory that they are made. The output dbfs are then stacked to
make a single table containing data for each split-catchment that can then be
appended with StreamCat data to calculate site-specific watershed output
written out at the end of the script.

@author: Rdebbout
"""
import os
import sys
import time
import glob

os.environ["PATH"] += r";C:\Program Files\ArcGIS\Pro\bin"
sys.path.append(r"C:\Program Files\ArcGIS\Pro\Resources\ArcPy")

from tkinter import Tk, messagebox
from tkinter.filedialog import askopenfilename

import arcpy
import geopandas as gpd
import georasters as gr
import numpy as np
import pandas as pd
from geopandas.tools import sjoin


sys.path.append("E:/projects/StreamCat")  # TODO: these functions should be joined into a single repo

from StreamCat_functions import chkColumnLength  # noqa

arcpy.CheckOutExtension("spatial")
from arcpy.sa import TabulateArea, ZonalStatisticsAsTable  # noqa

arcpy.env.cellSize = "30"
Tk().withdraw()

if len(sys.argv) == 1:
    control_file = askopenfilename(
        title="Select the control table ",
        filetypes=[("Acceptable Files", ("*.csv")), ("All Files", "*.*")],
        initialdir=os.path.expanduser("~"),
    )
else:
    control_file = sys.argv[1]

ctl = pd.read_csv(control_file)
sys.path.append(os.path.dirname(control_file))

os.chdir(os.path.dirname(control_file))

from settings import masks, nhd, pt_file, stream_cat, uid  # noqa

if not os.path.exists(nhd):
    print("'nhd' path is incorrect in settings.py")
    sys.exit()
if not os.path.exists(stream_cat):
    print("'stream_cat' path is incorrect in settings.py")
    sys.exit()
###############################################################################

home = os.path.dirname(pt_file)
ingrid_dir = stream_cat + "/LandscapeRasters/QAComplete"
upDir = stream_cat + "/Allocation_and_Accumulation"
out_dir = home + "/out"
if not os.path.exists(out_dir):
    os.mkdir(out_dir)
dbf_dir = out_dir + "/DBF_stash"
if not os.path.exists(dbf_dir):
    os.mkdir(dbf_dir)
pts = gpd.read_file(pt_file)
settings_file = os.path.dirname(home) + "/settings.py"


def arc_table_stats(accum_type, inZoneData, valras, temp, **kwargs):
    """this is necessary to garbage collect the .lock file that seems
    to keep tripping up the `os.remove` on the cpg and xml files"""
    if accum_type == "Categorical":
        result = TabulateArea(
            inZoneData, "Value", valras, "Value", temp, "30", **kwargs
        )
    if accum_type == "Continuous":
        result = ZonalStatisticsAsTable(
            inZoneData, "Value", valras, temp, "DATA", "ALL", **kwargs
        )
    del result # useful to remove .lock on resource


for line in range(len(ctl.values)):
    if ctl.run[line] == 1:
        ftn = ctl.FullTableName[line]
        print(f"running {ftn}")
        accum_type = ctl.accum_type[line]
        metric_type = ctl.MetricType[line]
        a_m = str(ctl.AppendMetric[line])
        if a_m == "nan":
            a_m = ""
        # point to masked splits if there is an append metric...
        # this will mean that the folder that masked splits are in MUST
        # be named w/ the appendMetric title!!!
        split_path = "%s/splits/%s" % (home, a_m if a_m else "split_full")
        if not os.path.exists(split_path):
            print(f"making masked splits at the following path: {split_path}")
            os.mkdir(split_path)
            from uidINtif import makeMaskTIFs

            mask_ras = masks.get(a_m)
            if not mask_ras:
                msg = (
                    "The AppendMetric value of the mask that you are using"
                    " does not exist in the masks dictionary in settings.py"
                )
                messagebox.showwarning("Warning", msg)
                print("Please add mask to settings.py before running.")
            full_split_path = f"{home}/splits/split_full"
            makeMaskTIFs(full_split_path, split_path, mask_ras)

        conversion = float(ctl.Conversion[line])
        metricName = ctl.MetricName[line]
        stats_stash = f"{dbf_dir}/{ftn}"
        if not os.path.exists(stats_stash) and accum_type != "Point":
            # no imtermediate data is used when running Point metrics!
            os.mkdir(stats_stash)
        if accum_type == "Point":  # make 'splitCat_polys' in the tkinter script
            value_pts = gpd.GeoDataFrame.from_file(
                f"{ingrid_dir}/{ctl.LandscapeLayer[line]}"
            )
            prefix = a_m if a_m else "full"
            shp_file = f"split_cat/splits/{prefix}_splits.shp"
            if not os.path.exists(shp_file):
                from make_split_shapes import make_split_shapes

                make_split_shapes(shp_file, split_path, uid, a_m, mask=None)

            polys = gpd.GeoDataFrame.from_file(shp_file)
            polys["CatAreaSqKm" + a_m] = polys.area * 1e-6
            if not value_pts.crs == polys.crs:
                value_pts = value_pts.to_crs(polys.crs)
            value_pts["latlon_tuple"] = zip(
                value_pts.geometry.map(lambda point: point.x),
                value_pts.geometry.map(lambda point: point.y),
            )
            value_pts2 = value_pts.drop_duplicates("latlon_tuple")
            try:
                point_poly_join = sjoin(value_pts2, polys, how="left", op="within")
            except:  # noqa
                # no intersection error with v. of pandas
                # https://github.com/geopandas/geopandas/issues/731
                # happened w/ NPDESrp100 WSA_WEST splits
                yowza = (
                    value_pts2.columns.tolist()  # noqa
                    + ["index_right"]  # noqa
                    + polys.drop("geometry", axis=1).columns.tolist()  # noqa
                )  # noqa
                point_poly_join = gpd.GeoDataFrame(columns=yowza)
            grouped = point_poly_join.groupby(uid)
            point_poly_count = grouped[uid].count()
            final = polys.join(point_poly_count, on=uid, rsuffix="_", how="left")
            final = final[[uid, "CatAreaSqKm" + a_m, uid + "_"]].fillna(0)
            final.rename({uid + "_": "CatCount" + a_m}, axis=1, inplace=True)
            if isinstance(ctl.summaryfield[line], str):
                point_poly_dups = sjoin(value_pts, polys, how="left", op="within")
                grouped2 = point_poly_dups.groupby(uid)
                for x in ctl.summaryfield[line].split(";"):
                    point_poly_stats = grouped2[x].sum()
                    sumtbl = polys.join(point_poly_stats, on=uid)[[uid, x]]
                    sumtbl.columns = [uid, "Cat" + x]
                    final = pd.merge(final, sumtbl, on=uid, how="left").fillna(0)
            final["CatPctFull" + a_m] = 100
        if accum_type in ["Categorical", "Continuous"]:
            mask = a_m
            a_m = "" if a_m and a_m in ["Slp20", "Slp10"] else a_m
            final = pd.DataFrame()
            for idx, row in pts.loc[pts.flowdir != "Uninitialized"].iterrows():
                fn = f"{row[uid]}.tif"
                inZoneData = f"{split_path}/{fn}"
                if not os.path.exists(inZoneData):
                    # retain id in final, not all splits intersect with masks
                    empty_row = {uid: row[uid]}
                    cols = [uid]
                    tbl = pd.DataFrame(empty_row, index=[0], columns=cols)
                    final = pd.concat([final, tbl], sort=False)
                    continue
                if ctl.ByRPU[line] == 1:
                    valras = (
                        f"{nhd}/NHDPlus{row.DrainageID}/NHDPlus{row.VPU}"
                        f"/NEDSnapshot/ned{row.RPU}/elev_cm"
                    )
                if ctl.ByRPU[line] == 0:
                    valras = f"{ingrid_dir}/{ctl.LandscapeLayer[line]}"
                out = f"{stats_stash}/Zstats_{row[uid]}.dbf"
                if not os.path.exists(out):
                    waiting = 0
                    while waiting < 10:
                        try:
                            arc_table_stats(accum_type, inZoneData, valras, out)
                            waiting = 47
                        except arcpy.ExecuteError as e:
                            tm = arcpy.GetMessages(0).split("\n")[-1].split(" ")[2:7]
                            print(f"Arc Fail: attempt {waiting} |", " ".join(tm))
                            time.sleep(3)
                            waiting += 1
                            continue

                tbl = gpd.GeoDataFrame.from_file(out)  # !!! use dbf2DF
                val_name = "VALUE" if "VALUE" in tbl.columns else "Value"
                # below fails when DBF is written out blank??
                try:
                    tbl = tbl.astype({val_name: pts[uid].dtype})
                except KeyError as e:
                    print(f"UID: {row[uid]} | outfile: \n\t{out}")
                    raise e
                tbl.at[0, val_name] = row[uid]  # VALUE when read in is 1
                ras = gr.from_file(inZoneData)
                zone_count = ras.count()
                AreaSqKM = (zone_count * 900) * 1e-6
                if accum_type == "Categorical":
                    tbl = chkColumnLength(tbl, valras)
                    tbl.insert(1, "AreaSqKm", [AreaSqKM])
                    cols = tbl.columns.tolist()[2:]
                    tbl["TotCount"] = tbl[cols].sum(axis=1)
                    tbl["PctFull"] = (
                        (tbl.TotCount * 1e-6) / AreaSqKM
                    ) * 100  # work here
                    tbl = tbl[[val_name, "AreaSqKm"] + cols + ["PctFull"]]
                    tbl.columns = (
                        [uid, "CatAreaSqKm" + a_m]
                        + ["Cat" + y + a_m for y in cols]
                        + ["CatPctFull" + a_m]
                    )
                if accum_type == "Continuous":
                    tbl.insert(1, "AreaSqKm", [AreaSqKM])
                    tbl["PctFull"] = (tbl.COUNT / zone_count) * 100
                    if ctl.ByRPU[line] == 1:
                        tbl = tbl[
                            [
                                val_name,
                                "AreaSqKm",
                                "COUNT",
                                "SUM",
                                "MAX",
                                "MIN",
                                "PctFull",
                            ]
                        ]
                    if ctl.ByRPU[line] == 0:
                        tbl = tbl[
                            [val_name, "AreaSqKm", "COUNT", "SUM", "PctFull"]
                        ].copy()
                    cols = [
                        "Cat" + y[0] + y[1:] + a_m
                        if "MAX" in y or "MIN" in y
                        else "Cat" + y[0] + y[1:].lower() + a_m
                        for y in tbl.columns.tolist()[2:-1]
                    ]
                    tbl.columns = (
                        [uid, "CatAreaSqKm" + a_m] + cols + ["CatPctFull" + a_m]
                    )  # .lower() to match names with StreamCat
                # stack to complete table and add pct full with areas
                final = pd.concat([final, tbl], sort=False)
        #            final[uid] = final[uid].astype(np.int64)
        digits = pts.loc[
            pts.flowdir != "Uninitialized", [uid, "cat_comid"]
        ]  # .astype(np.int64)
        final = pd.merge(digits, final, how="left", on=uid)

        uninit = pd.DataFrame()
        for zone in pts.loc[pts.flowdir == "Uninitialized"].VPU.unique():
            uninit_get = pd.read_csv(f"{upDir}/{ftn}_{zone}.csv")
            coms = pts.loc[
                (pts["VPU"] == zone) & (pts["flowdir"] == "Uninitialized")
            ].cat_comid
            uninit_get = uninit_get.loc[uninit_get.COMID.isin(coms)]
            uninit = uninit.append(uninit_get)

        keep = [col for col in uninit.columns if "Ws" not in col]
        uninit = uninit[keep]
        columns = [x for x in uninit.columns if "Cat" == x[:3]]

        for col in columns:
            if col in ["CatAreaSqKm" + a_m]:
                uninit["Up" + col] = 0
            else:
                uninit["Up" + col] = np.nan
        uninit.rename(columns={"COMID": "cat_comid"}, inplace=True)
        uninit = pd.merge(
            pts.loc[pts.flowdir == "Uninitialized", [uid, "cat_comid"]],
            uninit,
            on="cat_comid",
            how="right",
        )
        #        final['CatPctFull'].fillna(0,inplace=True)
        # replace the NA below later?? what happens if we don't do this?

        if "RdCrs" in metricName:
            slpAll = pd.DataFrame()
            iterator = pts[["DrainageID", "VPU"]].drop_duplicates().iterrows()
            for _, row in iterator:
                slp = (
                    f"{nhd}/NHDPlus{row.DrainageID}/NHDPlus{row.VPU}"
                    f"/NHDPlusAttributes/elevslope.dbf"
                )
                slp_cols = ["COMID", "SLOPE"]
                slptbl = gpd.GeoDataFrame.from_file(slp).loc[:, slp_cols]
                slptbl = slptbl.loc[slptbl.COMID.isin(pts.cat_comid)]
                slptbl.loc[slptbl["SLOPE"] == -9998.0, "SLOPE"] = 0
                slpAll = pd.concat([slpAll, slptbl.copy()])

            final = pd.merge(
                final, slpAll, left_on="cat_comid", right_on="COMID", how="left"
            )
            final.SLOPE = final.SLOPE.fillna(0)
            final["CatSlpWtd"] = final["CatSum"] * final["SLOPE"].astype(float)
            final = final.drop(["COMID", "SLOPE"], axis=1)

        #        print "Gathering StreamCat upstream data..."
        addFinal = pd.DataFrame()
        for zone in pts.loc[pts.flowdir != "Uninitialized"].VPU.unique():
            add = pd.read_csv(f"{upDir}/{ftn}_{zone}.csv")
            add = add.loc[add.COMID.isin(pts.cat_comid)]
            columns = [x for x in add.columns if "Up" in x]
            add = add[["COMID"] + columns]
            addFinal = pd.concat([addFinal, add])
        assert len(addFinal) == len(pts.cat_comid.unique())
        # addFinal includes Uninitialized, but will be overwritten in 3 lines
        r = pd.merge(final, addFinal, left_on="cat_comid", right_on="COMID")
        r = r[uninit.columns.tolist()]
        r = r.append(uninit)
        c_area = "CatAreaSqKm" + a_m
        u_area = "UpCatAreaSqKm" + a_m
        w_area = "WsAreaSqKm" + a_m
        c_cnt = "CatCount" + a_m
        u_cnt = "UpCatCount" + a_m
        w_cnt = "WsCount" + a_m
        c_sum = "CatSum" + a_m
        u_sum = "UpCatSum" + a_m
        w_sum = "WsSum" + a_m
        c_pfl = "CatPctFull" + a_m
        u_pfl = "UpCatPctFull" + a_m
        w_pfl = "WsPctFull" + a_m
        r[w_area] = r[c_area].fillna(0) + r[u_area].fillna(0)
        if accum_type == "Continuous":
            cnt = r[c_cnt].fillna(0) + r[u_cnt].fillna(0)
            r[w_cnt] = cnt
            tot = r[c_sum].fillna(0) + r[u_sum].fillna(0)
            r[w_sum] = tot
            if ctl.ByRPU[line] == 1:
                top = r.loc[:, ["CatMAX" + a_m, "UpCatMAX" + a_m]].max(axis=1)
                r["WsMAX" + a_m] = top
                btm = r.loc[:, ["CatMIN" + a_m, "UpCatMIN" + a_m]].min(axis=1)
                r["WsMIN" + a_m] = btm
            r[w_pfl] = (
                (r[c_area].fillna(0) * r[c_pfl].fillna(0))
                + (r[u_area].fillna(0) * r[u_pfl].fillna(0))
            ) / (r[c_area].fillna(0) + r[u_area].fillna(0))
            nanz = r.loc[:, w_pfl] == 0
            r.loc[nanz, [w_cnt, w_sum]] = np.nan
            if metric_type == "Density":
                r[metricName + "Ws" + a_m] = (
                    r[w_sum] / r[w_area] * (r[w_pfl] / 100)
                ) * conversion
            else:
                r[metricName + "Ws" + a_m] = (r[w_sum] / r[w_cnt]) * conversion
            if metricName == "RdCrs":
                r["RdCrsSlpWtdWs"] = r.CatSlpWtd.fillna(0) + r.UpCatSlpWtd.fillna(0)
            if ctl.ByRPU[line] == 1:
                r["ELEV_WS_MAX" + a_m] = (
                    r.loc[:, ["CatMAX" + a_m, "UpCatMAX" + a_m]].max(axis=1)
                    * conversion
                )
                r["ELEV_WS_MIN" + a_m] = (
                    r.loc[:, ["CatMIN" + a_m, "UpCatMIN" + a_m]].min(axis=1)
                    * conversion
                )
            r[ftn + "_WS_PctFull" + a_m] = r[w_pfl]
        if accum_type == "Categorical":
            look_file = f"{stream_cat}/ControlTables/{ctl.MetricName[line]}"
            lookup = pd.read_csv(look_file)
            for name in lookup.raster_val.values:
                r["Ws" + name + a_m] = r["Cat" + name + a_m].fillna(0) + r[
                    "UpCat" + name + a_m
                ].fillna(0)
            columns = ["Ws" + g + a_m for g in lookup.raster_val]
            r["TotCount" + a_m] = r[columns].sum(axis=1) * 1e-6
            pfull = r["TotCount" + a_m] / r[w_area]
            r[w_pfl] = pfull * 100
            r = r.drop("TotCount" + a_m, axis=1)
            for idx in range(len(lookup)):
                r["Pct" + lookup.final_val[idx] + "Ws" + a_m] = (
                    (r["Ws" + lookup.raster_val[idx] + a_m] * 1e-6) / r[w_area]
                ) * 100
            r[ftn + "_WS_PctFull" + a_m] = r[w_pfl]
        if accum_type == "Point":
            r[w_cnt] = r[c_cnt].fillna(0) + r[u_cnt].fillna(0)
            if isinstance(ctl.summaryfield[line], str):
                for x in ctl.summaryfield[line].split(";"):
                    r["Ws" + x] = r["Cat" + x].fillna(0) + r["UpCat" + x].fillna(0)
            r[w_pfl] = (
                (r[c_area].fillna(0) * r[c_pfl].fillna(0))
                + (r[u_area].fillna(0) * r[u_pfl].fillna(0))
            ) / (r[c_area].fillna(0) + r[u_area].fillna(0))
            r[metricName + "Ws" + a_m] = (r[w_cnt] / r[w_area]) * conversion
            if isinstance(ctl.summaryfield[line], str):
                for x in ctl.summaryfield[line].split(";"):
                    title = metricName[:-4] + x.replace("M3", "") + "Ws"
                    r[title] = r["Ws" + x] / (r[w_area] * (r[w_pfl] / 100))
            r[ftn + "_WS_PctFull" + a_m] = r[w_pfl]
            r.loc[(r[w_area] == 0), r.columns[-2:]] = np.nan
        r.to_csv(f"{out_dir}/{ftn}.csv", index=False)
        [os.unlink(f) # delete all .xml an .cpg files
                for ext in ["*.cpg","*.xml"]
                for f in glob.glob(f"{stats_stash}/{ext}")]
        print(f"Done with {ftn} length: {len(r)}")
