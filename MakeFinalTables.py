"""
Created on Jan 22, 2016
Process landscape layers through NHDPlusV21 framework with
control CSV using the `run` column to determine processing layers.
Assumes landscape layer in desired projection with appropriate
pre-processing to deal with any reclassing of values or recoding of
NA, and directories of NHDPlusV2 data installed in standard directory
format.

\b
examples:
    * `$ python StreamCat.py -c alt.csv`
    * `$ python StreamCat.py -c rel/path/alt.csv`
    * `$ python StreamCat.py -c /abs/path/alt.csv`
"""

import math
import os
import sys
import click
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from stream_cat_config import(
    # LOCAL_DIR,
    FINAL_DIR,
    ACCUM_DIR,
    LENGTHS,
    OUT_DIR
    )
control = "ControlTable_StreamCat.csv"


def build_stats(tbl, stats):
    if not stats:
        for c in tbl.columns.tolist():
            stats[c] = {"min": tbl[c].min(), "max": tbl[c].max()}
        return stats
    for col in tbl.columns.tolist():
        if tbl[col].min() < stats[col]["min"]:
            stats[col]["min"] = tbl[col].min()
        if tbl[col].max() > stats[col]["max"]:
            stats[col]["max"] = tbl[col].max()
    return stats


LENGTH_ERROR_MESSAGE = (
    "Table {} length vpu {} incorrect!!!!"
    "...check Allocation and Accumulation results"
)

OUT_DIR = Path(OUT_DIR)
FINAL_DIR = Path(FINAL_DIR)


ctl = pd.read_csv(control)
print(ctl.query("run == 1").MetricName.head())
#exit()

inputs = np.load(ACCUM_DIR + "/vpu_inputs.npy", allow_pickle=True).item()

runners = ctl.query("run == 1").groupby("Final_Table_Name")
tables = runners["FullTableName"].unique().to_dict()
# check that all accumulated files are present
missing = []
fn = "{}_{}.parquet"
for table, metrics in tables.items():  # make sure all tables exist
    for vpu in inputs:
        for metric in metrics:
            accumulated_file = OUT_DIR / fn.format(metric, vpu)
            if not accumulated_file.exists():
                missing = pd.concat([missing,accumulated_file], axis=0, ignore_index=False)

if len(missing) > 0:
    for miss in missing:
        print(f"Missing {miss.name}")
    print("Check output from StreamCat.py")
    sys.exit()

for table, metrics in tables.items():
    print(f"Running {table} .....into {FINAL_DIR}")
    out_file = FINAL_DIR / fn.format(table, vpu)
    final_file = FINAL_DIR / f"{table}.parquet"
    if not out_file.exists():
        for metric_count, metric in enumerate(metrics):
            idx = ctl.loc[ctl.FullTableName == metric].index.item()
            row = ctl.iloc[idx].copy()

            a_m = "" if row.AppendMetric == "none" else row.AppendMetric
            # Read in the StreamCat allocation and accumulation table
            tbl = pd.read_parquet(OUT_DIR / fn.format(metric, vpu))
            front_cols = [
                title
                for title in tbl.columns
                for x in ["COMID", "AreaSqKm", "PctFull"]
                if x in title and not "Up" in title
            ]
            _, catArea, catPct, wsArea, wsPct = front_cols
            # re-order for correct sequence
            front_cols = [front_cols[i] for i in [0, 1, 3, 2, 4]]

            # this protects summarization if the field is
            summaries = (
                row.summaryfield.split(";")
                if not str(row.summaryfield) == "nan"
                else None
            )

            weighted_cat_area = tbl[catArea] * (tbl[catPct] / 100)
            weighted_ws_area = tbl[wsArea] * (tbl[wsPct] / 100)

            if row.MetricType == "Mean":
                cat_colname = row.MetricName + "Cat" + a_m
                ws_colname = row.MetricName + "Ws" + a_m
                tbl[cat_colname] = (
                    tbl["CatSum%s" % a_m] / tbl["CatCount%s" % a_m]
                ) * row.Conversion
                tbl[ws_colname] = (
                    tbl["WsSum%s" % a_m] / tbl["WsCount%s" % a_m]
                ) * row.Conversion
                if metric_count == 0:
                    final = tbl[front_cols + [cat_colname] + [ws_colname]]
                else:
                    tbl = tbl[["COMID", cat_colname, ws_colname]]
                    final = pd.merge(final, tbl, on="COMID")

            if row.MetricType == "Density":
                cat_colname = row.MetricName + "Cat" + a_m
                ws_colname = row.MetricName + "Ws" + a_m
                if summaries:
                    cat_sums = []
                    ws_sums = []
                    for summary in summaries:
                        if "Dens" in row.MetricName:
                            row.MetricName = row.MetricName[:-4]
                        sum_col_cat = (
                            row.MetricName + summary + "Cat" + a_m
                        ).replace("M3", "")
                        sum_col_ws = (
                            row.MetricName + summary + "Ws" + a_m
                        ).replace("M3", "")
                        tbl[sum_col_cat] = tbl["Cat" + summary] / weighted_cat_area
                        tbl[sum_col_ws] = tbl["Ws" + summary] / weighted_ws_area
                        cat_sums = pd.concat([cat_sums,sum_col_cat], axis=0, ignore_index=False)
                        ws_sums = pd.concat([ws_sums,sum_col_ws], axis=0, ignore_index=False)
                if table in ["RoadStreamCrossings", "CanalDensity"]:
                    tbl[cat_colname] = (
                        tbl.CatSum / weighted_cat_area * row.Conversion
                    )
                    tbl[ws_colname] = tbl.WsSum / weighted_ws_area * row.Conversion
                else:
                    tbl[cat_colname] = (
                        tbl["CatCount%s" % a_m] / weighted_cat_area * row.Conversion
                    )
                    tbl[ws_colname] = (
                        tbl["WsCount%s" % a_m] / weighted_ws_area * row.Conversion
                    )
                if summaries:
                    end_cols = (
                        [cat_colname]
                        + [x.strip("M3") for x in cat_sums]
                        + [ws_colname]
                        + [x.strip("M3") for x in ws_sums]
                    )
                else:
                    end_cols = [cat_colname, ws_colname]
                if metric_count == 0:
                    final = tbl[front_cols + end_cols].copy()
                else:
                    tbl = tbl[["COMID"] + end_cols]
                    final = pd.merge(final, tbl, on="COMID")

            if row.MetricType == "Percent":
                lookup = pd.read_parquet(row.MetricName)
                cat_named = [
                    "Pct{}Cat{}".format(x, a_m) for x in lookup.final_val.values
                ]
                ws_named = [
                    "Pct{}Ws{}".format(x, a_m) for x in lookup.final_val.values
                ]
                catcols, wscols = [], []
                for col in tbl.columns:
                    if "CatVALUE" in col and not "Up" in col:
                        tbl[col] = (tbl[col] * 1e-6) / weighted_cat_area * 100
                        catcols = pd.concat([catcols,col], axis=0, ignore_index=False)
                    if "WsVALUE" in col:
                        tbl[col] = (tbl[col] * 1e-6) / weighted_ws_area * 100
                        wscols = pd.concat([wscols,col], axis=0, ignore_index=False)
                if metric_count == 0:
                    final = tbl[front_cols + catcols + wscols]
                    final.columns = front_cols + cat_named + ws_named
                else:
                    final2 = tbl[["COMID"] + catcols + wscols]
                    final2.columns = ["COMID"] + cat_named + ws_named
                    final = pd.merge(final, final2, on="COMID")

        final = final.set_index("COMID")
        final_table = pa.Table.from_pandas(final)
        pq.write_table(final_table, final_file)
