#!/usr/bin/env python
"""
 Script to call StreamCat functions script and run allocation and
 accumulation of landscape metrics to NHDPlus catchments.  Assumes
 landscape rasters in desired projection with appropriate
 pre-processing to deal with any reclassing of values or recoding
 of NA, and directories of NHDPlusV2 data installed in standard
 directory format.
          __                                       __
    _____/ /_________  ____  ____ ___  _________ _/ /_
   / ___/ __/ ___/ _ \/ __ `/ __ `__ \/ ___/ __ `/ __/
  (__  ) /_/ /  /  __/ /_/ / / / / / / /__/ /_/ / /_
 /____/\__/_/   \___/\__,_/_/ /_/ /_/\___/\__,_/\__/

 Authors:  Marc Weber<weber.marc@epa.gov>,
           Ryan Hill<hill.ryan@epa.gov>,
           Darren Thornbrugh<thornbrugh.darren@epa.gov>,
           Rick Debbout<debbout.rick@epa.gov>,
           and Tad Larsen<laresn.tad@epa.gov>

 Date: November 29, 2015

 NOTE: Navigate to the directory and run script:
 > python StreamCat.py
 --------------------------------------------------------
"""
# TODO: remake npz files for bastards only, don't retain comids w/o upstream!
# TODO: calculate WS metrics by table math, not in numpy accumulation process
# TODO: create function like findUpstreamNPY for up and ws

import os
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime as dt
from stream_cat_config import (
    LYR_DIR,
    NHD_DIR,
    OUT_DIR,
    mask_dir_RP100,
    mask_dir_Slp10,
    mask_dir_Slp20,
    pct_full_file,
    pct_full_file_RP100,
)
from StreamCat_functions import (
    Accumulation,
    appendConnectors,
    createCatStats,
    interVPU,
    makeNumpyVectors,
    nhd_dict,
    PointInPoly,
)

# Load table of layers to be run...
ctl = pd.read_csv("ControlTable_StreamCat.csv")


totTime = dt.now()

# Load table of inter vpu connections
inter_vpu = pd.read_csv("InterVPU.csv")

if not os.path.exists(OUT_DIR):
    os.mkdir(OUT_DIR)

if not os.path.exists("accum_npy"):
    # TODO: work out children OR bastards only
    makeNumpyVectors(inter_vpu, NHD_DIR)

INPUTS = np.load(
    "accum_npy/vpu_inputs.npy", allow_pickle=True
).item()

for _, row in ctl.query("run == 1").iterrows():

    print("running: " + row.FullTableName)
    apm = "" if row.AppendMetric == "none" else row.AppendMetric
    if row.use_mask == 1:
        mask_dir = mask_dir_RP100
    elif row.use_mask == 2:
        mask_dir = mask_dir_Slp10
    elif row.use_mask == 3:
        mask_dir = mask_dir_Slp20
    else:
        mask_dir = ""
    LL = f"{LYR_DIR}/{row.LandscapeLayer}"
    summaryfield = None
    if type(row.summaryfield) == str:
        summaryfield = row.summaryfield.split(";")
    if row.accum_type == "Point":  # Load in point geopandas table and Pct_Full table
        if row.use_mask == 0:  # TODO: script to create this pct_full_file
            pct_full_file = pct_full_file
        if row.use_mask == 1:
            pct_full_file = pct_full_file_RP100
        pct_full = pd.read_csv(pct_full_file)
        points = gpd.GeoDataFrame.from_file(LL)
    if not os.path.exists(OUT_DIR + "/DBF_stash"):
        os.mkdir(OUT_DIR + "/DBF_stash")
    # File string to store InterVPUs needed for adjustments
    Connector = f"{OUT_DIR}/{row.FullTableName}_connectors.csv"
    catTime = dt.now()
    for zone, hydroregion in INPUTS.items():
        if not os.path.exists(f"{OUT_DIR}/{row.FullTableName}_{zone}.csv"):
            pre = f"{NHD_DIR}/NHDPlus{hydroregion}/NHDPlus{zone}"
            if not row.accum_type == "Point":
                izd = f"{mask_dir}/{zone}.tif" if mask_dir else f"{pre}/NHDPlusCatchment/cat"
                cat = createCatStats(
                    row.accum_type,
                    LL,
                    izd,
                    OUT_DIR,
                    zone,
                    row.by_RPU,
                    mask_dir,
                    NHD_DIR,
                    hydroregion,
                    apm,
                )
            if row.accum_type == "Point":
                izd = f"{pre}/NHDPlusCatchment/Catchment.shp"
                cat = PointInPoly(
                    points, zone, izd, pct_full, mask_dir, apm, summaryfield
                )
            cat.to_csv(f"{OUT_DIR}/{row.FullTableName}_{zone}.csv", index=False)
            in2accum = len(cat.columns)
    print("Cat Results Complete in : " + str(dt.now() - catTime))
    try:
        # if in2accum not defined...Cat process done,but error thrown in accum
        in2accum
    except NameError:
        # get number of columns to test if accumulation needs to happen
        in2accum = len(pd.read_csv(f"{OUT_DIR}/{row.FullTableName}_{zone}.csv").columns)
    accumTime = dt.now()
    for zone in INPUTS:
        cat = pd.read_csv(f"{OUT_DIR}/{row.FullTableName}_{zone}.csv")
        in2accum = len(cat.columns)
        if len(cat.columns) == in2accum:
            if zone in inter_vpu.ToZone.values:
                cat = appendConnectors(cat, Connector, zone, inter_vpu)
            accum = np.load(f"accum_npy/accum_{zone}.npz")
            
            cat.COMID = cat.COMID.astype(accum["comids"].dtype)
            cat.set_index("COMID",inplace=True)
            cat = cat.loc[accum["comids"]].reset_index().copy()                
            
            up = Accumulation(
                cat, accum["comids"], accum["lengths"], accum["upstream"], "Up"
            )

            ws = Accumulation(
                cat, accum["comids"], accum["lengths"], accum["upstream"], "Ws"
            )

            if zone in inter_vpu.ToZone.values:
                cat = pd.read_csv(f"{OUT_DIR}/{row.FullTableName}_{zone}.csv")
            if zone in inter_vpu.FromZone.values:
                interVPU(
                    ws,
                    cat.columns[1:],
                    row.accum_type,
                    zone,
                    Connector,
                    inter_vpu.copy(),
                    summaryfield,
                )
            upFinal = pd.merge(up, ws, on="COMID")
            final = pd.merge(cat, upFinal, on="COMID")
            final.to_csv(f"{OUT_DIR}/{row.FullTableName}_{zone}.csv", index=False)
    print("Accumulation Results Complete in : " + str(dt.now() - accumTime))
print("total elapsed time " + str(dt.now() - totTime))
