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

import os

import click
import geopandas as gpd
import numpy as np
import pandas as pd

from stream_cat_config import (LYR_DIR, MASK_DIR_RP100, MASK_DIR_SLP10,
                               MASK_DIR_SLP20, NHD_DIR, OUT_DIR, PCT_FULL_FILE,
                               PCT_FULL_FILE_RP100)
from StreamCat_functions import (Accumulation, PointInPoly, appendConnectors,
                                 createCatStats, interVPU, makeNumpyVectors,
                                 nhd_dict)


@click.command()
def run_stream_cat():
    """Process Landscape Layers through NHDPlusV21 framework"""

    # Load table of layers to be run...
    ctl = pd.read_csv("ControlTable_StreamCat.csv")

    # Load table of inter vpu connections
    inter_vpu = pd.read_csv("InterVPU.csv")

    if not os.path.exists(OUT_DIR):
        os.mkdir(OUT_DIR)

    if not os.path.exists("accum_npy"):
        # TODO: work out children OR bastards only
        makeNumpyVectors(inter_vpu, NHD_DIR)

    INPUTS = np.load("accum_npy/vpu_inputs.npy", allow_pickle=True).item()

    already_processed = []

    for _, row in ctl.query("run == 1").iterrows():

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
        if (
            row.accum_type == "Point"
        ):  # Load in point geopandas table and Pct_Full table
            # TODO: script to create this PCT_FULL_FILE
            pct_full = pd.read_csv(
                PCT_FULL_FILE if row.use_mask == 0 else PCT_FULL_FILE_RP100
            )
            points = gpd.read_file(LL)
        if not os.path.exists(OUT_DIR + "/DBF_stash"):
            os.mkdir(OUT_DIR + "/DBF_stash")
        # File string to store InterVPUs needed for adjustments
        Connector = f"{OUT_DIR}/{row.FullTableName}_connectors.csv"
        print(
            f"Acquiring `{row.FullTableName}` catchment statistics...",
            end="",
            flush=True,
        )
        for zone, hydroregion in INPUTS.items():
            if not os.path.exists(f"{OUT_DIR}/{row.FullTableName}_{zone}.csv"):
                print(zone, end=", ", flush=False)
                pre = f"{NHD_DIR}/NHDPlus{hydroregion}/NHDPlus{zone}"
                if not row.accum_type == "Point":
                    izd = (
                        f"{mask_dir}/{zone}.tif"
                        if mask_dir
                        else f"{pre}/NHDPlusCatchment/cat"
                    )
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
        print("done!")
        print("Accumulating...", end="", flush=True)
        for zone in INPUTS:
            fn = f"{OUT_DIR}/{row.FullTableName}_{zone}.csv"
            cat = pd.read_csv(fn)
            processed = cat.columns.str.extract(r"^(UpCat|Ws)").any().bool()
            if processed:
                print("skipping!", flush=False)
                already_processed.append(row.FullTableName)
                break
            print(zone, end=", ", flush=True)
            if zone in inter_vpu.ToZone.values:
                cat = appendConnectors(cat, Connector, zone, inter_vpu)
            accum = np.load(f"accum_npy/accum_{zone}.npz")

            cat.COMID = cat.COMID.astype(accum["comids"].dtype)
            cat.set_index("COMID", inplace=True)
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
        print(end="") if processed else print("done!")
    if already_processed:
        print(
            "\n!!!Processing Problem!!!\n\n"
            f"{', '.join(already_processed)} already run!\n"
            "Be sure to delete the associated files in your `OUTDIR` to rerun:"
            f"\n\t> {OUT_DIR}\n\n"
            f"!!! `$OUT_DIR/DBF_stash/*` output used in 'Continuous' and 'Categorical' metrics!!!"
        )


if __name__ == "__main__":
    run_stream_cat()
