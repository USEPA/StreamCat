"""
          __                                       __
    _____/ /_________  ____  ____ ___  _________ _/ /_
   / ___/ __/ ___/ _ \/ __ `/ __ `__ \/ ___/ __ `/ __/
  (__  ) /_/ /  /  __/ /_/ / / / / / / /__/ /_/ / /_
 /____/\__/_/   \___/\__,_/_/ /_/ /_/\___/\__,_/\__/

 Authors:  Marc Weber<weber.marc@epa.gov>,
           Ryan Hill<hill.ryan@epa.gov>,
           Rick Debbout<debbout.rick@epa.gov>,

 Date: April 2022
"""

import os
import geopandas as gpd
import numpy as np
import pandas as pd


from stream_cat_config_vfgen import (
    CONTROL,
    LYR_DIR,
    NHD_DIR,
    REGS,
    OUT_DIR,
    PCT_FULL_FILE,
)
from StreamCat_functions import (
    Accumulation,
    PointInPoly,
    createCatStats,
    makeNumpyVectors,
    mask_points,
    nhd_dict,
)


# Load table of layers to be run...
ctl = pd.read_csv(CONTROL)


if not os.path.exists(OUT_DIR):
    os.mkdir(OUT_DIR)

if not os.path.exists(OUT_DIR + "/DBF_stash"):
    os.mkdir(OUT_DIR + "/DBF_stash")

if not os.path.exists("accum_npy"):
    # TODO: work out children OR bastards only
    makeNumpyVectors(NHD_DIR, REGS)

already_processed = []

for _, row in ctl.query("run == 1").iterrows():

    layer = (
        row.LandscapeLayer
        if os.sep in row.LandscapeLayer
        else (f"{LYR_DIR}/{row.LandscapeLayer}")
    )  # use abspath
    if isinstance(row.summaryfield, str):
        summary = row.summaryfield.split(";")
    else:
        summary = None
    if row.accum_type == "Point":
        # Load in point geopandas table and Pct_Full table
        # TODO: script to create this PCT_FULL_FILE
        pct_full = pd.read_csv(PCT_FULL_FILE)
        points = gpd.read_file(layer)
    
    print(
        f"Acquiring `{row.FullTableName}` catchment statistics...",
        end="",
        flush=True,
    )
    for REG in REGS:
        if not os.path.exists(f"{OUT_DIR}/{row.FullTableName}_{zone}.csv"):
            print(REG, end=", ", flush=True)
            if not row.accum_type == "Point":
                izd = (f"{NHD_DIR}/NHDPlusHRVFGen_01_V2.gdb/cat")
                cat = createCatStats(
                    row.accum_type,
                    layer,
                    izd,
                    OUT_DIR,
                    REG,
                    NHD_DIR,)
            if row.accum_type == "Point":
                izd = f"{pre}/NHDPlusCatchment/Catchment.shp"
                cat = PointInPoly(
                    points, zone, izd, pct_full, mask_dir, apm, summary
                )
            cat.to_csv(f"{OUT_DIR}/{row.FullTableName}_{zone}.csv", index=False)
    print("done!")
    print("Accumulating...", end="", flush=True)
    for zone in INPUTS:
        fn = f"{OUT_DIR}/{row.FullTableName}_{zone}.csv"
        cat = pd.read_csv(fn)
        processed = cat.columns.str.extract(r"^(UpCat|Ws)").any().bool()
        if processed:
            print("skipping!")
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
        f"\n\t> {OUT_DIR}\n\n!!! `$OUT_DIR/DBF_stash/*` "
        f"output used in 'Continuous' and 'Categorical' metrics!!!"
    )


if __name__ == "__main__":
    run_stream_cat()
