### TODO:
### Rewrite StreamCat API functions using GPU / parquet files functions in the RAPIDS package.
### This assumes we have a database connection for fast writing. 
### Read / Get functions will be done ORDS/REST API when available
#%load_ext cudf.pandas  # pandas operations now use the GPU! Requires RAPIDS install first
import argparse
import os
import requests
import pandas as pd # Change to cuDf after RAPIDS install
import geopandas as gpd
#import cuspatial
#import numpy as np
import cupy as cp
#cupy calls numpy functions and auto maps them to the current device

from StreamCat_functions_gpu import Accumulation, AdjustCOMs, PointInPoly, appendConnectors, createCatStats, interVPU, makeVectors, mask_points, nhd_dict

def main(args):
    print("----- Welcome to the StreamCat high res data pipeline ----- \n")
    
    # These csv files are small and don't necessarily need GPU speedup
    # change from local csv's to database functions
    control_table = pd.read_csv(args.control_table) 
    inter_vpu = pd.read_csv(args.inter_vpu)
    if not os.path.exists(args.config.OUT_DIR):
        os.mkdir(args.config.OUT_DIR)

    if not os.path.exists(args.config.OUT_DIR + "/DBF_stash"):
        os.mkdir(args.config.OUT_DIR + "/DBF_stash")

    if not os.path.exists(args.config.ACCUM_DIR):
        # TODO: work out children OR bastards only
        makeVectors(inter_vpu, args.config.NHD_DIR)
    
    INPUTS = cp.load(args.config.ACCUM_DIR +"/vpu_inputs.npy", allow_pickle=True).item()
    already_processed = []
    
    for _, row in control_table.query("run == 1").iterrows():
        apm = "" if row.AppendMetric == "none" else row.AppendMetric
        if row.use_mask == 1:
            mask_dir = args.config.MASK_DIR_RP100
        elif row.use_mask == 2:
            mask_dir = args.config.MASK_DIR_SLP10
        elif row.use_mask == 3:
            mask_dir = args.config.MASK_DIR_SLP20
        else:
            mask_dir = ""
        layer = (
            row.LandscapeLayer
            if os.sep in row.LandscapeLayer
            else (f"{args.config.LYR_DIR}/{row.LandscapeLayer}")
        )  # use abspath
        if isinstance(row.summaryfield, str):
            summary = row.summaryfield.split(";")
        else:
            summary = None
        if row.accum_type == "Point":
            # Load in point geopandas table and Pct_Full table
            # TODO: script to create this PCT_FULL_FILE
            pct_full = pd.read_csv(
                args.config.PCT_FULL_FILE if row.use_mask == 0 else args.config.PCT_FULL_FILE_RP100
            )
            points = gpd.read_file(layer)
            if mask_dir:
                points = mask_points(points, mask_dir, INPUTS)
        # File string to store InterVPUs needed for adjustments
        Connector = f"{args.config.OUT_DIR}/{row.FullTableName}_connectors.csv"
        print(
            f"Acquiring `{row.FullTableName}` catchment statistics...",
            end="",
            flush=True,
        )
        print("done!")
    print("Accumulating...", end="", flush=True)
    for zone in INPUTS:
        fn = f"{args.config.OUT_DIR}/{row.FullTableName}_{zone}.csv"
        cat = pd.read_csv(fn)
        processed = cat.columns.str.extract(r"^(UpCat|Ws)").any().bool()
        if processed:
            print("skipping!")
            already_processed.append(row.FullTableName)
            break
        print(zone, end=", ", flush=True)

        if zone in inter_vpu.ToZone.values:
            cat = appendConnectors(cat, Connector, zone, inter_vpu)
        accum = cp.load(f"accum_npy/accum_{zone}.npz")

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
            cat = pd.read_csv(f"{args.config.OUT_DIR}/{row.FullTableName}_{zone}.csv")
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
        final.to_csv(f"{args.config.OUT_DIR}/{row.FullTableName}_{zone}.csv", index=False)
        # TODO Instead of to csv we should Create DB table here
        
    print(end="") if processed else print("done!")
    if already_processed:
        print(
            "\n!!!Processing Problem!!!\n\n"
            f"{', '.join(already_processed)} already run!\n"
            "Be sure to delete the associated files in your `OUTDIR` to rerun:"
            f"\n\t> {args.config.OUT_DIR}\n\n!!! `$OUT_DIR/DBF_stash/*` "
            f"output used in 'Continuous' and 'Categorical' metrics!!!"
    )

if __name__ == '__main__':
    # add arg parser for config file and control table layer info
    parser = argparse.ArgumentParser()
    parser.add_argument('control_table', type=str, default='config_tables/ControlTable_StreamCat.csv', help="Path to control table csv")
    parser.add_argument('running_layer_name', type=str, help="Name of layer in control table to set run = 1")
    parser.add_argument('inter_vpu', type=str, default='config_tables/Inter_VPU.csv', help="Path to interVPU csv")
    parser.add_argument('config', type=str, default='config_tables/stream_cat_config.py', help="Path to config file")
    
    # parser.add_argument('device', type=str, default='cpu', help="Device to execute pipeline on")
    
    args = parser.parse_args()
    main()