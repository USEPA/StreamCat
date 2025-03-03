"""
          __                                       __
    _____/ /_________  ____  ____ ___  _________ _/ /_
   / ___/ __/ ___/ _ \/ __ `/ __ `__ \/ ___/ __ `/ __/
  (__  ) /_/ /  /  __/ /_/ / / / / / / /__/ /_/ / /_
 /____/\__/_/   \___/\__,_/_/ /_/ /_/\___/\__,_/\__/

 Authors:  Marc Weber<weber.marc@epa.gov>,
           Ryan Hill<hill.ryan@epa.gov>,
           Darren Thornbrugh<thornbrugh.darren@epa.gov>,
           Rick Debbout<debbout.rick@epa.gov>,
           Tad Larsen<laresn.tad@epa.gov>

 Date: November 29, 2015
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
import os
import click
import geopandas as gpd
import numpy as np
import pandas as pd
# control = "ControlTable_StreamCat.csv"

from stream_cat_config import ConfigArgs
# from stream_cat_config import (
#     LYR_DIR,
#     MASK_DIR_RP100,
#     MASK_DIR_SLP10,
#     MASK_DIR_SLP20,
#     ACCUM_DIR,
#     NHD_DIR,
#     OUT_DIR,
#     PCT_FULL_FILE,
#     PCT_FULL_FILE_RP100,
#     USER_ZONES,
# )

from functions import Accumulation, RasterOperations, SpatialOperations, ZonalOperations, NHDProcessing, InterVPU, dbf2df
# from StreamCat_functions import (
#     Accumulation,
#     AdjustCOMs,
#     PointInPoly,
#     appendConnectors,
#     createCatStats,
#     interVPU,
#     makeNumpyVectors,
#     mask_points,
#     nhd_dict,
# )

    
def process_metric(args, row, inter_vpu, INPUTS):
    
    already_processed = []
    for _, row in ctl.query("run == 1").iterrows():

        apm = "" if row.AppendMetric == "none" else row.AppendMetric
        if row.use_mask == 1:
            mask_dir = args.MASK_DIR_RP100
        elif row.use_mask == 2:
            mask_dir = args.MASK_DIR_SLP10
        elif row.use_mask == 3:
            mask_dir = args.MASK_DIR_SLP20
        else:
            mask_dir = ""
        layer = (
            row.LandscapeLayer
            if "/" in row.LandscapeLayer or "\\" in row.LandscapeLayer
            else (f"{args.LYR_DIR}/{row.LandscapeLayer}")
        )  # use abspath
        if isinstance(row.summaryfield, str):
            summary = row.summaryfield.split(";")
        else:
            summary = None
        if row.accum_type == "Point":
            # Load in point geopandas table and Pct_Full table
            # TODO: script to create this PCT_FULL_FILE
            pct_full = pd.read_csv(
                args.PCT_FULL_FILE if row.use_mask == 0 else args.PCT_FULL_FILE_RP100
            )
            points = gpd.read_file(layer)
            if mask_dir:
                points = SpatialOperations.mask_points(points, mask_dir, INPUTS)
        # File string to store InterVPUs needed for adjustments
        Connector = f"{args.OUT_DIR}/{row.FullTableName}_connectors.csv"
        click.echo(
            f"Acquiring `{row.FullTableName}` catchment statistics..."
        )
        for zone, hydroregion in INPUTS.items():
            if not os.path.exists(f"{args.OUT_DIR}/{row.FullTableName}_{zone}.csv"):
                click.echo(zone)
                pre = f"{args.NHD_DIR}/NHDPlus{hydroregion}/NHDPlus{zone}"
                if not row.accum_type == "Point":
                    izd = (
                        f"{mask_dir}/{zone}.tif"
                        if mask_dir
                        else f"{pre}/NHDPlusCatchment/cat"
                    )
                    cat = ZonalOperations.createCatStats(
                        row.accum_type,
                        layer,
                        izd,
                        args.OUT_DIR,
                        zone,
                        row.by_RPU,
                        mask_dir,
                        args.NHD_DIR,
                        hydroregion,
                        apm,
                    )
                if row.accum_type == "Point":
                    izd = f"{pre}/NHDPlusCatchment/Catchment.shp"
                    cat = SpatialOperations.point_in_poly(
                        points, zone, izd, pct_full, mask_dir, apm, summary
                    )
                cat.to_csv(f"{args.OUT_DIR}/{row.FullTableName}_{zone}.csv", index=False)
        click.echo("done!")
        click.echo("Accumulating...")
        for zone in INPUTS:
            fn = f"{args.OUT_DIR}/{row.FullTableName}_{zone}.csv"
            cat = pd.read_csv(fn)
            processed = cat.columns.str.extract(r"^(UpCat|Ws)").any().bool()
            if processed:
                click.echo("skipping!")
                already_processed.append(row.FullTableName)
                break
            click.echo(zone)

            if zone in inter_vpu.ToZone.values:
                cat = InterVPU.appendConnectors(cat, Connector, zone, inter_vpu)
            accum = np.load(f"accum_npy/accum_{zone}.npz")

            cat.COMID = cat.COMID.astype(accum["comids"].dtype)
            cat.set_index("COMID", inplace=True)
            cat = cat.loc[accum["comids"]].reset_index().copy()
            Accumulator = Accumulation(accum["comids"], accum["lengths"], accum["upstream"])
            up = Accumulator.accumulate(cat, tbl_type="Up")
            # up = Accumulation(
            #     cat, accum["comids"], accum["lengths"], accum["upstream"], "Up"
            # )

            ws = Accumulator.accumulate(cat, tbl_type="Ws")
            # ws = Accumulation(
            #     cat, accum["comids"], accum["lengths"], accum["upstream"], "Ws"
            # )

            if zone in inter_vpu.ToZone.values:
                cat = pd.read_csv(f"{args.OUT_DIR}/{row.FullTableName}_{zone}.csv")
            if zone in inter_vpu.FromZone.values:
                InterVPU.inter_vpu(
                    ws,
                    cat.columns[1:],
                    row.accum_type,
                    zone,
                    Connector,
                    inter_vpu.copy(),
                )
            # TODO final = pd.concat([up.set_index('COMID'), ws.set_index('COMID'), cat.set_index('COMID')] axis=1)
            upFinal = pd.merge(up, ws, on="COMID")
            final = pd.merge(cat, upFinal, on="COMID")
            final.to_csv(f"{args.OUT_DIR}/{row.FullTableName}_{zone}.csv", index=False)
        click.echo(end="processed!") if processed else click.echo("done!")
        if already_processed:
            click.echo(
                "\n!!!Processing Problem!!!\n\n"
                f"{', '.join(already_processed)} already run!\n"
                "Be sure to delete the associated files in your `OUTDIR` to rerun:"
                f"\n\t> {args.OUT_DIR}\n\n!!! `$OUT_DIR/DBF_stash/*` "
                f"output used in 'Continuous' and 'Categorical' metrics!!!"
            )

if __name__ == '__main__':
    args = ConfigArgs()

    ctl = pd.read_csv(args.control_table)
    # Load table of inter vpu connections
    inter_vpu = pd.read_csv(args.inter_vpu)

    if not os.path.exists(args.OUT_DIR):
        os.mkdir(args.OUT_DIR)

    if not os.path.exists(args.OUT_DIR + "/DBF_stash"):
        os.mkdir(args.OUT_DIR + "/DBF_stash")

    if not os.path.exists(args.ACCUM_DIR):
        # TODO: work out children OR bastards only
        nhd = NHDProcessing(args.NHD_DIR)
        nhd.makeNumpyVectors(inter_vpu, args.NHD_DIR, args.USER_ZONES)

    INPUTS = np.load(args.ACCUM_DIR +"/vpu_inputs.npy", allow_pickle=True).item()

    
    for i, row in ctl.query("run == 1").iterrows():
        process_metric(args, row, inter_vpu, INPUTS)