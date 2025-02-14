### TODO:
### Rewrite StreamCat API functions using GPU / parquet files functions in the RAPIDS package.
### This assumes we have a database connection for fast writing. 
### Read / Get functions will be done ORDS/REST API when available
#%load_ext cudf.pandas  # pandas operations now use the GPU! Requires RAPIDS install first
import argparse
import os
import time
import pandas as pd # Change to cuDf after RAPIDS install
import geopandas as gpd
#import cuspatial
import numpy as np

import dask
from dask.distributed import Client, LocalCluster
import dask.dataframe as dd
from dask_jobqueue import SLURMCluster
from dask_jobqueue.slurm import SLURMRunner, SLURMJob

from joblib import Parallel, delayed

from StreamCat_functions_gpu import Accumulation, AdjustCOMs, PointInPoly, appendConnectors, createCatStats, interVPU, makeVectors, mask_points, nhd_dict
# from config_tables.stream_cat_config import (
#     LYR_DIR,
#     MASK_DIR_RP100,
#     MASK_DIR_SLP10,
#     MASK_DIR_SLP20,
#     ACCUM_DIR,
#     NHD_DIR,
#     OUT_DIR,
#     PCT_FULL_FILE,
#     PCT_FULL_FILE_RP100,
# )
from config_tables.stream_cat_config import ConfigArgs, DebugArgs
#from database import DatabaseConnection
from itertools import islice
from collections import OrderedDict

def process_metric(row, config, INPUTS, already_processed, inter_vpu):
    print(row.FullTableName)
    apm = "" if row.AppendMetric == "none" else row.AppendMetric
    if row.use_mask == 1:
        mask_dir = config.MASK_DIR_RP100
    elif row.use_mask == 2:
        mask_dir = config.MASK_DIR_SLP10
    elif row.use_mask == 3:
        mask_dir = config.MASK_DIR_SLP20
    else:
        mask_dir = ""
    layer = (
        row.LandscapeLayer
        if os.sep in row.LandscapeLayer
        else (f"{config.LYR_DIR}/QAComplete/{row.LandscapeLayer}")
    )  # TODO use abspath
    if isinstance(row.summaryfield, str):
        summary = row.summaryfield.split(";")
    else:
        summary = None
    if row.accum_type == "Point":
        # Load in point geopandas table and Pct_Full table
        # TODO: script to create this PCT_FULL_FILE
        pct_full = pd.read_csv(
            config.PCT_FULL_FILE if row.use_mask == 0 else config.PCT_FULL_FILE_RP100
        )
        points = gpd.read_file(layer) #TODO could add from_pandas or dg.read_file for dask conversion
        if mask_dir:
            points = mask_points(points, mask_dir, INPUTS)
    # File string to store InterVPUs needed for adjustments
    # Currently stored in Streamcat Accumulation_and_Allocation
    Connector = f"{config.FINAL_OUT_DIR}/{row.FullTableName}_connectors.csv"
    print(
        f"Acquiring `{row.FullTableName}` catchment statistics...",
        end="",
        flush=True,
    )

    for zone, hydroregion in INPUTS.items():
        if not os.path.exists(f"{config.OUT_DIR}/{row.FullTableName}_{zone}.csv"):
            print(f"Region {zone}, ")
            pre = f"{config.NHD_DIR}/NHDPlus{hydroregion}/NHDPlus{zone}"
            if not row.accum_type == "Point":
                izd = (
                    f"{mask_dir}/{zone}.tif"
                    if mask_dir
                    else f"{pre}/NHDPlusCatchment/cat"
                )
                cat = createCatStats(
                    row.accum_type,
                    layer,
                    izd,
                    config.OUT_DIR,
                    zone,
                    row.by_RPU,
                    mask_dir,
                    config.NHD_DIR,
                    hydroregion,
                    apm,
                )
            if row.accum_type == "Point":
                izd = f"{pre}/NHDPlusCatchment/Catchment.shp"
                cat = PointInPoly(
                    points, zone, izd, pct_full, mask_dir, apm, summary
                )
            # TODO instead of writing to csv add cat to dask dataframe and persist()
            # This is because dask dataframes are a collection of pandas dataframes
            # This dataframe should have be named {row.FullTableName}_{zone} then below in the next loop we should 
            # get fn / cat dataframe by name 
            cat.to_csv(f"{config.OUT_DIR}/{row.FullTableName}_{zone}.csv", index=False) 
    print("done!\n")
    print("Accumulating...")
    #TODO if we change cats to be parititons in a dask dataframe then we need to change from zones to iterating through the dask sections
    # Also consider writing large dask dataframe out to parquet file here as a checkpoint of sorts
    for zone in INPUTS:
        # TODO 
        # read fn table from database 
        print(f"Region {zone}, ")
        fn = f"{config.OUT_DIR}/{row.FullTableName}_{zone}.csv"
        cat = pd.read_csv(fn)
        processed = cat.columns.str.extract(r"^(UpCat|Ws)").any().item() #TODO .bool() is depreciated change to .item()
        if processed:
            print("skipping!")
            already_processed.append(row.FullTableName)
            break
        # print(zone, end=", ", flush=True)

        if zone in inter_vpu.ToZone.values:
            cat = appendConnectors(cat, Connector, zone, inter_vpu)
        accum = np.load(f"{config.ACCUM_DIR}/accum_{zone}.npz")

        cat.COMID = cat.COMID.astype(accum["comids"].dtype)
        cat.set_index("COMID", inplace=True)
        cat = cat.loc[accum["comids"]].reset_index().copy()
        print(f"Starting Accumulation for {row.FullTableName} in region {zone}")
        start_accum = time.time()
        up = Accumulation(
            cat, accum["comids"], accum["lengths"], accum["upstream"], "Up"
        )

        ws = Accumulation(
            cat, accum["comids"], accum["lengths"], accum["upstream"], "Ws"
        )
        end_accum = time.time()
        print(f"Finished Parallel accumulation in {end_accum - start_accum} seconds")

        if zone in inter_vpu.ToZone.values:
            cat = pd.read_csv(f"{config.OUT_DIR}/{row.FullTableName}_{zone}.csv")
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
        final.to_csv(f"{config.OUT_DIR}/{row.FullTableName}_{zone}.csv", index=False)
        # TODO Instead of to csv we should Create DB table here
        # Could create a dict / array / named array for each zone
        # When done with zones concat above array
        # write concatenated dataframe to database
    return processed


def main(args):
    print("----- Welcome to the StreamCat high res data pipeline ----- \n")

    # TODO set execute to true when doing real streamcat run
    # db_conn = DatabaseConnection() 
    # db_conn.connect()
    
    # These csv files are small and don't necessarily need GPU speedup
    # change from local csv's to database functions
    # TODO write static csv's to database then read via db_conn.metadata.tables
    control_table = pd.read_csv(args.control_table) 
    inter_vpu = pd.read_csv(args.inter_vpu)
    config = ConfigArgs()
    if not os.path.exists(config.OUT_DIR):
        os.mkdir(config.OUT_DIR)

    if not os.path.exists(config.OUT_DIR + "/DBF_stash"):
        os.mkdir(config.OUT_DIR + "/DBF_stash")

    if not os.path.exists(config.ACCUM_DIR):
        # TODO: work out children OR bastards only
        # Is this step necessary
        makeVectors(inter_vpu, config.NHD_DIR)
    
    INPUTS = np.load(config.ACCUM_DIR +"/vpu_inputs.npy", allow_pickle=True).item()
    # C:\Users\thudso02\repositories\parallel_streamcat\StreamCat\accum_npy\vpu_inputs.npy
    
    sliced = islice(INPUTS.items(), 7, 8)
    INPUTS = OrderedDict(sliced)
    #INPUTS = OrderedDict([('01', 'NE')])
    already_processed = []
    # metric_results = Parallel(n_jobs=2) (
    #     delayed(process_metric)(row, config, INPUTS, already_processed, inter_vpu) for _, row in control_table.query("run == 1").iterrows()
    # )
    for _, row in control_table.query("run == 1").iterrows():
        metric_results = process_metric(row, config, INPUTS, already_processed, inter_vpu)

    for processed in metric_results:
        print(processed)
        if processed:
            print(
                "\n!!!Processing Problem!!!\n\n"
                f"{', '.join(already_processed)} already run!\n"
                "Be sure to delete the associated files in your `OUTDIR` to rerun:"
                f"\n\t> {config.OUT_DIR}\n\n!!! `$OUT_DIR/DBF_stash/*` "
                f"output used in 'Continuous' and 'Categorical' metrics!!!"
        )
    
    # for i, row in control_table.query("run == 1").iterrows():
    #     print(row.FullTableName)
    #     apm = "" if row.AppendMetric == "none" else row.AppendMetric
    #     if row.use_mask == 1:
    #         mask_dir = config.MASK_DIR_RP100
    #     elif row.use_mask == 2:
    #         mask_dir = config.MASK_DIR_SLP10
    #     elif row.use_mask == 3:
    #         mask_dir = config.MASK_DIR_SLP20
    #     else:
    #         mask_dir = ""
    #     layer = (
    #         row.LandscapeLayer
    #         if os.sep in row.LandscapeLayer
    #         else (f"{config.LYR_DIR}/QAComplete/{row.LandscapeLayer}")
    #     )  # TODO use abspath
    #     if isinstance(row.summaryfield, str):
    #         summary = row.summaryfield.split(";")
    #     else:
    #         summary = None
    #     if row.accum_type == "Point":
    #         # Load in point geopandas table and Pct_Full table
    #         # TODO: script to create this PCT_FULL_FILE
    #         pct_full = pd.read_csv(
    #             config.PCT_FULL_FILE if row.use_mask == 0 else config.PCT_FULL_FILE_RP100
    #         )
    #         points = gpd.read_file(layer) #TODO could add from_pandas or dg.read_file for dask conversion
    #         if mask_dir:
    #             points = mask_points(points, mask_dir, INPUTS)
    #     # File string to store InterVPUs needed for adjustments
    #     # Currently stored in Streamcat Accumulation_and_Allocation
    #     Connector = f"{config.FINAL_OUT_DIR}/{row.FullTableName}_connectors.csv"
    #     print(
    #         f"Acquiring `{row.FullTableName}` catchment statistics...",
    #         end="",
    #         flush=True,
    #     )
    
    #     for zone, hydroregion in INPUTS.items():
    #         if not os.path.exists(f"{config.OUT_DIR}/{row.FullTableName}_{zone}.csv"):
    #             print(f"Region {zone}, ")
    #             pre = f"{config.NHD_DIR}/NHDPlus{hydroregion}/NHDPlus{zone}"
    #             if not row.accum_type == "Point":
    #                 izd = (
    #                     f"{mask_dir}/{zone}.tif"
    #                     if mask_dir
    #                     else f"{pre}/NHDPlusCatchment/cat"
    #                 )
    #                 cat = createCatStats(
    #                     row.accum_type,
    #                     layer,
    #                     izd,
    #                     config.OUT_DIR,
    #                     zone,
    #                     row.by_RPU,
    #                     mask_dir,
    #                     config.NHD_DIR,
    #                     hydroregion,
    #                     apm,
    #                 )
    #             if row.accum_type == "Point":
    #                 izd = f"{pre}/NHDPlusCatchment/Catchment.shp"
    #                 cat = PointInPoly(
    #                     points, zone, izd, pct_full, mask_dir, apm, summary
    #                 )
    #             # TODO instead of writing to csv add cat to dask dataframe and persist()
    #             # This is because dask dataframes are a collection of pandas dataframes
    #             # This dataframe should have be named {row.FullTableName}_{zone} then below in the next loop we should 
    #             # get fn / cat dataframe by name 
    #             cat.to_csv(f"{config.OUT_DIR}/{row.FullTableName}_{zone}.csv", index=False) 
    #     print("done!\n")
    #     print("Accumulating...")
    #     #TODO if we change cats to be parititons in a dask dataframe then we need to change from zones to iterating through the dask sections
    #     # Also consider writing large dask dataframe out to parquet file here as a checkpoint of sorts
    #     for zone in INPUTS:
    #         # TODO 
    #         # read fn table from database 
    #         print(f"Region {zone}, ")
    #         fn = f"{config.OUT_DIR}/{row.FullTableName}_{zone}.csv"
    #         cat = pd.read_csv(fn)
    #         processed = cat.columns.str.extract(r"^(UpCat|Ws)").any().item() #TODO .bool() is depreciated change to .item()
    #         if processed:
    #             print("skipping!")
    #             already_processed.append(row.FullTableName)
    #             break
    #         # print(zone, end=", ", flush=True)

    #         if zone in inter_vpu.ToZone.values:
    #             cat = appendConnectors(cat, Connector, zone, inter_vpu)
    #         accum = np.load(f"{config.ACCUM_DIR}/accum_{zone}.npz")

    #         cat.COMID = cat.COMID.astype(accum["comids"].dtype)
    #         cat.set_index("COMID", inplace=True)
    #         cat = cat.loc[accum["comids"]].reset_index().copy()

    #         up = Accumulation(
    #             cat, accum["comids"], accum["lengths"], accum["upstream"], "Up"
    #         )

    #         ws = Accumulation(
    #             cat, accum["comids"], accum["lengths"], accum["upstream"], "Ws"
    #         )

    #         if zone in inter_vpu.ToZone.values:
    #             cat = pd.read_csv(f"{config.OUT_DIR}/{row.FullTableName}_{zone}.csv")
    #         if zone in inter_vpu.FromZone.values:
    #             interVPU(
    #                 ws,
    #                 cat.columns[1:],
    #                 row.accum_type,
    #                 zone,
    #                 Connector,
    #                 inter_vpu.copy(),
    #             )
    #         upFinal = pd.merge(up, ws, on="COMID")
    #         final = pd.merge(cat, upFinal, on="COMID")
    #         final.to_csv(f"{config.OUT_DIR}/{row.FullTableName}_{zone}.csv", index=False)
    #         # TODO Instead of to csv we should Create DB table here
    #         # Could create a dict / array / named array for each zone
    #         # When done with zones concat above array
    #         # write concatenated dataframe to database

            
    #     print(end="") if processed else print("done!")
    


if __name__ == '__main__':

    # test = np.load('C:\\Users\\thudso02\\repositories\\parallel_streamcat\\StreamCat\\accum_npy\\vpu_inputs.npy')
    # print(test)
    # #TODO move this to a CLI type script called streamcat_cli.py
    # parser = argparse.ArgumentParser()
    # parser.add_argument('control_table', type=str, default='config_tables/ControlTable_StreamCat.csv', help="Path to control table csv")
    # # parser.add_argument('running_layer_name', nargs='+', type=str, help="Name of layers (space delimited) in control table to set run = 1")
    # parser.add_argument('inter_vpu', type=str, default='config_tables/Inter_VPU.csv', help="Path to interVPU csv")
    # parser.add_argument('config', type=str, default='config_tables/streamcat_config.json', help="Path to config file")
    
    # # parser.add_argument('device', type=str, default='cpu', help="Device to execute pipeline on")
    
    # args = parser.parse_args()
    args = DebugArgs()
    # num_workers = 8 # os.cpu_count()
    # cluster = LocalCluster(n_workers=num_workers)

    # If using HPC slurm machine
    # uncomment following 3 lines
    # cluster = SLURMCluster()
    # cluster.adapt(minimum=1, maximum=64)
    # cluster = SLURMRunner()
    
    # client = Client(cluster)
    # print(client.dashboard_link)
    # start_time = time.time()
    main(args)
    
    # end_time = time.time()
    # print(f"Time to complete full pipeline: {(end_time-start_time)/60} minutes. ")