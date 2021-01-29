# -*- coding: utf-8 -*-
"""
Created on Fri Jan 29 15:08:24 2021

This is a quick and dirty script to check the values w/in each table to check
that they are equivalent to the output that we have already on the L drive.

@author: Rdebbout
"""


import numpy as np
import pandas as pd
INPUTS = np.load("accum_npy/vpu_inputs.npy", allow_pickle=True).item()
from stream_cat_config import OUT_DIR, FINAL_DIR


def run_checks(metrics, final=False):
    check_dir = OUT_DIR if not final else FINAL_DIR
    alloc_dir = "L:/Priv/CORFiles/Geospatial_Library_Projects/StreamCat/"
    alloc_dir += ("FTP_Staging/HydroRegions" if final 
                    else "Allocation_and_Accumulation")
    for metric in metrics:
        print(metric)
        for zone in INPUTS:
            print(zone, end="...", flush=True)
            fn = f"{metric}{zone}.csv" if final else f"{metric}_{zone}.csv"
            t1 = pd.read_csv(alloc_dir + "/" + fn).set_index("COMID").sort_index()
            t2 = pd.read_csv(check_dir + "/" + fn).set_index("COMID").sort_index()
            assert all(t1.index == t2.index)
            assert t1.columns.sort_values().tolist() == t2.columns.sort_values().tolist()
            tot = pd.merge(t1, t2, left_on=t1.index, right_on=t2.index)
            for col in t1.columns:
                tot["diff"] = abs(tot[f"{col}_x"] - tot[f"{col}_y"])
                assert len(tot.loc[tot["diff"] > 0.0000001]) == 0
            print("good!")

metrics = ["nlcd2001_RipBuf100", "Dams", "CBNF"]
final_metrics = ["Dams_Region","Elevation_Region","Lithology_Region","STATSGO_Set1_Region"]
run_checks(metrics)
run_checks(final_metrics, final=True)