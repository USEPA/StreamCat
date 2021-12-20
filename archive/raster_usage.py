import os
import pandas as pd

lk_path = "E:/projects/LakeCat"
split_path = ("L:/Priv/CORFiles/Geospatial_Library_Projects/StreamCat/"
                "NARS_SplitCat_Metrics/NRSA-2013-14")
ctl = pd.read_csv("ControlTable_StreamCat.csv")
lk_ctl = pd.read_csv(f"{lk_path}/ControlTable_LakeCat.csv")
split_ctl = pd.read_csv(f"{split_path}/Control.csv")
print(ctl.columns)
print(split_ctl.columns)
print(lk_ctl.columns)
qa_complete = ("O:/PRIV/CPHEA/PESD/COR/CORFiles/Geospatial_Library_Projects/"
               "StreamCat/LandscapeRasters/QAComplete/")
stored = list(filter(
                lambda f: f.endswith(".tif") or f.endswith(".shp"),
                os.listdir(qa_complete)))
print(len(stored))

todos = list(set((ctl.LandscapeLayer.tolist() + 
        split_ctl.LandscapeLayer.tolist() + 
        lk_ctl.LandscapeLayer.tolist())))

outs = list(filter(lambda x: x not in stored, todos))

cleaned = list(filter(
                lambda f: not any([f.startswith("NLCD"),
                        f.startswith("elev")]),
                outs))
# `cleaned` represents layers that we have in control tables
# but not in the QAComplete folder
keep = pd.DataFrame()
for df, name in zip([ctl, lk_ctl, split_ctl], ["StreamCat","LakeCat","SplitCat"]):
    hold = df.loc[df.LandscapeLayer.isin(cleaned),
                            ["FullTableName","LandscapeLayer"]]
    hold["REPO"] = name
    keep = keep.append(hold)
keep.sort_values("LandscapeLayer", inplace=True)
# `keep` is all of the rasters that are in ControlTables, but not in QAComplete


qs = list(filter(lambda x: x not in todos, stored))

dbf_stash = ("O:/PRIV/CPHEA/PESD/COR/CORFiles/Geospatial_Library_Projects/"
               "StreamCat/Allocation_and_Accumulation/DBF_stash")

dbfs = list(filter(lambda x: x.endswith(".dbf"), os.listdir(dbf_stash)))
# our standard convention for naming the output file that we use in either
# ZonalStatisticsAsTable or TablulateArea is as follows:
# f"zonalstats_{LandscapeLayer}{VPU}.dbf"
import numpy as np
inputs = np.load("accum_npy/vpu_inputs.npy", allow_pickle=True).item()

r_dict = dict()
for tif in qs:
    pre, post = tif.split(".")
    zstat_files = set([f"zonalstats_{pre}{vpu}.dbf" for vpu in inputs.keys()])
    rand = list(zstat_files.intersection(set(dbfs)))
    r_dict.update({tif: rand})

zz = [x for x in dbfs if "RipBuf100" in x]


