import os
from ftplib import FTP

import pandas as pd

ftp = FTP("newftp.epa.gov")
ftp.login()
ftp.cwd("/EPADataCommons/ORD/NHDPlusLandscapeAttributes/StreamCat/HydroRegions/")


published = ftp.nlst()

local_list = os.listdir(
    "O:/PRIV/CPHEA/PESD/COR/CORFiles/"
    "Geospatial_Library_Projects/StreamCat/FTP_Staging/"
    "HydroRegions/zips"
)

local_published = list(
    set([x.split("_Region")[0] for x in local_list if x in published])
)

control_file = "ControlTable_StreamCat.csv"
control = pd.read_csv(control_file)
orig = control.copy()

control["Published"] = control.Final_Table_Name.isin(local_published).map(
    {True: "Yes", False: "No"}
)

# not sure of the best way to deal with this? we could add a list of metrics
# that we know we don't want published to this script and check against that?
newly_published = orig.loc[orig.compare(control).index, "Final_Table_Name"]
if not newly_published.empty:
    print(
        "The following metrics have been recently published in StreamCat:\n\t->",
        ", ".join(newly_published.tolist()),
    )

    try:
        control.to_csv(control_file, index=False)
    except PermissionError as e:
        print(f"You may have {control_file} open in Excel?\n", e)

# move over to LakeCat directory for published zips
ftp.cwd("/EPADataCommons/ORD/NHDPlusLandscapeAttributes/LakeCat/FinalTables/")

# assuming that LakeCat repo is in same parent folder as StreamCat
lk_control = pd.read_csv("../LakeCat/ControlTable_LakeCat.csv")

lake_cat_ftp = [x.split(".zip")[0] for x in ftp.nlst() if x.endswith(".zip")]
# find zips that are published in StreamCat but not in LakeCat
# remove mask metrics with `.str.contains` -- NO MASKS IN LKCAT
lkcat_unpublished = (
    control.query("Published == 'Yes'")
    .loc[~control.Final_Table_Name.isin(lake_cat_ftp)]
    .loc[~control.FullTableName.str.contains(r"(?:RipBuf100|MidSlp|HiSlp)")]
    .FullTableName
)
print(
    "The following metrics have been run in StreamCat but not in LakeCat:\n\t->",
    ", ".join(lkcat_unpublished.tolist()),
)


# LOOKING AT DIFFERENCES BETWEEN LKCAT AND STRMCAT CONTROL TABLES
#
# lk_control.columns
# control.columns
# lk_control.FullTableName.isin(control.FullTableName)
# check = lk_control.loc[~lk_control.FullTableName.isin(control.FullTableName), ["FullTableName", "LandscapeLayer","MetricName"]]
# for row in check.itertuples():
#     if not control.loc[control.LandscapeLayer == row.LandscapeLayer].empty:
#         print(row)

# check2 = lk_control.loc[~lk_control.LandscapeLayer.isin(control.LandscapeLayer), ["FullTableName", "LandscapeLayer","MetricName"]]

# check3 = lk_control.loc[~lk_control.MetricName.isin(control.MetricName), ["FullTableName", "LandscapeLayer","MetricName"]]
