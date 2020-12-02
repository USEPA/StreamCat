# -*- coding: utf-8 -*-
"""
Created on Jan 22, 2016
Script to build final StreamCat tables.
Run script from command line passing directory and name of this script 
and then directory and name of the control table to use like this:
    >> python "F:\Watershed Integrity Spatial Prediction\Scripts\makeFinalTables.py" 

@author: rdebbout, mweber
"""

import os
import sys
import math
import zipfile
import numpy as np
import pandas as pd
from pathlib2 import Path

from stream_cat_config import OUT_DIR, LENGTHS, FINAL_DIR

def build_stats(tbl, stats):
    if not stats:
        for c in tbl.columns.tolist():
            stats[c] = {"min": tbl[c].min(), "max":tbl[c].max()}
        return stats
    for col in tbl.columns.tolist():
        if tbl[col].min() < stats[col]["min"]:
            stats[col]["min"] = tbl[col].min()
        if tbl[col].max() > stats[col]["max"]:
            stats[col]["max"] = tbl[col].max()
    return stats

LENGTH_ERROR_MESSAGE = ("Table {} length vpu {} incorrect!!!!"
                        "...check Allocation and Accumulation results")

OUT_DIR = Path(OUT_DIR) # TODO: change this in the config
FINAL_DIR = Path(FINAL_DIR) # TODO: change this in the config
ctl = pd.read_csv("ControlTable_StreamCat.csv") # TODO move CONTROL_TABLE to config

inputs = np.load("accum_npy/vpu_inputs.npy").item()

tables = dict()
runners = ctl.query("run == 1").groupby("Final_Table_Name")
tables = runners["FullTableName"].unique().to_dict()
# check that all accumulated files are present
missing = []
fn = "{}_{}.csv"
for table, metrics in tables.items(): # make sure all tables exist
    for vpu in inputs:
        for metric in metrics:
            accumulated_file = OUT_DIR / fn.format(metric, vpu)
            if not accumulated_file.exists():
                missing.append(accumulated_file)

if len(missing) > 0:
    for miss in missing:
        print("Missing {}".format(miss.name))
    print "Check output from StreamCat.py"
    sys.exit()

states_lookup = Path("state_dict.npz")
states_dict = np.load(str(states_lookup))["data"].item()

STATES_DIR = FINAL_DIR.parents[0] / "States"
if not FINAL_DIR.exists():
    FINAL_DIR.mkdir(parents=True)
    (FINAL_DIR / "zips").mkdir()
    STATES_DIR.mkdir()

region_fn = "{}_Region{}.csv"
for table, metrics in tables.items():

    print("Running {} .....into {}".format(table, FINAL_DIR))
    # this will print stats for every final table, used for metadata
    stats = dict()
    # Looop through NHD Hydro-regions
    for vpu in inputs:
        out_file = FINAL_DIR / region_fn.format(table,vpu)
        zip_file = FINAL_DIR / "zips" / "_Region{}.zip".format(table, vpu)

        # Check if output tables exist before writing
        if not out_file.exists():
            for metric_count, metric in enumerate(metrics):
                idx = ctl.loc[ctl.FullTableName == metric].index.item()
                row = ctl.iloc[idx].copy()


                a_m = "" if row.AppendMetric == "none" else row.AppendMetric
                # Read in the StreamCat allocation and accumulation table
                tbl = pd.read_csv(OUT_DIR / fn.format(metric, vpu))
                front_cols = [title for title in tbl.columns
                             for x in ["COMID","AreaSqKm","PctFull"]
                             if x in title and not "Up" in title]
                _, catArea, catPct, wsArea, wsPct = front_cols
                #re-order for correct sequence
                front_cols = [front_cols[i] for i in [0,1,3,2,4]]
                
                # this protects summarization if the field is 
                summaries = (row.summaryfield.split(";")
                            if not str(row.summaryfield) == "nan"
                            else None)

                weighted_cat_area = tbl[catArea] * (tbl[catPct]/100)
                weighted_ws_area = tbl[wsArea] * (tbl[wsPct]/100)

                if row.MetricType == "Mean":
                    cat_colname = row.MetricName + "Cat" + a_m
                    ws_colname = row.MetricName + "Ws" + a_m
                    tbl[cat_colname] = ((tbl["CatSum%s" % a_m] /
                                           tbl["CatCount%s" % a_m]) *
                                            row.Conversion)
                    tbl[ws_colname] = ((tbl["WsSum%s" % a_m] /
                                           tbl["WsCount%s" % a_m]) *
                                            row.Conversion)
                    if metric_count == 0:
                        final = tbl[front_cols + [cat_colname] + [ws_colname]]
                    else:
                        tbl = tbl[["COMID",cat_colname,ws_colname]]
                        final = pd.merge(final, tbl, on="COMID")

                if row.MetricType == "Density":
                    cat_colname = row.MetricName + "Cat" + a_m
                    ws_colname = row.MetricName + "Ws" + a_m
                    if summaries:
                        cat_sums = []
                        ws_sums = []
                        for summary in summaries:
                            if "Dens" in  row.MetricName:
                                row.MetricName = row.MetricName[:-4]
                            sum_col_cat = (row.MetricName +
                                           summary +
                                           "Cat" +
                                           a_m).replace("M3","")
                            sum_col_ws = (row.MetricName +
                                          summary +
                                          "Ws" +
                                          a_m).replace("M3","")
                            tbl[sum_col_cat] = (tbl["Cat" + summary] /
                                               weighted_cat_area)
                            tbl[sum_col_ws] = (tbl["Ws" + summary] /
                                               weighted_ws_area)
                            cat_sums.append(sum_col_cat)
                            ws_sums.append(sum_col_ws)
                    if table in ["RoadStreamCrossings", "CanalDensity"]:
                        tbl[cat_colname] = (tbl.CatSum / 
                                           weighted_cat_area *
                                           row.Conversion)
                        tbl[ws_colname] = (tbl.WsSum /
                                           weighted_ws_area *
                                           row.Conversion)
                    else:
                        tbl[cat_colname] = (tbl["CatCount%s" % a_m] /
                                           weighted_cat_area *
                                           row.Conversion)
                        tbl[ws_colname] = (tbl["WsCount%s" % a_m] /
                                           weighted_ws_area *
                                           row.Conversion)
                    if summaries:
                        end_cols = ([cat_colname] +
                                    [x.strip("M3") for x in cat_sums] +
                                    [ws_colname] +
                                    [x.strip("M3") for x in ws_sums])
                    else:
                        end_cols = [cat_colname, ws_colname]
                    if metric_count == 0:
                        final = tbl[front_cols + end_cols].copy()
                    else:
                        tbl = tbl[["COMID"] + end_cols]
                        final = pd.merge(final, tbl, on="COMID")

                if row.MetricType == "Percent":
                    lookup = pd.read_csv(row.MetricName)
                    cat_named = ["Pct{}Cat{}".format(x, a_m)
                                for x in lookup.final_val.values]
                    ws_named = ["Pct{}Ws{}".format(x, a_m)
                                for x in lookup.final_val.values]
                    catcols,wscols = [],[]
                    for col in tbl.columns:
                        if "CatVALUE" in col and not "Up" in col:
                            tbl[col] = ((tbl[col] * 1e-6)/weighted_cat_area*100)
                            catcols.append(col)
                        if "WsVALUE" in col:
                            tbl[col] = ((tbl[col] * 1e-6)/weighted_ws_area*100)
                            wscols.append(col)
                    if metric_count == 0:
                        final = tbl[front_cols + catcols + wscols]
                        final.columns = front_cols + cat_named + ws_named
                    else:
                        final2 = tbl[["COMID"] + catcols + wscols]
                        final2.columns = ["COMID"] + cat_named + ws_named
                        final = pd.merge(final,final2,on="COMID")

            final = final.set_index("COMID")
            if len(final[np.isinf(final)].stack().dropna()) > 0:
                # inf values in dams layer - vpu 01 remove
                final = final.replace([np.inf, -np.inf], np.nan) 
<<<<<<< HEAD
            if zone == '04':
                rmtbl = pd.read_csv('L:/Priv/CORFiles/Geospatial_Library_Projects/StreamCat/FTP_Staging/Documentation/DataProcessingAndQualityAssurance/QA_Files/ProblemStreamsR04.csv')[['COMID']]
                final = final.drop(rmtbl.COMID.tolist(),axis=0)
            if zone == '06':
                stats = {}
                for c in final.columns.tolist():
                    stats[c] = {'min': final[c].min(), 'max':final[c].max()}
            if zone != '06':
                try:
                    stats
                except NameError:
                    pass
                else:
                    for c in final.columns.tolist():
                        if final[c].min() < stats[c]['min']:
                            stats[c]['min'] = final[c].min()
                        if final[c].max() > stats[c]['max']:
                            stats[c]['max'] = final[c].max()
            final = final.fillna('NA')
            final = final[final.columns.tolist()[:5] + [x for x in final.columns[5:] if 'Cat' in x] + [x for x in final.columns[5:] if 'Ws' in x]].fillna('NA')
            if 'ForestLossByYear0013' in table:
                final.drop([col for col in final.columns if 'NoData' in col], axis=1, inplace=True)
            if not LENGTHS[zone] == len(final):
                print "Table %s length zone %s incorrect!!!!...check Allocation\
                        and Accumulation results" % (table, zone)
            final.to_csv(outDir  + '/%s_Region%s.csv'%(table,zone))
    print table
    try:
        stats
    except NameError:
        pass
    else:
        for stat in stats:
            print stat + ' ' + str(stats[stat])
    print 'All Done.....'
=======
            if vpu == "04":
                rmtbl = pd.read_csv("L:/Priv/CORFiles/Geospatial_Library_Projects/StreamCat/FTP_Staging/Documentation/DataProcessingAndQualityAssurance/QA_Files/ProblemStreamsR04.csv")[["COMID"]]
                final = final.drop(rmtbl.COMID.tolist())

            stats = build_stats(final, stats)
            final = final.fillna("NA")

            # pretty sure these next to if stmnts could go away if we remove
            # these values from the lookup tables
            if table == "AgMidHiSlopes":
                droppers = [x for x in final.columns
                            if "Unknown" in x]
                final.drop(droppers, axis=1, inplace=True)
            if table == "ForestLossByYear0013":
                droppers = [col for col in final.columns if "NoData" in col]
                final.drop(droppers, axis=1, inplace=True)

            if not LENGTHS[vpu] == len(final):
                print(LENGTH_ERROR_MESSAGE.format(table, vpu))
            final.to_csv(out_file)

        # ZIP up every region as we write them out
        zip_name = out_file.name.replace("csv","zip")
        zf = zipfile.ZipFile(str(FINAL_DIR / "zips" / zip_name), mode="w")
        zf.write(str(out_file), out_file.name,
                 compress_type=zipfile.ZIP_DEFLATED)
        zf.close()

    # Make the state tables
    for state in states_dict:
        state_tbl = pd.DataFrame()
        keepers = states_dict[state]["COMIDs"]
        for vpu in states_dict[state]["VPUs"]:
            vpu_tbl = pd.read_csv(FINAL_DIR  / region_fn.format(table, vpu))
            vpu_tbl.query("COMID in @keepers", inplace=True)
            state_tbl = state_tbl.append(vpu_tbl)
        state_tbl.to_csv(STATES_DIR / fn.format(table, state), index=False)

    print(table)

    for stat in stats:
        print stat + " " + str(stats[stat])
    print "All Done....."

###########################
#table = "RoadStreamCrossings"
#aa = []
#for f in os.listdir(str(REDO_DIR)):
#    s = f.split("_Region")[0]
#    if not s in aa:
#        aa.append(f.split("_Region")[0])
#FINAL_DIR = Path("L:/Priv/CORFiles/Geospatial_Library_Projects/StreamCat/FTP_Staging/HydroRegions")
#for table in tables:
#for table in aa[49:-1]:
#    print(table)
#    for vpu in inputs:
#        print(vpu)
#        orig = pd.read_csv(FINAL_DIR / region_fn.format(table,vpu))
#        new = pd.read_csv(REDO_DIR / region_fn.format(table,vpu))
#        if not orig.equals(new):
#            print(table, vpu, orig.equals(new))

#for col in orig.columns:
#    print(col, (orig[col] == new[col]).all())
#    if not (orig[col] == new[col]).all():
#        break
#
#qq = pd.merge(orig[["COMID", col]], new[["COMID", col]],
#              on="COMID", suffixes=("_orig", "_new"))


#for state in states_dict:
#
#    f = fn.format(table, state)
#    orig = pd.read_csv(STATES_DIR / f)
#    new = pd.read_csv(REDO_STATES / f)
#    print(table, state, orig.equals(new))









>>>>>>> 465b8b5d4a242d0861bcd3f9181121b4f23b1842
