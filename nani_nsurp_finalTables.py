# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 12:06:12 2021

This script is used to process FinalTables for the hybridized metrics 
that MPennino has utilized for his paper. Each of these are based on other
previously produced metrics with some simple table math to arrive at the final
metric.

@author: Rdebbout
"""

import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from stream_cat_config import FINAL_DIR, LENGTHS, OUT_DIR


def build_stats(tbl, stats):
    if not stats:
        for c in tbl.columns.tolist():
            stats[c] = {"min": tbl[c].min(), "max": tbl[c].max()}
        return stats
    for col in tbl.columns.tolist():
        if tbl[col].min() < stats[col]["min"]:
            stats[col]["min"] = tbl[col].min()
        if tbl[col].max() > stats[col]["max"]:
            stats[col]["max"] = tbl[col].max()
    return stats


LENGTH_ERROR_MESSAGE = (
    "Table {} length vpu {} incorrect!!!!"
    "...check Allocation and Accumulation results"
)

OUT_DIR = Path(OUT_DIR)
FINAL_DIR = Path(FINAL_DIR)
ctl = pd.read_csv("ControlTable_StreamCat.csv")

inputs = np.load("accum_npy/vpu_inputs.npy", allow_pickle=True).item()

states_lookup = Path("state_dict.npz")
states_dict = np.load(str(states_lookup), allow_pickle=True, encoding="latin1")[
    "data"
].item()

STATES_DIR = FINAL_DIR.parents[0] / "States"

region_fn = "{}_Region{}.csv"
stats = dict()

for vpu in inputs:
    out_file = FINAL_DIR / region_fn.format("Nsurp_NANI", vpu)
    # Check if output tables exist before writing
    if not out_file.exists():
        tbl = pd.read_csv(FINAL_DIR / f"AgriculturalNitrogen_Region{vpu}.csv")

        front_cols = [
            title
            for title in tbl.columns
            for x in ["COMID", "AreaSqKm"]
            if x in title and not "Up" in title
        ]

        tbl["Fert06_kg_Cat"] = tbl.FertCat * tbl.CatAreaSqKm * 100
        tbl["Fert06_kg_Ws"] = tbl.FertWs * tbl.WsAreaSqKm * 100
        tbl["Manure06_kg_Cat"] = tbl.ManureCat * tbl.CatAreaSqKm * 100
        tbl["Manure06_kg_Ws"] = tbl.ManureWs * tbl.WsAreaSqKm * 100
        tbl["CBNF06_kg_Cat"] = tbl.CBNFCat * tbl.CatAreaSqKm * 100
        tbl["CBNF06_kg_Ws"] = tbl.CBNFWs * tbl.WsAreaSqKm * 100

        tbl["Livestock_N_Content_kg_Cat"] = tbl.Manure06_kg_Cat * 0.25 * 1.37
        tbl["Livestock_N_Content_kg_Ws"] = tbl.Manure06_kg_Ws * 0.25 * 1.37
        tbl["Livestock_N_Demand_kg_Cat"] = tbl.Manure06_kg_Cat * 1.37
        tbl["Livestock_N_Demand_kg_Ws"] = tbl.Manure06_kg_Ws * 1.37

        t = tbl[
            [
                "COMID",
                "CatAreaSqKm",
                "WsAreaSqKm",
                "Fert06_kg_Cat",
                "Fert06_kg_Ws",
                "Manure06_kg_Cat",
                "Manure06_kg_Ws",
                "CBNF06_kg_Cat",
                "CBNF06_kg_Ws",
                "Livestock_N_Content_kg_Cat",
                "Livestock_N_Content_kg_Ws",
                "Livestock_N_Demand_kg_Cat",
                "Livestock_N_Demand_kg_Ws",
            ]
        ]

        urb_fert = pd.read_csv(OUT_DIR / f"N_Urb_Fert_{vpu}.csv")
        urb_fert["UrbFert_kg_Cat"] = urb_fert.CatSum / 100_000
        urb_fert["UrbFert_kg_Ws"] = urb_fert.WsSum / 100_000
        t = pd.merge(
            t, urb_fert[["COMID", "UrbFert_kg_Cat", "UrbFert_kg_Ws"]], on="COMID"
        )

        tdep = pd.read_csv(FINAL_DIR / f"TDEP_Region{vpu}.csv")
        tdep["TNDep06_kg_Cat"] = tdep.N_TW2006Cat * tdep.CatAreaSqKm * 100
        tdep["TNDep06_kg_Ws"] = tdep.N_TW2006Ws * tdep.WsAreaSqKm * 100
        tdep["NOXI06_kg_Cat"] = tdep.NOXI_TW2006Cat * tdep.CatAreaSqKm * 100
        tdep["NOXI06_kg_Ws"] = tdep.NOXI_TW2006Ws * tdep.WsAreaSqKm * 100
        t = pd.merge(
            t,
            tdep[
                [
                    "COMID",
                    "TNDep06_kg_Cat",
                    "TNDep06_kg_Ws",
                    "NOXI06_kg_Cat",
                    "NOXI06_kg_Ws",
                ]
            ],
            on="COMID",
        )

        popden = pd.read_csv(FINAL_DIR / f"USCensus2010_Region{vpu}.csv")
        popden["HumanWaste_kg_Cat"] = popden.PopDen2010Cat * popden.CatAreaSqKm * 4.7
        popden["HumanWaste_kg_Ws"] = popden.PopDen2010Ws * popden.WsAreaSqKm * 4.7
        popden["Human_N_Demand_kg_Cat"] = (
            popden.PopDen2010Cat * popden.CatAreaSqKm * 6.21
        )
        popden["Human_N_Demand_kg_Ws"] = popden.PopDen2010Ws * popden.WsAreaSqKm * 6.21
        t = pd.merge(
            t,
            popden[
                [
                    "COMID",
                    "HumanWaste_kg_Cat",
                    "HumanWaste_kg_Ws",
                    "Human_N_Demand_kg_Cat",
                    "Human_N_Demand_kg_Ws",
                ]
            ],
            on="COMID",
        )

        urb_rmv = pd.read_csv(OUT_DIR / f"N_rmv_{vpu}.csv")
        urb_rmv["crop_N_rmv_kg_Cat"] = urb_rmv.CatSum / 10_000
        urb_rmv["crop_N_rmv_kg_Ws"] = urb_rmv.WsSum / 10_000
        t = pd.merge(
            t,
            urb_rmv[["COMID", "crop_N_rmv_kg_Cat", "crop_N_rmv_kg_Ws"]],
            on="COMID",
        )

        t["NsurpCat"] = (
            t.Fert06_kg_Cat
            + t.UrbFert_kg_Cat
            + t.TNDep06_kg_Cat
            + t.CBNF06_kg_Cat
            + t.Manure06_kg_Cat
            + t.HumanWaste_kg_Cat
        ) - t.crop_N_rmv_kg_Cat
        t["NsurpWs"] = (
            t.Fert06_kg_Ws
            + t.UrbFert_kg_Ws
            + t.TNDep06_kg_Ws
            + t.CBNF06_kg_Ws
            + t.Manure06_kg_Ws
            + t.HumanWaste_kg_Ws
        ) - t.crop_N_rmv_kg_Ws
        t["NANICat"] = (
            t.Fert06_kg_Cat
            + t.UrbFert_kg_Cat
            + t.CBNF06_kg_Cat
            + t.NOXI06_kg_Cat
            + t.Human_N_Demand_kg_Cat
            + t.Livestock_N_Demand_kg_Cat
            - t.crop_N_rmv_kg_Cat
            - t.Livestock_N_Content_kg_Cat
        )
        t["NANIWs"] = (
            t.Fert06_kg_Ws
            + t.UrbFert_kg_Ws
            + t.CBNF06_kg_Ws
            + t.NOXI06_kg_Ws
            + t.Human_N_Demand_kg_Ws
            + t.Livestock_N_Demand_kg_Ws
            - t.crop_N_rmv_kg_Ws
            - t.Livestock_N_Content_kg_Ws
        )

        final = t[front_cols + ["NsurpCat", "NsurpWs", "NANICat", "NANIWs"]]

        final = final.set_index("COMID")

        stats = build_stats(final, stats)
        final.fillna("NA", inplace=True)

        if not LENGTHS[vpu] == len(final):
            print(LENGTH_ERROR_MESSAGE.format(table, vpu))
        final.to_csv(out_file)

    # ZIP up every region as we write them out
    zip_name = out_file.name.replace("csv", "zip")
    zf = zipfile.ZipFile(str(FINAL_DIR / "zips" / zip_name), mode="w")
    zf.write(str(out_file), out_file.name, compress_type=zipfile.ZIP_DEFLATED)
    zf.close()

# Make the state tables
for state in states_dict:
    print(state)
    state_tbl = pd.DataFrame()
    keepers = states_dict[state]["COMIDs"]
    state_file = STATES_DIR / fn.format(table, state)
    for vpu in states_dict[state]["VPUs"]:
        vpu_tbl = pd.read_csv(FINAL_DIR / region_fn.format(table, vpu))
        vpu_tbl.query("COMID in @keepers", inplace=True)
        state_tbl = state_tbl.append(vpu_tbl)
    state_tbl.to_csv(state_file, index=False)

    # ZIP up every state as we write them out
    zip_name = state_file.name.replace("csv", "zip")
    zf = zipfile.ZipFile(str(STATES_DIR / "zips" / zip_name), mode="w")
    zf.write(str(state_file), state_file.name, compress_type=zipfile.ZIP_DEFLATED)
    zf.close()

for stat in stats:
    print(stat + " " + str(stats[stat]))
print("All Done.....")
