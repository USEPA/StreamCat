import os
import click
import numpy as np
import pandas as pd
from pathlib import Path as P


def longest_decimal(col):
    if col.dtype.kind == "i":
        return 0
    return col.astype(str).str.split(".", expand=True)[1].str.len().max()


def update_stats(d, s, c):
    for stat in d.keys():
        if stat ==  "min" and d[stat] < s[c][stat]:
            s[c][stat] = d[stat]
        if stat ==  "max" and d[stat] > s[c][stat]:
            s[c][stat] = d[stat]
        if stat ==  "precision_digits" and d[stat] > s[c][stat]:
            s[c][stat] = d[stat]


def build_stats(tbl, stats):
    for col in tbl.columns.tolist():
        data = dict(
            min=tbl[col].min(),
            max=tbl[col].max(),
            precision_digits=longest_decimal(tbl[col]),
            dtype=str(tbl[col].dtype)
        )
        if not col in stats.keys():
            stats[col] = data
        else:
            update_stats(data, stats, col)
    return stats


@click.command()
@click.option(
    "--allocation", "-a",
    default=False,
    show_default=True,
    is_flag=True,
    help="check against allocation tables, default is final",
)
@click.option(
    "--out", "-o",
    default=False,
    show_default=True,
    is_flag=True,
    help="check against allocation tables, default is final",
)
def gather_stats(final):
    """Create stats for tables produced in StreamCat.

    \b

    example:

        `$ python precision.py -f`
    """
    ACC = "Allocation_and_Accumulation"
    FTP = "FTP_Staging/HydroRegions"
    ODV = P("O:/PRIV/CPHEA/PESD/COR/CORFiles/Geospatial_Library_Projects/StreamCat")
    AST = "*"
    control = pd.read_csv("ControlTable_StreamCat.csv")

    alloc_dir = ODV.joinpath(FTP if final else ACC)
    INPUTS = np.load("accum_npy/vpu_inputs.npy", allow_pickle=True).item()
    name = "Final_Table_Name" if final else "FullTableName"

    for metric in control[name]:
        stats = dict()
        print("Checking --", metric)
        for zone in INPUTS:
            und = "" if final else "_"
            fn = f"{metric}{und}{zone}.csv"
            print(zone, end="..", flush=True)
            tbl = pd.read_csv(alloc_dir / fn).set_index("COMID")
            stats = build_stats(tbl, stats)
        print("done!")
        if not os.path.exists("allocation_stats.txt"):
            with open("allocation_stats.txt", "w") as fifi:
                pass
        with open("allocation_stats.txt", 'a') as fifi:
            fifi.write(f"{AST*49}\n")
            fifi.write(f"!{metric:=^{47}}!\n")
            fifi.write(f"{AST*49}\n")
            for key, value in stats.items():
                fifi.write(f"@ {key}\n")
                for k, v in value.items():
                    fifi.write(f"\t- {k}: {v}\n")


if __name__ == "__main__":

    gather_stats()

