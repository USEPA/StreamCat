import click
import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal
from pathlib import Path as P

from stream_cat_config import OUT_DIR, FINAL_DIR

ACC = "Allocation_and_Accumulation"
FTP = "FTP_Staging/HydroRegions"
ODV = P("O:/PRIV/CPHEA/PESD/COR/CORFiles/Geospatial_Library_Projects/StreamCat")
config_error = (
    "\n\tImproperly Configured!!!\n\n\tYour `stream_cat_config.py` file's "
    "`{}` value\n\n\twill need to point to a directory other than:\n\n\t> {}")
check_file_error = (
    "\n\tFile Missing!!!\n\n\tMetric --  `{}` does not exist in:\n\n\t> {}"
    )


@click.command()
@click.option(
        "--debug",
        "-d",
        show_default=True,
        is_flag=True,
        help="enter pdb debugger on fail",
        default=False,
        )
@click.option(
    "--precision", "-p",
    type=float,
    show_default=True,
    help="value threshold on comparison",
    default=0.00001,
)
@click.option(
    "--final", "-f",
    default=False,
    show_default=True,
    is_flag=True,
    help="check against final tables, default is alloc/accum",
)
@click.argument("metrics", nargs=-1)
def compare(debug, metrics, final, precision):
    """Assert that values from another run of StreamCat align with
    the base run that we have on th O: drive. It is imperative that 
    the 'OUT_DIR` or `FINAL_DIR` paths in `stream_cat_config.py` 
    point to a place other than where our data is on the O:

    \b
    assertions:
        * column names match
        * table lengths match
        * COMIDs are equivalent
        * equality across columns w/in `precision`

    example:

        `$ python check_metrics.py compare nlcd2006 Clay Dams`
    """

    check_dir = P(FINAL_DIR) if final else P(OUT_DIR)
    alloc_dir = ODV.joinpath(FTP if final else ACC)
    if (check_dir == alloc_dir or 
        check_dir.match("*Geospatial_Library_Projects*")):
        click.echo(
            config_error.format(
                "FINAL_DIR" if final else "OUT_DIR", check_dir))
        exit()
    INPUTS = np.load("accum_npy/vpu_inputs.npy", allow_pickle=True).item()
    for metric in metrics:
        print("Checking --", metric)
        for zone in INPUTS:
            und = "" if final else "_"
            fn = f"{metric}{und}{zone}.csv"
            if not (check_dir / fn).exists():
                click.echo(check_file_error.format(fn, OUT_DIR))
                exit()
            print(zone, end="..", flush=True)
            t1 = pd.read_csv(alloc_dir / fn).set_index("COMID").sort_index()
            t2 = pd.read_csv(check_dir / fn).set_index("COMID").sort_index()
            try:
                assert t1.index.equals(t2.index)
                assert t1.columns.equals(t2.columns)
                for col in t1.columns:
                    if "StorM3" in col: # N/A values won't compare in Dams summary
                        continue
                    assert_series_equal(t1[col],t2[col], check_dtype=False,
                            atol=precision, rtol=precision)
            except AssertionError as e:
                if debug:
                    print(e, f"\n\ncolumn: {col}\n")
                    import pdb
                    pdb.set_trace()
                else:
                    raise e
        print("good!")


if __name__ == "__main__":

    compare()
