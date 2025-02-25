# General Utility functions
# TODO add functions from the util folder here that are still used / necessary
import pandas as pd
import pyogrio

def dbf2df(f, upper=True):
    data = pyogrio.read_dataframe(f, read_geometry=False, use_arrow=True)
    # data = pd.read_csv(f)
    # if "geometry" in data:
    #     data.drop("geometry", axis=1, inplace=True)
    if upper:
        data.columns = data.columns.str.upper()
    return data