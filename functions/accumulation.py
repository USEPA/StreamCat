# Functions related to the accumulation process
# TODO merge parallel processes into this file
import numpy as np
import pandas as pd

class Accumulation:
    def __init__(self, comids, lengths, upstream):
        self.comids = comids
        self.lengths = lengths
        self.upstream = upstream

    @staticmethod
    def swapper(coms, upStream):
        bsort = np.argsort(coms)
        apos = np.searchsorted(coms[bsort], upStream)
        indices = bsort[apos]
        return indices

    def accumulate(self, tbl, tbl_type, icol="COMID"):
        np.seterr(all="ignore")
        coms = tbl[icol].values.astype("int32")
        indices = self.swapper(coms, self.upstream)
        cols = tbl.columns[1:]
        z = np.zeros(self.comids.shape)
        data = np.zeros((len(self.comids), len(tbl.columns)))
        data[:, 0] = self.comids
        accumulated_indexes = np.add.accumulate(self.lengths)[:-1]

        for index, column in enumerate(cols, 1):
            col_values = tbl[column].values.astype("float")
            all_values = np.split(col_values[indices], accumulated_indexes)
            if tbl_type == "Ws":
                all_values = np.array(
                    [np.append(val, col_values[idx]) for idx, val in enumerate(all_values)],
                    dtype=object,
                )
            if index == 1:
                area = all_values.copy()
            if "PctFull" in column:
                values = [
                    np.ma.average(np.nan_to_num(val), weights=w)
                    for val, w in zip(all_values, area)
                ]
            elif "MIN" in column or "MAX" in column:
                func = np.max if "MAX" in column else np.min
                initial = -999999 if "MAX" in column else 999999
                values = np.array([func(val, initial=initial) for val in all_values])
                values[self.lengths == 0] = col_values[self.lengths == 0]
            else:
                values = np.array([np.nansum(val) for val in all_values])
            data[:, index] = values
        data = data[np.in1d(data[:, 0], coms), :]
        outDF = pd.DataFrame(data)
        prefix = "UpCat" if tbl_type == "Up" else "Ws"
        outDF.columns = [icol] + [c.replace("Cat", prefix) for c in cols.tolist()]
        areaName = outDF.columns[outDF.columns.str.contains("Area")][0]
        no_area_rows, na_columns = (outDF[areaName] == 0), outDF.columns[2:]
        outDF.loc[no_area_rows, na_columns] = np.nan
        return outDF