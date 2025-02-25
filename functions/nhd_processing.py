# Functions related to NHD data like creating dictionaries and zonal processing
# TODO merge parallelization from speedup branch
import os
import numpy as np
import pandas as pd
from collections import OrderedDict, defaultdict, deque
from utils import dbf2df

class NHDProcessing:
    def __init__(self, nhd_dir):
        self.nhd = nhd_dir
    
    @staticmethod
    def nhd_dict(nhd, unit="VPU", user_zones=None):
        inputs = OrderedDict()
        if user_zones:
            inputs |= user_zones
            np.save("./accum_npy/vpu_inputs.npy", inputs)
            return inputs
        bounds = pd.read_csv(f"{nhd}/NHDPlusGlobalData/BoundaryUnit.csv")
        remove = bounds.loc[bounds.DRAINAGEID.isin(["HI", "CI"])].index
        bounds = bounds.drop(remove, axis=0)
        if unit == "VPU":
            vpu_bounds = bounds.loc[bounds.UNITTYPE == "VPU"].sort_values("HYDROSEQ", ascending=False)
            for _, row in vpu_bounds.iterrows():
                inputs[row.UNITID] = row.DRAINAGEID
            np.save("./accum_npy/vpu_inputs.npy", inputs)
            return inputs

    @staticmethod
    def make_all_cat_comids(nhd, inputs):
        all_comids = np.array([], dtype=np.int32)
        for zone, hr in inputs.items():
            cats = pd.read_csv(f"{nhd}/NHDPlus{hr}/NHDPlus{zone}/NHDPlusCatchment/Catchment.csv")
            all_comids = np.append(all_comids, cats.FEATUREID.values.astype(int))
        np.savez_compressed("./accum_npy/allCatCOMs.npz", all_comids=all_comids)
        return set(all_comids)
    
    @staticmethod
    def children(token, tree, chkset=None):
        """
        Returns a list of every child.

        Arguments
        ---------
        token: int
            a single COMID
        tree: dict
            Full dictionary of list of upstream COMIDs for each COMID in the zone
        chkset: set
            set of all the NHD catchment COMIDs used to remove flowlines with no associated catchment
        """
        visited = set()
        to_crawl = deque([token])
        while to_crawl:
            current = to_crawl.popleft()
            if current in visited:
                continue
            visited.add(current)
            node_children = set(tree[current])
            to_crawl.extendleft(node_children - visited)
        if chkset is not None:
            visited = visited.intersection(chkset)
        return list(visited)

    @staticmethod
    def bastards(token, tree):
        """
        Returns a list of every child without the father (key) included.

        Arguments
        ---------
        token: int
            a single COMID
        tree: dict
            Full dictionary of list of upstream COMIDs for each COMID in the zone
        """
        visited = set()
        to_crawl = deque([token])
        while to_crawl:
            current = to_crawl.popleft()
            if current in visited:
                continue
            visited.add(current)
            node_children = set(tree[current])
            to_crawl.extendleft(node_children - visited)
        visited.remove(token)
        return list(visited)
    
    def makeNumpyVectors(self, inter_tbl, user_zones):
        """
        Uses the NHD tables to create arrays of upstream catchments which are used
        in the Accumulation function

        Arguments
        ---------
        inter_tbl   : table of inter-VPU connections
        nhd         : directory where NHD is stored
        """
        os.mkdir("accum_npy")
        inputs = self.nhd_dict(self.nhd, user_zones=user_zones)
        all_comids = self.make_all_cat_comids(self.nhd, inputs)
        print("Making numpy files in zone...", end="", flush=True)
        for zone, hr in inputs.items():
            print(zone, end=", ", flush=True)
            pre = f"{self.nhd}/NHDPlus{hr}/NHDPlus{zone}"
            flow = dbf2df(f"{pre}/NHDPlusAttributes/PlusFlow.dbf")[["TOCOMID", "FROMCOMID"]]
            flow = flow[(flow.TOCOMID != 0) & (flow.FROMCOMID != 0)]
            fls = dbf2df(f"{pre}/NHDSnapshot/Hydrography/NHDFlowline.dbf")
            coastfl = fls.COMID[fls.FTYPE == "Coastline"]
            flow = flow[~flow.FROMCOMID.isin(coastfl.values)]
            # remove these FROMCOMIDs from the 'flow' table, there are three COMIDs
            # here that won't get filtered out any other way
            flow = flow[~flow.FROMCOMID.isin(inter_tbl.removeCOMs)]
            # find values that are coming from other zones and remove the ones that
            # aren't in the interVPU table
            out = np.setdiff1d(flow.FROMCOMID.values, fls.COMID.values)
            out = out[
                np.nonzero(out)
            ]  # this should be what combines zones and above^, but we force connections with inter_tbl
            flow = flow[
                ~flow.FROMCOMID.isin(np.setdiff1d(out, inter_tbl.thruCOMIDs.values))
            ]
            # Table is ready for processing and flow connection dict can be created
            flow_dict = defaultdict(list)
            for _, row in flow.iterrows():
                flow_dict[row.TOCOMID].append(row.FROMCOMID)
            # add IDs from UpCOMadd column if working in ToZone, forces the flowtable connection though not there
            for interLine in inter_tbl.values:
                if interLine[6] > 0 and interLine[2] == zone:
                    flow_dict[int(interLine[6])].append(int(interLine[0]))
            out_of_vpus = inter_tbl.loc[
                (inter_tbl.ToZone == zone) & (inter_tbl.DropCOMID == 0)
            ].thruCOMIDs.values
            cats = dbf2df(f"{pre}/NHDPlusCatchment/Catchment.dbf").set_index("FEATUREID")
            comids = cats.index.values
            comids = np.append(comids, out_of_vpus)
            # list of upstream lists, filter comids in all_comids
            ups = [list(all_comids.intersection(self.bastards(x, flow_dict))) for x in comids]
            lengths = np.array([len(u) for u in ups])
            upstream = np.hstack(ups).astype(np.int32)  # Convert to 1d vector
            assert len(ups) == len(lengths) == len(comids)
            np.savez_compressed(
                f"./accum_npy/accum_{zone}.npz",
                comids=comids,
                lengths=lengths,
                upstream=upstream,
            )