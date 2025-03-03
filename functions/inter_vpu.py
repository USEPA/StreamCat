# Functions for creating / adjusting inter vpu connections
import pandas as pd
import numpy as np
import os

class InterVPU:
    @staticmethod
    def adjust_coms(tbl, comid1, comid2, tbl2=None):
        """Adjust values for COMIDs where values from one need to be subtracted from another. 
            Depending on the type of accumulation, subtracts values for each column in the table other than COMID and PctFull

        Args:
            tbl (pd.DataFrame): throughVPU table from InterVPU function
            comid1 (int): COMID which will be adjusted
            comid2 (int): COMID whose values will be subtracted from comid1
            tbl2 (pd.DataFrame, optional): toVPU table from InterVPU function in the case where a COMID comes from a different zone. Defaults to None.
        """
        if tbl2 is None:
            tbl2 = tbl.copy()
        for idx in tbl.columns[:-1]:
            tbl.loc[comid1, idx] -= tbl2.loc[comid2, idx]
    
    @staticmethod
    def appendConnectors(cat, Connector, zone, interVPUtbl):
        """Appends the connector file of inter VPU COMIDS to the cat table before going into accumulation process

        Args:
            cat (pd.DataFrame): Results table of catchment summarization
            Connector (str): string to file holding the table of inter VPU COMIDs
            zone (str): string of an NHDPlusV2 VPU zone, i.e. 10L, 16, 17
            interVPUtbl (pd.DataFrame): table of interVPU adjustments
        """
        con = pd.read_csv(Connector)
        for comidx in con.COMID.values.astype(int):
            if comidx in cat.COMID.values.astype(int):
                cat = cat.drop(cat[cat.COMID == comidx].index)
        con = con.loc[
            con.COMID.isin(
                np.append(
                    interVPUtbl.loc[interVPUtbl.ToZone.values == zone].thruCOMIDs.values,
                    interVPUtbl.loc[interVPUtbl.ToZone.values == zone].toCOMIDs.values[
                        np.nonzero(
                            interVPUtbl.loc[
                                interVPUtbl.ToZone.values == zone
                            ].toCOMIDs.values
                        )
                    ],
                )
            )
        ]

        cat = pd.concat([cat, con], axis=0, ignore_index=False)   
        return cat.reset_index(drop=True)

    @staticmethod
    def inter_vpu(tbl, cols, zone, connector, inter_vpu_tbl):
        """Loads watershed values for given COMIDs to be appended to catResults table for accumulation.

        Args:
            tbl (pd.DataFrame): Watershed results table
            cols (list[str]): List of dataframe columns for Cat Results table needed to overwrite onto Connector table
            zone (str): an NHDPlusV2 VPU number, i.e. 10, 16, 17
            connector (str: Location of the connector file
            inter_vpu_tbl (pd.DataFrame): table of interVPU exchanges
        """
        through_vp_us = (
            tbl[tbl.COMID.isin(inter_vpu_tbl.thruCOMIDs.values)].set_index("COMID").copy()
        )
        inter_vpu_tbl = inter_vpu_tbl.loc[inter_vpu_tbl.FromZone.values == zone]
        through_vp_us.columns = cols
        if any(inter_vpu_tbl.toCOMIDs.values > 0):
            inter_alloc = f"{connector[:connector.find('_connectors')]}_{inter_vpu_tbl.ToZone.values[0]}.csv"
            tbl = pd.read_csv(inter_alloc).set_index("COMID")
            to_vp_us = tbl[tbl.index.isin([x for x in inter_vpu_tbl.toCOMIDs if x > 0])].copy()
        for _, row in inter_vpu_tbl.iterrows():
            if row.toCOMIDs > 0:
                InterVPU.adjust_coms(to_vp_us, int(row.toCOMIDs), int(row.thruCOMIDs), through_vp_us)
            if row.AdjustComs > 0:
                InterVPU.adjust_coms(through_vp_us, int(row.AdjustComs), int(row.thruCOMIDs), None)
            if row.DropCOMID > 0:
                through_vp_us = through_vp_us.drop(int(row.DropCOMID))
        if any(inter_vpu_tbl.toCOMIDs.values > 0):
            con = pd.read_csv(connector).set_index("COMID")
            con.columns = map(str, con.columns)
            to_vp_us = pd.concat([to_vp_us, con], axis=0, ignore_index=False)
            to_vp_us.to_csv(connector)
        if os.path.exists(connector):
            con = pd.read_csv(connector).set_index("COMID")
            con.columns = map(str, con.columns)
            through_vp_us = pd.concat([through_vp_us, con], axis=0, ignore_index=False)
        through_vp_us.to_csv(connector)