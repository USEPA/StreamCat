# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 13:11:33 2015
FinalTablesStates
@author: mweber
"""
import pandas as pd
import os

var = 'ICI_IWI_v2'
# Read in a state / hydro-region lookup table
lookupdir = 'L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat/StateLookup'
stateVPU = pd.read_csv(lookupdir + '/State_VPU.csv')
stateVPU.head()
# let's convert thet states / VPUs in pandas data frame to a dictionary
g = stateVPU.groupby('NAME')
state_lookup = g['UnitID'].unique().to_dict()

# Read in states
for i in os.listdir(lookupdir):
    if not i.count('State_VPU') and not i.count('states_lookup'):
        ST_NM = i.split('.')[0]
        state = pd.read_csv(lookupdir + '/' + i)
        count = 0
        for VPU in state_lookup[ST_NM]:
            if count == 0:
                outtable = pd.read_csv('L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat/FTP_Staging/StreamCat/HydroRegions/' + var + '_Region' + VPU + '.csv')
                outtable = outtable[outtable['COMID'].isin(state['COMID'])]
            if count > 0:
                temp = pd.read_csv('L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat/FTP_Staging/StreamCat/HydroRegions/' + var + '_Region' + VPU + '.csv')
                temp = temp[temp['COMID'].isin(state['COMID'])]
                outtable = outtable.append(temp, ignore_index = True)
            count+=1
        # grab state two letter code to use in writing out file
        St_Abbr = pd.read_csv(lookupdir + '/states_lookup.csv')
        St_Abbr_lookup = St_Abbr.set_index('STATE')['STATE_ABBR'].to_dict()
        outtable.to_csv('L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat/FTP_Staging/StreamCat/States/' + var + '_' + St_Abbr_lookup[ST_NM] + '.csv',index=False) 
        
