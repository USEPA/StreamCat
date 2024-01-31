# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 12:19:11 2024

@author: mweber
"""

# Import libraries
import pandas as pd

# Read in lookup table for COMIDs and Hydroregions
lookupdir = 'O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/'
COMID_VPU = pd.read_csv(lookupdir + 'COMID_HydroRegion.csv')

COMID_VPU.head()
# array of unique VPUs
VPU = COMID_VPU['VPU'].unique()

# Nutrient file
nut_dir = 'O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/NutrientInventory/Inputs/'
nut = pd.read_csv(nut_dir + 'COMID_Scaled_AgVars.csv')

# add VPU using lookup table
nut = pd.merge(nut, COMID_VPU, how='left', left_on=['COMID'], right_on=['COMID'])

# select columns - this part we can modify to iterate through columns
farm_fert = nut[['COMID', 'N_Fert_Farm_kg_Cat_Ag_2007', 'CatAreaSqKm', 'VPU']]

farm_fert = farm_fert.set_axis(['COMID', 'CatSum', 'CatCount', 'VPU'], axis=1)

for i in VPU:
    print(i)
    df = farm_fert[farm_fert['VPU'] == i]
    df = df.drop(columns=['VPU'])
    df.to_csv(nut_dir + '/ByHydroregion/FarmFert_' + str(i) + '.csv',
              , index=False)
