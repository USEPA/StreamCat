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
COMID_VPU['VPU'].replace({4: '04', 5: '05', 6: '06', 7: '07', 8: '08',
             11: '11', 12: '12', 13: '13', 14: '14', 15: '15', 
             16: '16', 17: '17', 18: '18'}, inplace=True)

# array of unique VPUs
VPU = COMID_VPU['VPU'].unique()

# Nutrient file
#nut_dir = 'O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/NutrientInventory/Inputs/'
nut_dir = 'E:/WorkingData/To_Be_Flow_Accumulated/'
nut = pd.read_csv(nut_dir + 'ClimTerms_2012_10.csv')
cat_area = pd.read_csv('O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/NutrientInventory/Inputs/COMID_Scaled_AgVars.csv')
cat_area = cat_area[['COMID','CatAreaSqKm']]
cat_area.head()
# add VPU using lookup table
nut = pd.merge(nut, COMID_VPU, how='left', left_on=['COMID'], right_on=['COMID'])
nut = pd.merge(nut, cat_area, how='left', left_on=['COMID'], right_on=['COMID'])
nut = nut.drop('Unnamed: 0', axis=1)
# nut = nut.drop('...1', axis=1)
list(nut)

# select columns - this part we can modify to iterate through columns
final = nut[['COMID', 'SNOW_YrMean', 'CatAreaSqKm', 'VPU']]
final = final.rename(columns={'SNOW_YrMean': 'CatSum'})
final['CatCount'] = final['CatAreaSqKm']
final['CatPctFull'] = 100
final = final.set_axis(['COMID', 'CatSum', 'CatAreaSqKm','VPU', 'CatCount', 'CatPctFull'], axis=1)

for i in VPU:
    print(i)
    df = final[final['VPU'] == i]
    df = df.drop(columns=['VPU'])
    df.to_csv(nut_dir + '/Allocation_and_Accumulation/SNOW_YrMean_' + str(i) + '.csv',
              index=False)
