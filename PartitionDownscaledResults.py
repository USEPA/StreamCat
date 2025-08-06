# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 12:19:11 2024

@author: mweber
"""

# Import libraries
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

# Read in lookup table for COMIDs and Hydroregions
lookupdir = 'O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/'
COMID_VPU = pd.read_csv(lookupdir + 'COMID_HydroRegion.csv')

# Read in template set of StreamCat COMIDS and restrict our COMID_VPU COMIDS to just those COMIDS
alloc_dir = "O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/Allocation_and_Accumulation"
# Get a list of matching files
files = [alloc_dir + '/' + f for f in os.listdir(alloc_dir) if f.count('Clay') and not f.count('connectors')]
StreamCat_template = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
#COMID_VPU = COMID_VPU[COMID_VPU['COMID'].isin(StreamCat_template['COMID'])]
COMID_VPU.head()
COMID_VPU['VPU'].replace({4: '04', 5: '05', 6: '06', 7: '07', 8: '08', 
             9: '09', 11: '11', 12: '12', 13: '13', 14: '14', 15: '15', 
             16: '16', 17: '17', 18: '18'}, inplace=True)

# array of unique VPUs
VPU = COMID_VPU['VPU'].unique()
# Nutrient file
#nut_dir = 'O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/NutrientInventory/Inputs/'
# nut_dir = 'E:/WorkingData/To_Be_Flow_Accumulated/'
# nut = pd.read_csv(nut_dir + 'ClimTerms_2012_10.csv')
#nut_dir = 'O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/AmaliaHandler/'
# nut_dir = 'O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/NutrientInventory/CountyCatResultsData/'
# nut = pd.read_parquet(nut_dir + 'p_crop_remCountyCatResults.parquet')
nut = pd.read_parquet('L:/Public/salford/WetInt/streamcat_wetlandchains.parquet')
cat_area = StreamCat_template[['COMID','CatAreaSqKm']]
cat_area.head()
# add VPU using lookup table
nut = pd.merge(COMID_VPU, nut, how='left', left_on=['COMID'], right_on=['COMID'])
nut = pd.merge(nut, cat_area, how='left', left_on=['COMID'], right_on=['COMID'])
# nut = nut.drop('Unnamed: 0', axis=1)
# nut = nut.drop('...1', axis=1)

# select columns - this part we can modify to iterate through columns
nut.columns = nut.columns.str.replace('_Cat','')
cols = [i for i in nut.columns if i not in ["COMID", "VPU", "CatAreaSqKm"]]
# cols = cols[29:31]
for col in cols:
    final = nut[['COMID', col, 'CatAreaSqKm', 'VPU']]
    final = final.rename(columns={col: 'CatSum'})
    final['CatCount'] = 1
    final['CatSum'] = final['CatSum'] * final['CatCount']
    final['CatPctFull'] = 100
    final = final[['COMID', 'CatAreaSqKm', 'CatCount', 'CatSum', 'CatPctFull', 'VPU']]

    for i in VPU:
        print(i)
        df = final[final['VPU'] == i]
        df = df.drop(columns=['VPU'])
        table = pa.Table.from_pandas(df)
        # pq.write_table(table, nut_dir + 'Allocation_and_Accumulation/' + col + '_' + str(i) + '.parquet')
        pq.write_table(table, 'L:/Public/salford/WetInt/Allocation_and_Accumulation/' + col + '_' + str(i) + '.parquet')
        #df.to_csv(nut_dir + '/Allocation_and_Accumulation/' + col + '_' + str(i) + '.csv',
        #          index=False)
