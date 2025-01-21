# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 12:19:11 2024

@author: mweber
"""

# Import libraries
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Read in lookup table for COMIDs and Hydroregions
lookupdir = 'O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/'
COMID_VPU = pd.read_csv(lookupdir + 'COMID_HydroRegion.csv')

# Read in template set of StreamCat COMIDS and restrict our COMID_VPU COMIDS to just those COMIDS
StreamCat_template = pd.read_parquet('O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/FTP_Staging/FinalTables/STATSGO_Set1.parquet')
COMID_VPU = COMID_VPU[COMID_VPU['COMID'].isin(StreamCat_template['COMID'])]
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
nut_dir = 'O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/NutrientInventory/CountyCatResultsData/'
nut = pd.read_parquet(nut_dir + 'P_Fert_FarmCountyCatResults.parquet')

cat_area = pd.read_csv('O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/NutrientInventory/Inputs/COMID_Scaled_AgVars.csv')
cat_area = cat_area[['COMID','CatAreaSqKm']]
cat_area.head()
# add VPU using lookup table
nut = pd.merge(nut, COMID_VPU, how='right', left_on=['COMID'], right_on=['COMID'])
nut = pd.merge(nut, cat_area, how='left', left_on=['COMID'], right_on=['COMID'])
# nut = nut.drop('Unnamed: 0', axis=1)
# nut = nut.drop('...1', axis=1)

# select columns - this part we can modify to iterate through columns
nut.columns = nut.columns.str.replace('_Cat','')
cols = [i for i in nut.columns if i not in ["COMID", "VPU", "CatAreaSqKm"]]

for col in cols:
    final = nut[['COMID', col, 'CatAreaSqKm', 'VPU']]
    final = final.rename(columns={col: 'CatSum'})
    final['CatCount'] = final['CatAreaSqKm']
    final['CatSum'] = final['CatSum'] * final['CatCount']
    final['CatPctFull'] = 100
    final = final[['COMID', 'CatAreaSqKm', 'CatCount', 'CatSum', 'CatPctFull', 'VPU']]

    for i in VPU:
        print(i)
        df = final[final['VPU'] == i]
        df = df.drop(columns=['VPU'])
        table = pa.Table.from_pandas(df)
        pq.write_table(table, nut_dir + 'test/' + col + '_' + str(i) + '.parquet')
        #df.to_csv(nut_dir + '/Allocation_and_Accumulation/' + col + '_' + str(i) + '.csv',
        #          index=False)
