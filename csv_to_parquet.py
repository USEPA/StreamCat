# -*- coding: utf-8 -*-
"""
Created on Thu Dec 26 09:25:30 2024

@author: mweber
"""
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# assign file directory to generate parquet files
file_dir = "O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/FTP_Staging/HydroRegions"

# make a list of .csv files in the directory to convert
files = [f for f in os.listdir(file_dir) if f.count('.csv') and not f.count('Archive') and not f.count('zips')]

for f in files:
    print(f)
    # Read CSV file into a Pandas DataFrame
    df = pd.read_csv(file_dir + "/" + f)
    
    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df)
    
    # Strip the .csv from file name for write out
    outname = f.split('.')[0]
    
    # Write PyArrow Table to Parquet file
    pq.write_table(table, file_dir + "/" + outname + ".parquet")
