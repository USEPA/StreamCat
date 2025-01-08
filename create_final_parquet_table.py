import pandas as pd
from database import DatabaseConnection
import os

if __name__ == '__main__':
    db_conn = DatabaseConnection()
    db_conn.connect()
    print("Connected to DB!")
    # Get sc_states
    # For each parquet join with those two dfs based on comid
    # sc_datasets = db_conn.GetTableAsDf('sc_datasets')
    # sc_datasets.drop(['dsid', 'dsname', 'active'], axis=1, inplace=True)

    sc_catchments = db_conn.GetTableAsDf('sc_catchments_2')
    print("Table retrieved")
    sc_catchments.set_index('comid')
    #sc_catchments = sc_catchments.drop([''])
    sc_states = db_conn.GetTableAsDf('sc_states')
    sc_states.set_index('comid')
    print("Table retrieved")
    #sc_state_region_area = sc_catchments.join(sc_states, on='comid')
    sc_state_region_area = pd.merge(sc_catchments, sc_states, on="comid")

    parq_file_dir = "O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/FTP_Staging/HydroRegions"
    parquet_files = [f for f in os.listdir(parq_file_dir) if f.count('.parquet') and not f.count('Archive') and not f.count('zips')]

    OUT_DIR = "O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/FTP_Staging/FinalTables"

    final_tables = {}
    for file in parquet_files:
        metricname = file.split("_Region")[0]
        if metricname not in final_tables.keys():
            final_tables[metricname] = [file]
        else:
            final_tables[metricname].append(file)
            
    
    for metric, file_list in final_tables.items():
        dfs = []
        for file in file_list:
            dfs.append(pd.read_parquet(f"{parq_file_dir}/{file}"))
        df = pd.concat(dfs, ignore_index=True)
        final_df = df.merge(df, sc_state_region_area, on='comid')
        final_df.to_parquet(f"{OUT_DIR}/{metric}.parquet", index=False)

    



