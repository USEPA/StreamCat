import pandas as pd
from database import DatabaseConnection
import os

if __name__ == '__main__':
    db_conn = DatabaseConnection()
    db_conn.connect()
    print("Connected to DB!")
    # Get sc_states
    # For each parquet join with those two dfs based on comid
    sc_datasets = db_conn.GetTableAsDf('sc_datasets')
    sc_datasets.drop(['dsid', 'dsname', 'active'], axis=1, inplace=True)

    sc_catchments = db_conn.GetTableAsDf('sc_catchments_2')
    print("Table sc_catchments_2 retrieved")
    sc_catchments.set_index('COMID')

    sc_states = db_conn.GetTableAsDf('sc_states_2')
    sc_states.set_index('COMID')
    print("Table sc_states_2 retrieved")
    #sc_state_region_area = sc_catchments.join(sc_states, on='comid')
    sc_state_region_area = pd.merge(sc_catchments, sc_states, on="COMID")

    # Get parquet file paths for hydroregions
    parq_file_dir = "O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/FTP_Staging/HydroRegions"
    parquet_files = [f for f in os.listdir(parq_file_dir) if f.count('.parquet') and not f.count('Archive') and not f.count('zips')]

    # Define output directory
    OUT_DIR = "O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/FTP_Staging/FinalTables"

    # Create dictionary where key = metric name and value = List[hydroregion file paths]
    final_tables = {}
    for file in parquet_files:
        metricname = file.split("_Region")[0]
        if metricname not in final_tables.keys():
            final_tables[metricname] = [file]
        else:
            final_tables[metricname].append(file)
            

    # For each in the dictionary created above concat parquets into one dataframe and write to output directory
    for metric, file_list in final_tables.items():
        dfs = []
        for file in file_list:
            dfs.append(pd.read_parquet(f"{parq_file_dir}/{file}"))
        df = pd.concat(dfs, ignore_index=True)
        final_df = df.merge(sc_state_region_area, left_on='COMID', right_on='COMID')
        final_df.to_parquet(f"{OUT_DIR}/{metric}.parquet", index=False)
        print(f"Wrote metric: {metric} to output dir\n")

    



