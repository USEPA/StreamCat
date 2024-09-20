import dask_geopandas as dg
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
import os
import pyogrio

def load_dask_geodb(path):
    file_list = [path +'/' + file for file in os.listdir('high_res_data') if file.endswith('.gdb')]
    df = pyogrio.read_dataframe(file_list[0])
    print(df.head())
    layers = pyogrio.list_layers(file_list[0])
    print(layers)
    geo_db_list = [gpd.read_file(file, engine="pyogrio") for file in file_list]

    dask_dfs = [dg.from_geopandas(gdf, npartitions=1) for gdf in geo_db_list]
    dask_gdf = dd.concat(dask_dfs)
    print(dask_gdf)
    return dask_gdf


def load_dask_geopackage(path, layers):
    # num_dataframes = len(os.listdir(path))
    # dask_geo_df = dg.read_file(os.path.join(path, "*.gpkg"), npartitions=num_dataframes)
    
    # List all .gpkg files in the high_res_data folder
    file_list = [path +'/' + file for file in os.listdir('high_res_data') if file.endswith('.gpkg')]

    #geo_list = [gpd.read_file(file, layer=layers[0], engine="pyogrio") for file in file_list]
    #layers = layers.pop(0)
    geo_list = []
    for layer in layers:
        for file in file_list:
            geo_list.append(gpd.read_file(file, layer=layer, engine="pyogrio"))
    # could apply pandas concat here
    # then do dg.from_geopandas on the result of pd.concat
    # layers = pyogrio.list_layers(file_list[0])
    # print(layers)
    
    dask_geo_list = [dg.from_geopandas(gdf, npartitions=1) for gdf in geo_list]
    dask_gdf = dd.concat(dask_geo_list)
    print(dask_gdf)
    return dask_gdf


if __name__ == '__main__':
    important_layers = ['NHDPlusCatchment', 'NHDPlusFlow', 'NHDPlusFlowlineVAA']
    data_path = 'high_res_data/NHDPlus_H_National_Release_1_GDB/NHDPlus_H_National_Release_1_GDB.gdb'
    print(pyogrio.list_layers(data_path))
    df = dg.read_file(data_path, npartitions=64, layer='NHDPlusCatchment')
    print(df)
    # length = len(df)
    # length = df['nhdplusid'].count().compute()

    # GDB Test
    # dask_gdb = load_dask_geodb('high_res_data')

    # Gpkg Test
    geo_ddf = load_dask_geopackage('high_res_data', important_layers)
    geo_ddf.describe()
    #TODO iterate through layers here and create larger dataframe of all necessary
    unique_layers = geo_ddf.columns
    print(unique_layers)
    for p in range(geo_ddf.npartitions):
        partition = geo_ddf.get_partition(p)