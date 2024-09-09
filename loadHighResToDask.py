import dask_geopandas as dg
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
import os
import pyogrio

def load_dask_geo_df(path):
    # num_dataframes = len(os.listdir(path))
    # dask_geo_df = dg.read_file(os.path.join(path, "*.gpkg"), npartitions=num_dataframes)
    
    # List all .gpkg files in the high_res_data folder
    file_list = [path +'/' + file for file in os.listdir('high_res_data') if file.endswith('.gpkg')]

    geo_list = [gpd.read_file(file, engine="pyogrio") for file in file_list]
    # could apply pandas concat here
    # then do dg.from_geopandas on the result of pd.concat
    layers = pyogrio.list_layers(file_list[0])
    print(layers)
    
    dask_geo_list = [dg.from_geopandas(gdf, npartitions=1) for gdf in geo_list]
    dask_gdf = dd.concat(dask_geo_list)
    print(dask_gdf)
    return dask_gdf

def process_layer(df, layer_name):
    
    filtered_df = df[df['layer'] == layer_name]
    #TODO perform specific layer operations here
    return filtered_df


if __name__ == '__main__':
    geo_ddf = load_dask_geo_df('high_res_data')
    geo_ddf.describe()
    #TODO iterate through layers here
    unique_layers = geo_ddf.columns
    print(unique_layers)
    for p in range(geo_ddf.npartitions):
        partition = geo_ddf.get_partition(p)
    # Iterate through each unique layer and apply the processing function
    # for layer_name in unique_layers:
    #     layer_result = geo_ddf.map_partitions(process_layer, layer_name=layer_name)
    #     # You can further process layer_result or collect it if needed
    #     # For example, to collect the results into a single GeoDataFrame
    #     layer_data = layer_result.compute()