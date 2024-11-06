#import dask_geopandas
import xarray as xr
import rioxarray
import geopandas as gpd
import os
#import numpy as np
from xrspatial.zonal import stats
from shapely.geometry import box
from rasterio.features import rasterize
from dask.distributed import Client, LocalCluster
import dask.array as da
import time

def load_raster(vpu_id, layer):
    raster_path = f'high_res_data/NHDPLUS_H_{vpu_id}_HU4_RASTERS/HRNHDPlusRasters{vpu_id}/{layer}.tif'
    if not os.path.exists(raster_path):
        raise FileNotFoundError(f"Raster file not found: {raster_path}")
    return rioxarray.open_rasterio(raster_path, chunks=True).sel(band=1).drop_vars('band') #, chunks=True

def load_vector_data(in_zone_data_path, vpu_id=None):
    #TODO add layer parameter so we can do all layers in the dask_geopandas file
    if vpu_id:
        vpu_data_path = os.path.join('high_res_data', f'NHDPLUS_H_{vpu_id}_HU4_GDB.gdb')
        if not os.path.exists(vpu_data_path):
            raise FileNotFoundError(f"VPU geodatabase not found: {vpu_data_path}")
        return gpd.read_file(vpu_data_path, layer='NHDPlusCatchment')# dask_geopandas.read_file(vpu_data_path, layer='NHDPlusCatchment', chunksize=256)
    else:
        return gpd.read_file(in_zone_data_path, layer='NHDPlusCatchment') # dask_geopandas.read_file(in_zone_data_path, layer='NHDPlusCatchment', chunksize=256) # TODO reduce down to vpu once returned
    
def compute_zonal_stats(raster, vector_data):
    # Ensure the vector data is reprojected to match the raster CRS
    vector_data = vector_data.to_crs(raster.rio.crs)
    
    # Convert vector data to a rasterized form
    # zones = raster.rio.clip(vector_data.geometry, vector_data.crs, drop=False)
    # zones.values = vector_data['nhdplusid'].values
    bounds = raster.rio.bounds()
    bbox = box(*bounds)

    vector_data_clipped = vector_data[vector_data.geometry.intersects(bbox)]
    # print(raster.rio.profile)
    # Rasterize the vector data to create the zones array
    # zones = raster.rio.reproject_match(vector_data_clipped, resampling='nearest')
    # zones = zones.where(zones != zones.rio.nodata, 0)
    
    # # Assign the nhdplusid values to the rasterized zones
    # zones = zones.rio.clip(vector_data_clipped.geometry, vector_data_clipped.crs, drop=False, invert=False)
    # zones.values = vector_data_clipped['NHDPlusID'].values
    
    # # Ensure the zones array has the same shape as the values array
    # zones = zones.rio.reproject_match(raster, resampling='nearest')

    transform = raster.rio.transform()
    width = raster.sizes['x']
    height = raster.sizes['y']
    # print(len(vector_data_clipped.GridCode.unique()))
    shapes = [(geom, value) for geom, value in zip(vector_data_clipped.geometry, vector_data_clipped['GridCode'])]
    
    zones = rasterize(
        shapes=shapes,
        out_shape=(height, width),
        transform=transform,
        fill=0,  # Fill value for areas outside the vector data
        dtype='int32'
    )

    zones_da = xr.DataArray(
        data=zones,
        dims=['y', 'x'],
        coords={
            'y': raster.y,
            'x': raster.x
        }
    ).chunk(raster.chunksizes)


    # print(type(zones_da.data)) # _da.data
    # print(type(raster.data))
    # numpy_zones_data = zones_da.data.to_numpy()

    # zones_dask = xr.DataArray(da.from_array(zones, chunks='auto'))
    # values_dask = xr.DataArray(da.from_array(raster.data, chunks='auto'))

    # print(type(zones_dask.data))
    # print(type(values_dask.data))

    # stats_df = stats(
    #     zones=zones_dask,
    #     values=values_dask,
    #     stats_funcs=['mean', 'sum', 'min', 'max', 'count']
    # )
    
    # Compute zonal statistics
    stats_df = stats(
        zones=zones_da,
        values=raster,
        stats_funcs=['mean', 'sum', 'min', 'max', 'count'],
        nodata_values=raster.rio.nodata
    )

    stats_df = stats_df.repartition(npartitions=8)
    #print(stats_df.head())
    final_df = stats_df.compute()
    #print("Result", stats_df.compute())
    
    return final_df


def process_vpu(vpu_id, layer, in_zone_data_path, output_path):
    #try:
    # Load raster data
    raster = load_raster(vpu_id, layer)
    
    # Load vector data
    vector_data = load_vector_data(in_zone_data_path, vpu_id)
    
    # Compute zonal statistics
    zonal_stats = compute_zonal_stats(raster, vector_data)
    
    # Save the results
    output_file = f'{output_path}/high_res_zonal_stats_dask{vpu_id}.csv'
    zonal_stats.to_csv(output_file)
    
    print(f"Processed VPU {vpu_id} and saved results to {output_file}")
    # except Exception as e:
    #     print(f"Error processing VPU {vpu_id}: {e}")

def main(layer, in_zone_data_path, output_path, vpu_ids):
    for vpu_id in vpu_ids:
        process_vpu(vpu_id, layer, in_zone_data_path, output_path)

if __name__ == "__main__":
    # cluster = LocalCluster(n_workers=8)
    # client = Client(cluster)
    # print(client.dashboard_link)
    layer = 'filldepth' # TODO make a list of raster layers to run (like the Control Table csv)
    in_zone_data_path = 'high_res_data/NHDPlus_H_National_Release_1_GDB/NHDPlus_H_National_Release_1_GDB.gdb'
    output_path = 'high_res_data/output'
    vpu_ids = ['1710'] #TODO get all vpu_ids from national release GDB
    start_time = time.time()
    main(layer, in_zone_data_path, output_path, vpu_ids)
    end_time = time.time()
    print(f"Time to get zonal stats for {layer} on VPU(s) (with dask and 8 workers) {vpu_ids}: {end_time-start_time} seconds")