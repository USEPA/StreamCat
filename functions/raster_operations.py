# Functions related to raster processing /transformations
import numpy as np
import pandas as pd
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling

class RasterOperations:
    def __init__(self, raster_file=None):
        self.raster_file = raster_file

    @staticmethod
    def get_raster_info(file_name):
        with rasterio.open(file_name) as src:
            ndv = src.nodata
            stats = src.stats(1)
            xsize, ysize = src.width, src.height
            geot = src.transform
            proj = src.crs
            data_type = src.dtypes[0]
        return ndv, stats, xsize, ysize, geot, proj, data_type

    @staticmethod
    def project(inras, outras, dst_crs, template_raster, nodata):
        with rasterio.open(inras) as src, rasterio.open(template_raster) as tmp:
            affine, width, height = calculate_default_transform(
                src.crs, dst_crs, src.width, src.height, *tmp.bounds
            )
            kwargs = src.meta.copy()
            kwargs.update(
                {
                    "crs": dst_crs,
                    "transform": affine,
                    "width": width,
                    "height": height,
                    "driver": "GTiff",
                }
            )
            with rasterio.open(outras, "w", **kwargs) as dst:
                reproject(
                    source=rasterio.band(src, 1),
                    destination=rasterio.band(dst, 1),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    src_nodata=nodata,
                    dst_transform=affine,
                    dst_crs=dst_crs,
                )

    @staticmethod
    def resample(inras, outras, resamp_type, resamp_res):
        with rasterio.open(inras) as src:
            affine, width, height = calculate_default_transform(
                src.crs, src.crs, src.width, src.height, *src.bounds, resolution=resamp_res
            )
            kwargs = src.meta.copy()
            kwargs.update(
                {
                    "crs": src.crs,
                    "transform": affine,
                    "width": width,
                    "height": height,
                    "driver": "GTiff",
                }
            )
            with rasterio.open(outras, "w", **kwargs) as dst:
                reproject(
                    source=rasterio.band(src, 1),
                    destination=rasterio.band(dst, 1),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=affine,
                    dst_crs=src.crs,
                    resampling=Resampling[resamp_type],
                )