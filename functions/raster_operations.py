# Functions related to raster processing /transformations
import numpy as np
import pandas as pd
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
import gc 
import arcpy
from arcpy.sa import *
arcpy.CheckOutExtension("Spatial")


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


    def catcsv2raster(self, inCSV, Value, inTemplate, outRaster, dtype='Int', idName='COMID'):
        '''
        __author__ = "Ryan Hill <hill.ryan@epa.gov>"
                    "Marc Weber <weber.marc@epa.gov>"
        Converts CSV table to GeoTIFF and save output.
        Requires template raster with NHDPlusV2 COMIDs that match CSV files.

        Arguments
        ---------
        inCSV           : csv table
                        Input catchment values
        Value           : Text input from user
                        Column in table to use as values
        inTemplate      : GeoTIFF
                        Template raster with COMIDs for pixel values
        outRaster       : GeoTIFF
                        Output raster file path and name
        dtype           : Text input from user
                        Desired data type of output raster - default = None
                        If no dtype is provided, the default is to produce an 'Int' raster
        idName          : Text input from user
                        Column in table to use as unique ID - default = 'COMID'
        '''
            #Read raster and CSV
        inRas = arcpy.Raster(inTemplate)
        #Fix environment settings to inRas
        dsc=arcpy.Describe(inRas)
        arcpy.env.extent=dsc.Extent
        arcpy.env.compression = 'LZW'
        ext=dsc.Extent
        ll = arcpy.Point(ext.XMin, ext.YMin)
        arcpy.env.outputCoordinateSystem=dsc.SpatialReference
        arcpy.env.cellSize=dsc.meanCellWidth
        arcpy.env.mask = inRas
        cellSize = dsc.meanCellWidth

        rstArray = arcpy.RasterToNumPyArray(inRas)
        lookup = pd.read_csv(inCSV)

            #Prep data by adding noData number to front of vectors
        b = np.float64(np.append(-9999, np.array(lookup[idName])))
        c = np.float64(np.append(-9999, np.array(lookup[Value])))
        a = rstArray.flatten()
        a[a==0] = -9999
        a = np.where(np.in1d(a,b), a, -9999)
            #Run numpy query to replace COMID raster with desired values:
        bsort = np.argsort(b) #Create sorting index
        apos = np.searchsorted(b[bsort], a) #Search a on sorted b
        indices = bsort[apos] #Get indices in b that match a
        z = c[indices] #Make final vector from desired data (c)
        z.shape = rstArray.shape #Reshape back to 2d
    
        newRas = arcpy.NumPyArrayToRaster(z, lower_left_corner=ll, x_cell_size=cellSize, y_cell_size=cellSize, value_to_nodata=-9999)
        if dtype == 'Int':
            arcpy.CopyRaster_management(newRas, outRaster, "", "", "", "", "", "16_BIT_SIGNED") 
        else:
            arcpy.CopyRaster_management(newRas, outRaster, "", "", "", "", "", "32_BIT_FLOAT")
        del newRas, a, b, c, bsort, apos, indices, z, inRas, rstArray
        gc.collect()


    def catcsv2raster2(self, lookup, Value, inTemplate, outRaster, dtype='Int', idName='COMID'):
        '''
        __author__ = "Ryan Hill <hill.ryan@epa.gov>"
                    "Marc Weber <weber.marc@epa.gov>"
        Converts CSV table to GeoTIFF and save output.
        Requires template raster with NHDPlusV2 COMIDs that match CSV files.

        Arguments
        ---------
        lookup           : pandas table
                        Input catchment values
        Value           : Text input from user
                        Column in table to use as values
        inTemplate      : GeoTIFF
                        Template raster with COMIDs for pixel values
        outRaster       : GeoTIFF
                        Output raster file path and name
        dtype           : Text input from user
                        Desired data type of output raster - default = None
                        If no dtype is provided, the default is to produce an 'Int' raster
        idName          : Text input from user
                        Column in table to use as unique ID - default = 'COMID'
        '''
            #Read raster and CSV
        inRas = arcpy.Raster(inTemplate)
        #Fix environment settings to inRas
        dsc=arcpy.Describe(inRas)
        arcpy.env.extent=dsc.Extent
        arcpy.env.compression = 'LZW'
        ext=dsc.Extent
        ll = arcpy.Point(ext.XMin, ext.YMin)
        arcpy.env.outputCoordinateSystem=dsc.SpatialReference
        arcpy.env.cellSize=dsc.meanCellWidth
        arcpy.env.mask = inRas
        cellSize = dsc.meanCellWidth

        rstArray = arcpy.RasterToNumPyArray(inRas)

        # Prep data by adding noData number to front of vectors
        b = np.float64(np.append(-9999, np.array(lookup[idName])))
        c = np.float64(np.append(-9999, np.array(lookup[Value])))
        a = rstArray.flatten()
        a[a==0] = -9999
        a = np.where(np.in1d(a,b), a, -9999)
            #Run numpy query to replace COMID raster with desired values:
        bsort = np.argsort(b) #Create sorting index
        apos = np.searchsorted(b[bsort], a) #Search a on sorted b
        indices = bsort[apos] #Get indices in b that match a
        z = c[indices] #Make final vector from desired data (c)
        z.shape = rstArray.shape #Reshape back to 2d
    
        newRas = arcpy.NumPyArrayToRaster(z, lower_left_corner=ll, x_cell_size=cellSize, y_cell_size=cellSize, value_to_nodata=-9999)
        if dtype == 'Int':
            arcpy.CopyRaster_management(newRas, outRaster, "", "", "", "", "", "16_BIT_SIGNED") 
        else:
            arcpy.CopyRaster_management(newRas, outRaster, "", "", "", "", "", "32_BIT_FLOAT")
        del newRas, a, b, c, bsort, apos, indices, z, inRas, rstArray
        gc.collect()