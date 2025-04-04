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

    def get_raster_info(self):
        with rasterio.open(self.raster_file) as src:
            ndv = src.nodata
            stats = src.stats(1)
            xsize, ysize = src.width, src.height
            geot = src.transform
            proj = src.crs
            data_type = src.dtypes[0]
        return ndv, stats, xsize, ysize, geot, proj, data_type

    def project(self, outras, dst_crs, template_raster, nodata):
        with rasterio.open(self.raster_file) as src, rasterio.open(template_raster) as tmp:
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

    def resample(self, outras, resamp_type, resamp_res):
        with rasterio.open(self.raster_file) as src:
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

    @staticmethod
    def Reclass(self, outras, reclass_dict, dtype=None):
        """
        __author__ =   "Marc Weber <weber.marc@epa.gov>"
                    "Ryan Hill <hill.ryan@epa.gov>"
        reclass a set of values in a raster to another value

        Arguments
        ---------
        inras           : an input raster file
        outras          : an output raster file
        reclass_dict    : dictionary of lookup values read in from lookup csv file
        in_nodata       : Returned no data values from
        out_dtype       : the data type of the raster, i.e. 'float32', 'uint8' (string)
        """

        with rasterio.open(self.raster_file) as src:
            # Set dtype and nodata values
            if dtype is None:  # If no dtype defined, use input dtype
                nd = src.meta["nodata"]
                dtype = src.meta["dtype"]
            else:
                try:
                    nd = eval("np.iinfo(np." + dtype + ").max")
                except:
                    nd = eval("np.finfo(np." + dtype + ").max")
                # exec 'nd = np.iinfo(np.'+out_dtype+').max'
            kwargs = src.meta.copy()
            kwargs.update(
                driver="GTiff",
                count=1,
                compress="lzw",
                nodata=nd,
                dtype=dtype,
                bigtiff="YES",  # Output will be larger than 4GB
            )

            windows = src.block_windows(1)

            with rasterio.open(outras, "w", **kwargs) as dst:
                for idx, window in windows:
                    src_data = src.read(1, window=window)
                    # Convert values
                    # src_data = np.where(src_data == in_nodata, nd, src_data).astype(dtype)
                    for inval, outval in reclass_dict.iteritems():
                        if np.isnan(outval).any():
                            # src_data = np.where(src_data != inval, src_data, kwargs['nodata']).astype(dtype)
                            src_data = np.where(src_data == inval, nd, src_data).astype(
                                dtype
                            )
                        else:
                            src_data = np.where(src_data == inval, outval, src_data).astype(
                                dtype
                            )
                    # src_data = np.where(src_data == inval, outval, src_data)
                    dst_data = src_data
                    dst.write_band(1, dst_data, window=window)

    def rasterMath(self, outras, expression=None, out_dtype=None):
        """
        __author__ =   "Marc Weber <weber.marc@epa.gov>"
                    "Ryan Hill<hill.ryan@epa.gov>"
        Applies arithmetic operation to a raster by a given value and returns raster
        in a specified data type - ideas from https://sgillies.net/page3.html

        Arguments
        ---------
        inras           : an input raster file (string)
        outras          : an output raster file (string)
        expression      : string of mathematical expression to be used that includes the input raster
                        as variable. If no expression provided, raster is copied. Function can be
                        used to change dtype of original raster.
                        Example:
                        inras = 'C:/some_locat_raster.tif'
                        expression = 'log(' + inras + '+1)' or inras + ' * 100'
        out_dtype       : the data type of the raster, i.e. 'float32', 'uint8' (string)
        """
        expression = expression.replace(self.raster_file, "src_data")

        with rasterio.drivers():
            with rasterio.open(self.raster_file) as src:
                # Set dtype and nodata values
                if out_dtype is None:  # If no dtype defined, use input dtype
                    nd = src.meta["nodata"]
                    dt = src.meta["dtype"]
                else:
                    try:
                        nd = eval("np.iinfo(np." + out_dtype + ").max")
                    except:
                        nd = eval("np.finfo(np." + out_dtype + ").max")
                    # exec 'nd = np.iinfo(np.'+out_dtype+').max'
                    dt = out_dtype
                kwargs = src.meta.copy()
                kwargs.update(driver="GTiff", count=1, compress="lzw", dtype=dt, nodata=nd)

                windows = src.block_windows(1)

                with rasterio.open(outras, "w", **kwargs) as dst:
                    for idx, window in windows:
                        src_data = src.read(1, window=window)
                        # Where src not eq to orig nodata, multiply by val, else set to new nodata. Set dtype
                        if expression == None:
                            # No expression produces copy of original raster (can use new data type)
                            dst_data = np.where(
                                src_data != src.meta["nodata"], src_data, kwargs["nodata"]
                            ).astype(dt)
                        else:
                            dst_data = np.where(
                                src_data != src.meta["nodata"],
                                eval(expression),
                                kwargs["nodata"],
                            ).astype(dt)
                        dst.write_band(1, dst_data, window=window)

    @staticmethod
    def rat_to_dict(self, old_val, new_val):
        """
        __author__ =  "Matt Gregory <matt.gregory@oregonstate.edu>"
                    "Marc Weber <weber.marc@epa.gov>"

        Given a GDAL raster attribute table, convert to a pandas DataFrame.  Idea from
        Matt Gregory's gist: https://gist.github.com/grovduck/037d815928b2a9fe9516
        Arguments
        ---------
        in_rat      : input raster
        old_val     : current value in raster
        new_val     : lookup value to use to replace current value
        """
        # Open the raster and get a handle on the raster attribute table
        # Assume that we want the first band's RAT
        ds = gdal.Open(self.raster_file)
        rb = ds.GetRasterBand(1)
        rat = rb.GetDefaultRAT()
        # Read in each column from the RAT and convert it to a series infering
        # data type automatically
        s = [
            pd.Series(rat.ReadAsArray(i), name=rat.GetNameOfCol(i))
            for i in range(rat.GetColumnCount())
        ]
        # Convert the RAT to a pandas dataframe
        df = pd.concat(s, axis=1)
        # Close the dataset
        ds = None

        # Write out the lookup dictionary
        reclass_dict = pd.Series(df[new_val].values, index=df[old_val]).to_dict()
        return reclass_dict


    @staticmethod
    def catcsv2raster(inCSV, Value, inTemplate, outRaster, dtype='Int', idName='COMID'):
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

    @staticmethod
    def catcsv2raster2(lookup, Value, inTemplate, outRaster, dtype='Int', idName='COMID'):
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