import arcpy
from arcpy.sa import *
arcpy.CheckOutExtension("Spatial")
import numpy as np
import pandas as pd
import gc


#####################################################################################################################
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

#####################################################################################################################

#####################################################################################################################
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

#####################################################################################################################
    
