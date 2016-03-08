# ---------------------------------------------------------------------------
# loticMask.py
# 3/31/2015
# Description: Combine NLCD water pixels that have contiguity to NHDPlus streams, and NHDPlus
#              stream pixels, to create a true water raster, and then buffer to create riparian buffer
# By: Darren Thornbrugh, Ryan Hill, Marc Weber
# ---------------------------------------------------------------------------

# Import arcpy module
import arcpy
import os
from arcpy.sa import *
# Check out any necessary licenses
arcpy.CheckOutExtension("spatial")
import numpy as np
import numpy.ma as ma
from osgeo import gdal
import osr

arcpy.env.overwriteOutput = True

# Local variables:
nlcd2006 = "L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/LandscapeRasters/QAComplete/nlcd2006.tif"
working_dir = 'J:/Watershed Integrity Spatial Prediction/WaterMask'
NHDDir = "L:/Priv/CORFiles/Geospatial_Library/Data/RESOURCE/PHYSICAL/HYDROLOGY/NHDPlusV21"

def array2raster(newRasterfn,rasterfn,array):
    geotransform = rasterfn.GetGeoTransform()
    originX = geotransform[0]
    originY = geotransform[3]
    pixelWidth = geotransform[1]
    pixelHeight = geotransform[5]
    cols = array.shape[1]
    rows = array.shape[0]

    driver = gdal.GetDriverByName('GTiff')
    outRaster = driver.Create(newRasterfn, cols, rows, 1, gdal.GDT_Byte)
    outRaster.SetGeoTransform((originX, pixelWidth, 0, originY, 0, pixelHeight))
    outband = outRaster.GetRasterBand(1)
    outband.WriteArray(array)
    outRasterSRS = osr.SpatialReference()
    outRasterSRS.ImportFromWkt(rasterfn.GetProjectionRef())
    outRaster.SetProjection(outRasterSRS.ExportToWkt())
    outband.FlushCache()
    
# Pull out NLCD Water to use in creating mask
if not arcpy.Exists('%s/nlcd_water.tif'%(working_dir)):
    nlcd = Raster(nlcd2006)
    NLCDWat = Con(nlcd ==11 ,1)
    NLCDWat.save('%s/nlcd_water.tif'%(working_dir))
inputs = {'CA':['18'],'CO':['14','15'],'GB':['16'],'GL':['04'],'MA':['02'],'MS':['05','06','07','08','10L','10U','11'],'NE':['01'],'PN':['17'],'RG':['13'],'SA':['03N','03S','03W'],'SR':['09'],'TX':['12']}
for regions in inputs.keys():
    for hydro in inputs[regions]:
        print 'on region ' + regions + ' and hydro number ' + hydro
        hydrodir = "%s/NHDPlus%s/NHDPlus%s"%(NHDDir,regions, hydro)
        for subdirs in os.listdir(hydrodir):
            if subdirs.count("FdrNull") and not subdirs.count('.txt') and not subdirs.count('.7z'):
                print 'working on ' + subdirs
                
                # Read in the fdr null raster for hydroregion
                fdrnull = "%s/%s/fdrnull"%(hydrodir, subdirs)
                dsc=arcpy.Describe(fdrnull)
                arcpy.env.extent=dsc.Extent
                ext=dsc.Extent
                ll = arcpy.Point(ext.XMin, ext.YMin)
                arcpy.env.outputCoordinateSystem=dsc.SpatialReference
                arcpy.env.cellSize=dsc.meanCellWidth
                fdr = "%s/%s/fdr"%(hydrodir, subdirs.replace('Null','Fac'))
                arcpy.env.mask = fdr 
                
                # can't use fdr null for riparian - we have to use stream raster that incorporates any possible stream lines (fdr null will leave some out)
#                # Create stream raster for hydro-region based on fdrnull==1
#                NullRaster = Raster(fdrnull)
#                StrmRas = Con(IsNull(NullRaster),1)
#                if not arcpy.Exists('%s/StreamRaster_%s.tif'%(working_dir,subdirs.split('Null',2)[1])):
#                    StrmRas.save('%s/StreamRaster_%s.tif'%(working_dir,subdirs.split('Null',2)[1]))
                StrmRas = Raster('J:/Watershed Integrity Spatial Prediction/Spatial Data/line100mbuffer/flowgrid%s'%(hydro))
                # Process: Region Group - this gives each group of contiguous pixels a unique region ID
                NLCDWat = Raster('%s/nlcd_water.tif'%(working_dir))
                RegionGroup = '%s/RegionGroup_%s.tif'%(working_dir,subdirs.split('Null',2)[1])
                if not arcpy.Exists('%s/RegionGroup_%s.tif'%(working_dir,subdirs.split('Null',2)[1])):
                    arcpy.gp.RegionGroup_sa(NLCDWat, RegionGroup, "EIGHT", "WITHIN", "NO_LINK", "")
                                                   
                # Now multiply the region group raster by the stream raster
                RgnGrp = Raster(RegionGroup)
                OutTimes = Times(StrmRas,RgnGrp)
                if not arcpy.Exists('%s/OutTimes_%s.tif'%(working_dir,subdirs.split('Null',2)[1])):
                    OutTimes.save('%s/OutTimes_%s.tif'%(working_dir,subdirs.split('Null',2)[1]))
                if not arcpy.Exists('%s/WaterMask_%s.tif'%(working_dir,subdirs.split('Null',2)[1])):
                    # Convert Rasters to numpy arrays
                    OutTimes = gdal.Open('%s/OutTimes_%s.tif'%(working_dir,subdirs.split('Null',2)[1]))
                    OutTimes_arr = np.array(OutTimes.GetRasterBand(1).ReadAsArray())
                    RgnGrp = gdal.Open('%s/RegionGroup_%s.tif'%(working_dir,subdirs.split('Null',2)[1]))
                    RgnGrp_arr = np.array(RgnGrp.GetRasterBand(1).ReadAsArray())
    #                OutTimes_arr = arcpy.RasterToNumPyArray('%s/OutTimes_%s.tif'%(working_dir,subdirs.split('Null',2)[1]))
    #                RgnGrp_arr = arcpy.RasterToNumPyArray('%s/RegionGroup_%s.tif'%(working_dir,subdirs.split('Null',2)[1]))
                    
                    unq = np.unique(OutTimes_arr) #Get unique values for query
                    unq = unq[1:] #The first member of the vector will be -32768. Let's us set new grid == 1 (see line 62)
                    #This is what the vector looked like before removing the first element:
                    #unq
                    #Out[46]: array([-32768,      3,      7, ...,   2462,   2464,   2466], dtype=int16)
                    
                    RgnGrp_arr = RgnGrp_arr.flatten() #Flatten 2d array to 1d
                    z = np.where(np.in1d(RgnGrp_arr, unq), 1, np.NaN)
                    #z = np.where(np.in1d(RgnGrp_arr, unq), RgnGrp_arr, np.NaN) #Make NaN where no match
                    #z[z==-32768] = np.NaN #Brings in big neg number when read into Python. Turn to NaN
                    z.shape = OutTimes_arr.shape #Reshape back to 2d
                    
    #                newRaster = arcpy.NumPyArrayToRaster(z, ll, dsc.meanCellWidth, dsc.meanCellHeight) #Make ESRI raster
                    newraster= array2raster('%s/OutTimesV2_%s.tif'%(working_dir,subdirs.split('Null',2)[1]), OutTimes, z)
                    newRaster = Raster('%s/OutTimesV2_%s.tif'%(working_dir,subdirs.split('Null',2)[1]))
                    WaterMask = Con(newRaster == 1, 1) #run it through a process to get it to be integer and in native ESRI format (exclude odd NUMPY stuff)                
                    WaterMask.save('%s/WaterMask_%s.tif'%(working_dir,subdirs.split('Null',2)[1]))
                                
                # After saving basic water mask for every region, need to: 
                # 1) build out 100m buffer from water mask using euc distance
                # 2) combine with the already existing 100m buf raster, but set the area in original water mask back to no data (so that buffer only extends
                #    from true water edge based on imagery)
                print 'processing Euclidean Distance raster for ' + subdirs.split('Null',2)[1]
                if not arcpy.Exists('%s/EucDist_%s.tif'%(working_dir,subdirs.split('Null',2)[1])):
                    arcpy.gp.EucDistance_sa('%s/WaterMask_%s.tif'%(working_dir,subdirs.split('Null',2)[1]), '%s/EucDist_%s.tif'%(working_dir,subdirs.split('Null',2)[1]), "800", "30", "")
                eucdist = Raster('%s/EucDist_%s.tif'%(working_dir,subdirs.split('Null',2)[1]))
                # subset the euclidean distance raster to less than or equal to 100m for a 100m buffer
                maksbuf100 = Con(eucdist <= 100, 1,)
                if not arcpy.Exists('%s/MaskBuf100_%s.tif'%(working_dir,subdirs.split('Null',2)[1])):
                    maksbuf100.save('%s/MaskBuf100_%s.tif'%(working_dir,subdirs.split('Null',2)[1]))
                
                
                # Combine our previous linear 100m buffer on all streams with this new 100m buffer on our water mask
                InRasters = '%s/MaskBuf100_%s.tif'%(working_dir,subdirs.split('Null',2)[1]) + ";" + \
                "L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/LandscapeRasters/QAComplete/line100mbuffer/lnbuf%s.tif"%(hydro)
                if not arcpy.Exists('%s/FullBuf100_%s.tif'%(working_dir,subdirs.split('Null',2)[1])):
                    arcpy.MosaicToNewRaster_management(InRasters, working_dir,'FullBuf100_%s.tif'%(subdirs.split('Null',2)[1]), "", "8_BIT_UNSIGNED", "", "1", "MAXIMUM", "FIRST")
                
                # Step to get rid of background values
                FullBuf = Raster('%s/FullBuf100_%s.tif'%(working_dir,subdirs.split('Null',2)[1]))
                FullBuf_adj = Con(FullBuf==1,1)
                if not arcpy.Exists('%s/FullBuf100V2_%s.tif'%(working_dir,subdirs.split('Null',2)[1])):
                    FullBuf_adj.save('%s/FullBuf100V2_%s.tif'%(working_dir,subdirs.split('Null',2)[1]))
                
                # Now mask out original mask raster from the final buffer
                # now erase out the actual water part in this new mask
                WaterMask = Raster('%s/WaterMask_%s.tif'%(working_dir,subdirs.split('Null',2)[1]))
                FinalRas = SetNull( ~(IsNull( WaterMask )), FullBuf_adj )
                if not arcpy.Exists('%s/RipBuf100_%s.tif'%(working_dir,subdirs.split('Null',2)[1])):
                    FinalRas.save('%s/RipBuf100_%s.tif'%(working_dir,subdirs.split('Null',2)[1]))
                
# Mosaic together RPUs
ToMosaic = [f for f in os.listdir(working_dir) if f.count('RipBuf100') and f[-4:]=='.tif']
RPU_Dict = dict()
inputs = {'CA':['18'],'CO':['14','15'],'GB':['16'],'GL':['04'],'MA':['02'],'MS':['05','06','07','08','10L','10U','11'],'NE':['01'],'PN':['17'],'RG':['13'],'SA':['03N','03S','03W'],'SR':['09'],'TX':['12']}
for regions in inputs.keys():
    for hydro in inputs[regions]:
        hydrodir = "%s/NHDPlus%s/NHDPlus%s"%(NHDDir,regions, hydro)
        print hydrodir
        RPU_Dict[hydro] = [f[-3:] for f in os.listdir(hydrodir) if f.count('FdrFac') ]
        ZoneMosaic = [f for f in ToMosaic if f.split('_')[1].split('.')[0] in RPU_Dict[hydro]]
        ZoneMosaic = str(ZoneMosaic).strip('[]')
        ZoneMosaic = ZoneMosaic.replace(",",';')
        arcpy.env.workspace = working_dir
        # Set Geoprocessing environments
        arcpy.env.snapRaster = "%s/NHDPlusCatchment/cat"%(hydrodir)
        tempdir = "C:/Users/mweber/Temp"
        outmosaic = "RipBuf100_%s.tif"%(hydro)
        # Process: Mosaic
        if not arcpy.Exists(outmosaic):
            arcpy.MosaicToNewRaster_management(ZoneMosaic, tempdir, outmosaic, "", "8_BIT_UNSIGNED", "", "1", "MAXIMUM", "")
        # Mosaic leaves 0's where we want NoData
        finalmosaic = SetNull('%s/%s'%(tempdir, outmosaic), 1, "VALUE <> 1")
        if not arcpy.Exists('%s/Mosaics/%s'%(working_dir, outmosaic)):
            finalmosaic.save('%s/Mosaics/%s'%(working_dir, outmosaic))
        