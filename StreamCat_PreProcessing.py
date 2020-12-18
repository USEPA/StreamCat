#--------------------------------------------------------
# Name: Standardize landscape features
# Purpose: Apply standard steps to each landscape raster
#          used in StreamCat, reading values in from a
#          control table to pass to functions
# Author: Marc Weber
# Created 2/4/2014
# Python Version:  2.7

# NOTE: run script from command line passing directory and name of this script
# and then directory and name of the control table to use like:
# > Python "F:\Watershed Integrity Spatial Prediction\Scripts\StreamCat_PreProcessing.py"
# L:\Priv\CORFiles\Geospatial_Library\Data\Project\SSWR1.1B\ControlTables\RasterControlTable.csv
#--------------------------------------------------------
import pandas as pd
from datetime import datetime as dt
import sys, os
ControlTable  = pd.read_csv(sys.argv[1])

from osgeo import ogr, gdal
import fiona
os.environ['GDAL_DATA'] = 'C:/Users/Rdebbout/AppData/Local/Continuum/Anaconda/pkgs/libgdal-1.11.2-2/Library/data'

#ControlTable  = pd.read_csv('L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/ControlTables/RasterControlTable_RD.csv')
#ControlTable  = pd.read_csv('L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/ControlTables/RasterControlTable_MW.csv')
#sys.path.append(ControlTable.DirectoryLocations[3])  #'F:/Watershed Integrity Spatial Prediction/Scripts'
sys.path.append('F:/Watershed Integrity Spatial Prediction/Scripts')
from StreamCat_functions import Reclass, rasterMath, getRasterInfo, dbf2DF, rat_to_dict
import geopandas as gpd
from subprocess import call
import arcpy

#############################
# Parameters
ControlTable  = pd.read_csv(sys.argv[1])
ReClassTable = pd.read_csv(ControlTable.DirectoryLocations[4])
FieldCalcTable = pd.read_csv(ControlTable.DirectoryLocations[5])
# ControlTable = pd.read_csv('L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/ControlTables/RasterControlTable_MW.csv')
#ReClassTable = pd.read_csv('L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/ControlTables/ReclassTable.csv')
#FieldCalcTable =pd.read_csv('L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/ControlTables/FieldCalcTable.csv')

#####################################################################################################################
# Populate variables from control table
InDir = ControlTable.DirectoryLocations[0]
TempDir = ControlTable.DirectoryLocations[1]
FinalDir = ControlTable.DirectoryLocations[2]
MaskRas = ControlTable.DirectoryLocations[3]
#####################################################################################################################

out_coor_system = "PROJCS['NAD_1983_Contiguous_USA_Albers',\
                    GEOGCS['GCS_North_American_1983',\
                        DATUM['D_North_American_1983',\
                            SPHEROID['GRS_1980',6378137.0,298.257222101]],\
                        PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],\
                    PROJECTION['Albers'],\
                    PARAMETER['false_easting',0.0],\
                    PARAMETER['false_northing',0.0],\
                    PARAMETER['central_meridian',-96.0],\
                    PARAMETER['standard_parallel_1',29.5],\
                    PARAMETER['standard_parallel_2',45.5],\
                    PARAMETER['latitude_of_origin',23.0],\
                    UNIT['Meter',1.0]]"

for line in ControlTable.values: # loop through each landscape_var in control table
    if line[-1] == 1:   # check 'run' field from the table, if 1 run, if not, skip
        print 'running ' + str(line[2])
        InFile = line[2]
        OutFile = line[3]
        FileType = line[4]
        DataCategory = line[5]
        ModifyBy = line[6]
        RastType = line[7]
        ReclassTable = line[8]
        RasterAttTable = line[9]
        ShapeFieldCalc = line[10]
        Convert = line[11]
        ConvertFields = line[12]
        ConvertRes = int(line[13])
        UseArcpy = line[14]
        UseStatesMask = line[15]
        if FileType != 'ESRI Shapefile':
            #get raster info for input raster
            if FileType == 'Image file':
                InRas = InFile + '.img'
            elif FileType == 'Geotiff':
                InRas = InFile + '.tif'
            else:
                InRas = InFile

            NDV, Stats, xsize, ysize, GeoT, Proj_projcs, Proj_geogcs, DataType = getRasterInfo(InDir + '/' + InRas)
            print DataType

            # Check if we need to reclass any raster values
            if ReclassTable=='Yes':
                reclass_dict = dict()
                if RasterAttTable!='No':
                    rat_dict = dict()
                    ingrid = InDir + '/' + InRas
                    rat_dict = rat_to_dict(ingrid, RasterAttTable.split(';')[0], RasterAttTable.split(';')[1])
                    g = ReClassTable.loc[ReClassTable['FileName'] == OutFile]
                    lookup = g.set_index('OldVal')['NewVal'].to_dict()
                    for k,v in rat_dict.iteritems():
                        if not v in lookup.keys():
                            reclass_dict[k] = 0
                        else:
                            if RastType != 'Float32':
                                try:
                                    reclass_dict[k] = int(lookup[v])
                                except:
                                    reclass_dict[k] = float(lookup[v])
                elif RasterAttTable=='No':
                    g = ReClassTable.loc[ReClassTable['FileName'] == OutFile]
                    lookup = g.set_index('OldVal')['NewVal'].to_dict()
                    OldVal = ReClassTable.loc[ReClassTable['FileName'] == OutFile,'OldVal']
                    NewVal = ReClassTable.loc[ReClassTable['FileName'] == OutFile,'NewVal']
                    # Need to pull values out of a pandas series as a simple integer or float to use in reclass
                    for i in OldVal.index.tolist():
                        print i
                        reclass_dict[float(OldVal[i])] = float(NewVal[i])
                tempras = TempDir + '/' + OutFile + '.tif'
                if not os.path.isfile(tempras):
                    Reclass(InDir + '/' + InRas, tempras, reclass_dict)
            # Check if we need to multiply or modify any raster values
            if not ModifyBy == 0:
                tempras = TempDir + '/' + OutFile + '_2.tif'
                inras = InDir + '/' + InRas
                if not os.path.isfile(tempras):
                    rasterMath(inras, tempras, expression= inras + ' * ' + str(ModifyBy), out_dtype=RastType)
            # if temp raster hasn't been created in previous steps, just poiint to input raster
            if not os.path.isfile(TempDir + '/' + OutFile + '.tif') and not os.path.isfile(TempDir + '/' + OutFile + '_2.tif'):
                if FileType == 'ESRI raster':
                    tempras = InDir + '/' + InFile
                elif FileType == 'Geotiff':
                    tempras = InDir + '/' + InFile + '.tif'
                elif FileType == 'Image file':
                    tempras = InDir + '/' + InFile + '.img'
                elif FileType == 'ASCII':
                    tempras = InDir + '/' + InFile

                    # ADD ELSE HERE IF NO CONDITIONS ARE MET, KICK OUT ERROR STATEMENT AND MOVE TO NEXT LINE OF LOOP

            # get raster info from temp raster
            minx = GeoT[0]
            maxy = GeoT[3]
            maxx = minx + GeoT[1]*xsize
            miny = maxy + GeoT[5]*ysize
            bounds = str(minx) + " " + str(miny) + " " + str(maxx) + " " + str(maxy)
            outDataType = DataType
            outNDV = NDV

            dst_crs = 'NAD_1983_Contiguous_USA_Albers'


            # If final file doesn't already exists use gdal resample to resample to desired resolution and project.
            # Also apply a mask if needed to produce final raster
            if not os.path.isfile(FinalDir + '/' + OutFile + '.tif'):
               if UseArcpy == 'Yes':
                   finalras = FinalDir + '/' + OutFile + '.tif'
                   if UseStatesMask == 'No':
                       desc = arcpy.Describe(tempras)
                       sr = desc.spatialReference.exportToString()
                       if DataCategory == 'continuous':
                           resamp_type='BILINEAR'
                       if DataCategory == 'categorical':
                           resamp_type='NEAREST'
                       snapping_pnt = "%f %f"%(desc.extent.XMin,desc.extent.YMin)
                       arcpy.ProjectRaster_management(tempras, finalras, out_coor_system, resamp_type, ConvertRes, "", snapping_pnt)
                   if UseStatesMask == 'Yes':
                        # Execute ExtractByMask
                       desc = arcpy.Describe(tempras)
                       sr = desc.spatialReference.exportToString()
                       arcpy.CheckOutExtension("Spatial")
                       arcpy.env.mask = MaskRas
                       if DataCategory == 'continuous':
                           resamp_type='BILINEAR'
                       if DataCategory == 'categorical':
                           resamp_type='NEAREST'
                       snapping_pnt = "%f %f"%(desc.extent.XMin,desc.extent.YMin)
                       arcpy.ProjectRaster_management(tempras, finalras, out_coor_system, resamp_type, ConvertRes, "", snapping_pnt)
               if UseArcpy == 'No':
                   # Need to add ability to mask as well with gdal / rasterio approach...
                   if not Proj_projcs==dst_crs:
                        resamp_ras = FinalDir + '/' + OutFile + '.tif'
                        resamp_string = "gdalwarp --config GDAL_DATA " + '"C:/Users/mweber/AppData/Local/Continuum/Anaconda/pkgs/libgdal-1.11.2-2/Library/data" ' +' -tr ' + str(ConvertRes) + ' -' + str(ConvertRes) + " -te " + bounds + " -srcnodata " + str(outNDV) +  " -dstnodata "  + str(outNDV) +  " -of GTiff -r near -t_srs " + dst_crs + " -co COMPRESS=DEFLATE -co TFW=YES -co TILED=YES -co TIFF_USE_OVR=TRUE -ot " + outDataType + " " + tempras + " " + resamp_ras
                        startTime = dt.now()
                        call(resamp_string)
                        print "elapsed time " + str(dt.now()-startTime)

        # Processes for vector features
        if FileType == 'ESRI Shapefile':
            Feat = gpd.GeoDataFrame.from_file(InDir + '/' + InFile + '.shp')
            # just for census block groups, subset just to CONUS
            if InFile == 'tl_2010_US_bg10':
                Feat = Feat.loc[~Feat['STATEFP10'].isin(['02','15','72'])]
            # check if we need to create fields and / or run calculations on fields in shapefile
            if ShapeFieldCalc == 'Yes':
                # first project if needed to Albers Equal Area
                if not Feat.crs['proj'] == 'aea':
                    Feat = Feat.to_crs(epsg=5070)
                # gather all the values from the ShapefileFieldCalc control table
                f = FieldCalcTable.loc[FieldCalcTable['FileName'] == InFile]
                JoinTable = f.loc[f['FileName'] == InFile,'JoinTable']
                InField = f.loc[f['FileName'] == InFile,'InField']
                OutField = f.loc[f['FileName'] == InFile,'OutField']
                Operation = f.loc[f['FileName'] == InFile,'Operation']
                Value = f.loc[f['FileName'] == InFile,'Value']

                # iterate through processes to run for each feature in the ShapfileFieldCalc control table
                rangelist = JoinTable.index.tolist()
                for k in rangelist:
                    if not pd.isnull(JoinTable[k]) and not InField[k] in list(Feat):
                        lookup = dbf2DF(InDir + '/' + JoinTable[k])
                        lookup=lookup[[InField[k],'STCNTRBG']]
                        lookup.rename(columns={'STCNTRBG':'GEOID10'}, inplace=True)
    #                    Feat = pd.merge(left=Feat,right=lookup, how='left', left_on='GEOID10', right_on='STCNTRBG')
                        Feat= Feat.merge(lookup, on='GEOID10')
                    if InField[k] == 'AREA':
                        expression= 'Feat.area * %f'%(float(Value[k]))
                        Feat[OutField[k]]=eval(expression)
                        pass
                    elif Operation[k] == 'Multiply':
                        # checking if value is string determines if we're just using existing field in expression rather than a value
#                        if not type(Value[k])==str:
#                            expression= 'Feat.%s * %f'%(InField[k],float(Value[k]))
#                        if type(Value[k])==str:
                        expression= 'Feat.%s * %s'%(InField[k],Value[k])
                        Feat[OutField[k]] = eval(expression)
                    elif Operation[k] == 'Divide':
                        # checking if value is string determines if we're just using existing field in expression rather than a value
                        if not type(Value[k]) == str:
                            expression= 'Feat.%s / %f'%(InField[k],Value[k])
                        if type(Value[k]) == str:
                            expression= 'Feat.%s / Feat.%s'%(InField[k],Value[k])
                        Feat[OutField[k]] = eval(expression)
            if not Feat.crs['proj'] == 'aea':
                Feat = Feat.to_crs(epsg=5070)
            if UseStatesMask=='Yes':
                mask = gpd.GeoDataFrame.from_file(ControlTable.DirectoryLocations[6])
                mask = mask.loc[0].geometry # see https://michelleful.github.io/code-blog/2015/04/29/geopandas-manipulation/ for explanation - geopandas still a bit beta
                Feat = Feat[Feat.geometry.within(mask)]
            Feat.to_file(FinalDir + '/' + OutFile + '.shp', driver = 'ESRI Shapefile')
            # Do we need to rasterize shapefile? (Right now only for census block groups)
            if Convert == 'Yes':
                for item in ConvertFields.split(';'):
                    print item
                    InShp = FinalDir + '/' + InFile + '.shp'
        #            InShp = InDir + '/' + Rast + '.shp'
                    OutRas =  FinalDir + '/' + item + '.tif'
#                    resamp_string = 'gdal_rasterize -a ' + item + ' -l ' + InFile +' -tr ' + str(ConvertRes) + ' -' + str(ConvertRes) + ' -co COMPRESS=DEFLATE ' +  InShp + ' ' + OutRas
                    startTime = dt.now()
#                    call(resamp_string)
                    ##  call() statement not working for me, use arcpy, rickD
                    arcpy.PolygonToRaster_conversion(InShp, item, OutRas, 'CELL_CENTER', "", str(ConvertRes))
                    print "elapsed time " + str(dt.now()-startTime)


