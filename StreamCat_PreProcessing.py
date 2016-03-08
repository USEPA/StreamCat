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
sys.path.append('F:/Watershed Integrity Spatial Prediction/Scripts')
from StreamCat_functions import Reclass, rasterMath, getRasterInfo, dbf2DF, rat_to_dict
import geopandas as gpd   
from subprocess import call
import arcpy
#############################
# Parameters
ControlTable  = pd.read_csv(sys.argv[1])
#ControlTable = pd.read_csv('L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/ControlTables/RasterControlTable.csv')
ReClassTable = pd.read_csv('L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/ControlTables/ReclassTable.csv')
FieldCalcTable =pd.read_csv('L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/ControlTables/FieldCalcTable.csv')
#####################################################################################################################
# Populate variables from control table
InDir = ControlTable.DirectoryLocations[0]
TempDir = ControlTable.DirectoryLocations[1]
FinalDir = ControlTable.DirectoryLocations[2]
#####################################################################################################################

for line in ControlTable.values: # loop through each landscape_var in control table 
    if line[-1] == 1:   # check 'run' field from the table, if 1 run, if not, skip
        print 'running ' + str(line[2])
        Rast = line[2]
        FileType = line[3]
        DataCategory = line[4]
        ModifyBy = line[5]
        RastType = line[6]
        ReclassTable = line[7]
        RasterAttTable = line[8]
        ShapeFieldCalc = line[9]
        Convert = line[10]
        ConvertFields = line[11]
        ConvertRes = int(line[12])
        UseArcpy = line[13]
        UseStatesMask = line[14]
        if FileType!= 'ESRI Shapefile':
            #get raster info for input raster
            if FileType=='Image file':
                InRas = Rast + '.img'
            if FileType=='Geotiff':
                InRas = Rast + '.tif'
            elif FileType=='ESRI raster':
                InRas = Rast
            NDV, Stats, xsize, ysize, GeoT, Proj_projcs, Proj_geogcs, DataType = getRasterInfo(InDir + '/' + InRas)
            print DataType
            
            # Check if we need to reclass any raster values
            if ReclassTable=='Yes':
                if RasterAttTable!='No':
                    ingrid = InDir + '/' + InRas
                    reclass_dict = rat_to_dict(ingrid, RasterAttTable.split(';')[0], RasterAttTable.split(';')[1])
                else:    
                    g = ReClassTable.loc[ReClassTable['FileName'] == Rast]
                    lookup = g.set_index('OldVal')['NewVal'].to_dict()
                    OldVal = ReClassTable.loc[ReClassTable['FileName'] == Rast,'OldVal']
                    NewVal = ReClassTable.loc[ReClassTable['FileName'] == Rast,'NewVal']
                    reclass_dict = dict()
                    # Need to pull values out of a pandas series as a simple integer or float to use in reclass
                    for i in OldVal.index.tolist():
                        print i
    #                    if not np.isnan(OldVal).any() and not np.isnan(NewVal).any() and not DataType.count('Float'):
    #                        reclass_dict[int(OldVal[i])] = NewVal=int(NewVal[i])    
    #                    if not np.isnan(OldVal).any() and not np.isnan(NewVal).any() and DataType.count('Float'):
                        reclass_dict[float(OldVal[i])] = float(NewVal[i])
                tempras = TempDir + '/' + Rast + '.tif'
                Reclass(InDir + '/' + InRas, tempras, reclass_dict, in_nodata = NDV, out_dtype=RastType)
            # Check if we need to multiply or modify any raster values
            if not ModifyBy==0:
                tempras = TempDir + '/' + Rast + '_2.tif'
                inras = InDir + '/' + InRas
                # for now we assume multiplication of values but may modify this in future
                rasterMath(inras, tempras, expression= inras + ' * ' + str(ModifyBy), out_dtype=RastType)
        
            
            # if temp raster hasn't been created in previous steps, just poiint to input raster
            if not os.path.isfile(TempDir + '/' + Rast + '.tif') or not os.path.isfile(TempDir + '/' + Rast + '_2.tif'):
                if FileType=='ESRI raster':
                    tempras = InDir + '/' + Rast
                if FileType=='Geotiff':
                    tempras = InDir + '/' + Rast + '.tif' 
                elif FileType=='Image file':
                    tempras = InDir + '/' + Rast + '.img' 
            
            # get raster info from temp raster
            NDV, Stats, xsize, ysize, GeoT, Proj_projcs, Proj_geogcs, DataType = getRasterInfo(tempras)
            minx = GeoT[0]
            maxy = GeoT[3]
            maxx = minx + GeoT[1]*xsize
            miny = maxy + GeoT[5]*ysize
            bounds = str(minx) + " " + str(miny) + " " + str(maxx) + " " + str(maxy)
            outDataType = DataType
            outNDV = NDV
            dst_crs = 'EPSG:5070'
            
            # If final file doesn't already exists use gdal resample to resample to desired resolution and project.  
            # Also apply a mask if needed to produce final raster
            if not os.path.isfile(FinalDir + '/' + Rast + '.tif'):
               if UseArcpy=='Yes': 
                   if UseStatesMask=='No':
                       # Project to temporary raster      
                       finalras = FinalDir + '/' + Rast + '.tif'
                       arcpy.ProjectRaster_management(tempras, finalras, "PROJCS['NAD_1983_Contiguous_USA_Albers',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['false_easting',0.0],PARAMETER['false_northing',0.0],PARAMETER['central_meridian',-96.0],PARAMETER['standard_parallel_1',29.5],PARAMETER['standard_parallel_2',45.5],PARAMETER['latitude_of_origin',23.0],UNIT['Meter',1.0]]", "NEAREST", "1000 1000", "", "", "PROJCS['NAD_1983_Albers',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['central_meridian',-96.0],PARAMETER['Standard_Parallel_1',29.5],PARAMETER['Standard_Parallel_2',45.5],PARAMETER['latitude_of_origin',23.0],UNIT['Meter',1.0]]")
                   if UseStatesMask=='Yes':
                        # Project to temporary raster      
                        projras = TempDir + '/' + Rast + 'proj.tif'
                        # Execute ExtractByMask
                        MaskRas = 'L:/Priv/CORFiles/Geospatial_Library/Data/RESOURCE/POLITICAL/BOUNDARIES/NATIONAL/States_limited_borders.shp'
                        from arcpy.sa import *
                        arcpy.CheckOutExtension("Spatial")
                        outExtractByMask = ExtractByMask(projras, MaskRas)
        #               Save the output      
                        finalras = FinalDir + '/' + Rast + '.tif'
                        outExtractByMask.save(finalras)
               if UseArcpy=='No':
                   if not Proj_projcs==dst_crs and not type(MaskRas) is str:
                        resamp_ras = FinalDir + '/' + Rast + '.tif'
                        resamp_string = "gdalwarp --config GDAL_DATA " + '"C:/Users/mweber/AppData/Local/Continuum/Anaconda/pkgs/libgdal-1.11.2-2/Library/data" ' +' -tr ' + str(ConvertRes) + ' -' + str(ConvertRes) + " -te " + bounds + " -srcnodata " + str(outNDV) +  " -dstnodata "  + str(outNDV) +  " -of GTiff -r near -t_srs " + dst_crs + " -co COMPRESS=DEFLATE -co TFW=YES -co TILED=YES -co TIFF_USE_OVR=TRUE -ot " + outDataType + " " + tempras + " " + resamp_ras
                        startTime = dt.now()
                        call(resamp_string)
                        print "elapsed time " + str(dt.now()-startTime)
                        
                   if not Proj_projcs==dst_crs and type(MaskRas) is str:
                        resamp_ras = TempDir + '/' + Rast + 'Resmp.tif'
                        resamp_string = "gdalwarp --config GDAL_DATA " + '"C:/Users/mweber/AppData/Local/Continuum/Anaconda/pkgs/libgdal-1.11.2-2/Library/data" ' +' -tr ' + str(ConvertRes) + ' -' + str(ConvertRes) + " -te " + bounds + " -srcnodata " + str(outNDV) +  " -dstnodata "  + str(outNDV) +  " -of GTiff -r near -t_srs " + dst_crs + " -co COMPRESS=DEFLATE -co TFW=YES -co TILED=YES -co TIFF_USE_OVR=TRUE -ot " + outDataType + " " + tempras + " " + resamp_ras
                        startTime = dt.now()
                        call(resamp_string)
                        print "elapsed time " + str(dt.now()-startTime)
        # Processes for vector features
        if FileType== 'ESRI Shapefile':
            Feat = gpd.GeoDataFrame.from_file(InDir + '/' + Rast + '.shp')
            # just for census block groups, subset just to CONUS
            if Rast=='tl_2010_US_bg10':
                Feat = Feat.loc[~Feat['STATEFP10'].isin(['02','15','72'])]
            # check if we need to create fields and / or run calculations on fields in shapefile
            if ShapeFieldCalc=='Yes':
                # first project if needed to Albers Equal Area
                if not Feat.crs['proj']=='aea':
                    Feat = Feat.to_crs(epsg=5070)
                # gather all the values from the ShapefileFieldCalc control table
                f = FieldCalcTable.loc[FieldCalcTable['FileName'] == Rast]
                JoinTable = f.loc[f['FileName'] == Rast,'JoinTable']
                InField = f.loc[f['FileName'] == Rast,'InField']
                OutField= f.loc[f['FileName'] == Rast,'OutField']
                Operation= f.loc[f['FileName'] == Rast,'Operation']
                Value= f.loc[f['FileName'] == Rast,'Value']
        
                # iterate through processes to run for each feature in the ShapfileFieldCalc control table
                rangelist = JoinTable.index.tolist()
                for k in rangelist:
                    if not pd.isnull(JoinTable[k]) and not InField[k] in list(Feat):
                        lookup = dbf2DF(InDir + '/' + JoinTable[k])
                        lookup=lookup[[InField[k],'STCNTRBG']]
                        lookup.rename(columns={'STCNTRBG':'GEOID10'}, inplace=True)
    #                    Feat = pd.merge(left=Feat,right=lookup, how='left', left_on='GEOID10', right_on='STCNTRBG')
                        
                        Feat= Feat.merge(lookup, on='GEOID10')
                    if InField[k]=='AREA':
                        expression= 'Feat.area * %f'%(float(Value[k]))
                        Feat[OutField[k]]=eval(expression)
                        pass
                    elif Operation[k]=='Multiply':
                        # checking if value is string determines if we're just using existing field in expression rather than a value
#                        if not type(Value[k])==str:
#                            expression= 'Feat.%s * %f'%(InField[k],float(Value[k]))
#                        if type(Value[k])==str:
                        expression= 'Feat.%s * %s'%(InField[k],Value[k])
                        Feat[OutField[k]] = eval(expression)
                    elif Operation[k]=='Divide':
                        # checking if value is string determines if we're just using existing field in expression rather than a value
                        if not type(Value[k])==str:
                            expression= 'Feat.%s / %f'%(InField[k],Value[k])
                        if type(Value[k])==str:
                            expression= 'Feat.%s / Feat.%s'%(InField[k],Value[k])
                        Feat[OutField[k]] = eval(expression)
            if not Feat.crs['proj']=='aea':
                Feat = Feat.to_crs(epsg=5070)
            Feat.to_file(FinalDir + '/' + Rast + '.shp', driver = 'ESRI Shapefile')
            # Do we need to rasterize shapefile? (Right now only for census block groups)
            if Convert=='Yes':
                for item in ConvertFields.split(';'):    
                    InShp = FinalDir + '/' + Rast + '.shp'
        #            InShp = InDir + '/' + Rast + '.shp'
                    OutRas =  FinalDir + '/' + item + '.tif'
                    resamp_string = 'gdal_rasterize -a ' + item + ' -l ' + Rast +' -tr ' + str(ConvertRes) + ' -' + str(ConvertRes) + ' -co COMPRESS=DEFLATE ' +  InShp + ' ' + OutRas
                    startTime = dt.now()
                    call(resamp_string)
                    print "elapsed time " + str(dt.now()-startTime)
                    


