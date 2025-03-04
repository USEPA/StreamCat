import pandas as pd
from datetime import datetime as dt
import os
import sys
from StreamCat_functions import Reclass, rasterMath, getRasterInfo, dbf2DF, rat_to_dict, getRasterInfo_xarray
import geopandas as gpd
from subprocess import call

# NOTE: run script from command line passing directory and name of this script
# and then directory and name of the control table to use like:
# > Python "F:\Watershed Integrity Spatial Prediction\Scripts\StreamCat_PreProcessing.py"
# L:\Priv\CORFiles\Geospatial_Library\Data\Project\SSWR1.1B\ControlTables\RasterControlTable.csv
#--------------------------------------------------------
def preprocess():
    # sys.path.append('F:/Watershed Integrity Spatial Prediction/Scripts')
    ControlTable  = pd.read_csv("RasterControlTable.csv") # sys.argv[1]
    ReClassTable = pd.read_csv("ReclassTable.csv") # ControlTable.DirectoryLocations[4]
    FieldCalcTable = pd.read_csv("FieldCalcTable.csv") # ControlTable.DirectoryLocations[5]

    # Populate variables from control table
    InDir = "L:/Priv/CORFILES/Geospatial_Library_Projects/StreamCat/LandscapeRasters/NeedsQA" # ControlTable.DirectoryLocations[0] # L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat/LandscapeRasters/NeedsQA
    TempDir = 'temp' # ControlTable.DirectoryLocations[1] # C:/Users/mweber/Temp
    FinalDir = ControlTable.DirectoryLocations[2] # L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat/LandscapeRasters/QAComplete
    MaskRas = ControlTable.DirectoryLocations[3] # F:/Watershed Integrity Spatial Prediction/Scripts

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

    #for line in ControlTable.values: # loop through each landscape_var in control table
    for _, line in ControlTable.query("Run == 1").iterrows():
        #if line[-1] == 1:   # check 'run' field from the table, if 1 run, if not, skip
        print(f"Running {str(line.values[2])}")
        # "L:\Priv\CORFILES\Geospatial_Library_Projects\StreamCat\LandscapeRasters\NeedsQA\nlcd2011.tif"
        InFile = line.values[2]
        OutFile = line.values[3]
        FileType = line.values[4]
        DataCategory = line.values[5]
        ModifyBy = line.values[6]
        RastType = line.values[7]
        ReclassTable = line.values[8]
        RasterAttTable = line.values[9]
        ShapeFieldCalc = line.values[10]
        Convert = line.values[11]
        ConvertFields = line.values[12]
        ConvertRes = int(line.values[13])
        # UseArcpy = line.values[14]
        UseStatesMask = line.values[15]

        if FileType != 'ESRI Shapefile':
            #get raster info for input raster
            if FileType == 'Image file':
                InRas = InFile + '.img'
            elif FileType == 'Geotiff':
                InRas = InFile + '.tif'
            else:
                InRas = InFile

            # TODO use rioxarray to do this instead of gdal
            raster, NDV, Stats, xsize, ysize, GeoT, Proj_projcs, Proj_geogcs, DataType, bounds = getRasterInfo_xarray(os.path.join(InDir,InRas))
            # NDV, Stats, xsize, ysize, GeoT, Proj_projcs, Proj_geogcs, DataType = getRasterInfo(InDir + '/' + InRas)
            print(f"Datatype: {DataType}")

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
                        print(i)
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
            # TODO replace this with bounds = rioxarray.rio.bounds() Should return it on line 70 with getRasterInfo_xarray function
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
                # OLD COMMENT: Need to add ability to mask as well with gdal / rasterio approach...
                # TODO use mask / window read approach with xarray spatial techniques
                if not Proj_projcs==dst_crs:
                    resamp_ras = FinalDir + '/' + OutFile + '.tif'
                    # change from gdal to raster.rio.reproject
                    resamp_string = "gdalwarp --config GDAL_DATA " + '"C:/Users/mweber/AppData/Local/Continuum/Anaconda/pkgs/libgdal-1.11.2-2/Library/data" ' +' -tr ' + str(ConvertRes) + ' -' + str(ConvertRes) + " -te " + bounds + " -srcnodata " + str(outNDV) +  " -dstnodata "  + str(outNDV) +  " -of GTiff -r near -t_srs " + dst_crs + " -co COMPRESS=DEFLATE -co TFW=YES -co TILED=YES -co TIFF_USE_OVR=TRUE -ot " + outDataType + " " + tempras + " " + resamp_ras
                    startTime = dt.now()
                    call(resamp_string)
                    print(f"elapsed time: {str(dt.now()-startTime)}")
        
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
                    print(item)
                    InShp = FinalDir + '/' + InFile + '.shp'
        #            InShp = InDir + '/' + Rast + '.shp'
                    OutRas =  FinalDir + '/' + item + '.tif'
#                    resamp_string = 'gdal_rasterize -a ' + item + ' -l ' + InFile +' -tr ' + str(ConvertRes) + ' -' + str(ConvertRes) + ' -co COMPRESS=DEFLATE ' +  InShp + ' ' + OutRas
                    startTime = dt.now()
#                    call(resamp_string)
                    ##  call() statement not working for me, use arcpy, rickD

                    # TODO replace following conversion with different function
                    #arcpy.PolygonToRaster_conversion(InShp, item, OutRas, 'CELL_CENTER', "", str(ConvertRes))
                    print(f"elapsed time: {str(dt.now()-startTime)}")

if __name__ == '__main__':
    preprocess()