import pandas as pd
import gdal

inraster = "E:/sbox/build_rat/nlcd_2019_land_cover_l48_20210604.tif"
def get_rat_vals(raster):
    """
    Given a GDAL raster attribute table, convert to a pandas DataFrame.  Idea from
    Matt Gregory's gist: https://gist.github.com/grovduck/037d815928b2a9fe9516

    # Open the raster and get a handle on the raster attribute table
    # Assume that we want the first band's RAT
    Parameters
    ---------
    raster : str
        Absolute path to raster

    Returns
    ---------
    list :
    """
    ds = gdal.Open(raster)
    rb = ds.GetRasterBand(1)
    rat = rb.GetDefaultRAT()
    df = pd.DataFrame.from_dict(
            {rat.GetNameOfCol(i): rat.ReadAsArray(i)
                for i in range(rat.GetColumnCount())}
            )
    return df.loc[(df.Opacity > 0) & (df.Histogram > 0)].index.tolist()


    o = pd.DataFrame(pd.Series(rat.ReadAsArray(i), name=rat.GetNameOfCol(i))
        for i in range(rat.GetColumnCount())
    )
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

import gdal, ogr, os, osr
import numpy as np


def array2raster(newRasterfn,rasterOrigin,pixelWidth,pixelHeight,array):

    cols = array.shape[1]
    rows = array.shape[0]
    originX = rasterOrigin[0]
    originY = rasterOrigin[1]

    driver = gdal.GetDriverByName('GTiff')
    outRaster = driver.Create(newRasterfn, cols, rows, 1, gdal.GDT_Byte)
    outRaster.SetGeoTransform((originX, pixelWidth, 0, originY, 0, pixelHeight))
    outband = outRaster.GetRasterBand(1)
    outband.WriteArray(array)
    outRasterSRS = osr.SpatialReference()
    outRasterSRS.ImportFromEPSG(4326)
    outRaster.SetProjection(outRasterSRS.ExportToWkt())
    outband.FlushCache()


def main(newRasterfn,rasterOrigin,pixelWidth,pixelHeight,array):
    reversed_arr = array[::-1]
    array2raster(newRasterfn,rasterOrigin,pixelWidth,pixelHeight,reversed_arr)


if __name__ == "__main__":
    rasterOrigin = (-123.25745,45.43013)
    pixelWidth = 10
    pixelHeight = 10
    newRasterfn = 'test.tif'
    array = np.array([[ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                      [ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                      [ 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 1, 1],
                      [ 1, 0, 1, 1, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 1],
                      [ 1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1, 1, 1],
                      [ 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 1],
                      [ 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 0, 0, 1],
                      [ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                      [ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                      [ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]])


    main(newRasterfn,rasterOrigin,pixelWidth,pixelHeight,array)
