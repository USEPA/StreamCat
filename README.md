# StreamCat

##Description: 
The StreamCat Dataset (http://www2.epa.gov/national-aquatic-resource-surveys/streamcat) provides summaries of natural and anthropogenic landscape features for ~2.65 million streams, and their associated catchments, within the conterminous USA. This repo contains code used in StreamCat to process a suite of landscape rasters to watersheds for streams and their associated catchments (local reach contributing area) within the conterminous USA using the [NHDPlus Version 2](http://www.horizon-systems.com/NHDPlus/NHDPlusV2_data.php) as the geospatial framework.

##Necessary Python Packages and Installation Tips
The scripts for StreamCat rely on several python modules a user will need to install, including numpy, pandas, osgeo, fiona, rasterio, geopandas, shapely, pysal, and ArcPy with an ESRI license (minimal steps still using ArcPy).  We highly recommend using a scientific python distribution such as [Anaconda](https://www.continuum.io/downloads) or [Enthought Canopy](https://www.enthought.com/products/canopy/).  We used the conda package manager to install necessary python modules. our environment and essential packages and versions used are listed below (Windows 64 and Python 2.7.11):

| Package       | Version       | 
| ------------- |--------------:|
| fiona         | 1.6.0         | 
| gdal          | 1.11.2        | 
| geopandas     | 0.1.0.dev     |  
| geos          | 3.4.2         |
| libgdal       | 2.0.0         |
| numpy         | 1.10.1        |
| pandas        | 0.17.1        |
| pyproj        | 1.9.4         |
| pysal         | 1.10.0        |
| pyshp         | 1.2.3         |
| rasterio      | 0.24.0        |
| shapely       | 1.5.13        |

##How to Run Scripts
###The scripts make use of 'control tables' to pass all the particular parameters to the three primary scripts: 
+ [StreamCat_PreProcessing.py](https://github.com/USEPA/StreamCat/blob/master/StreamCat_PreProcessing.py)
+ [StreamCat.py](https://github.com/USEPA/StreamCat/blob/master/StreamCat.py)
+ [MakeFinalTables.py](https://github.com/USEPA/StreamCat/blob/master/StreamCat_functions.py).  

In turn, these scripts rely on a generic functions in [StreamCat_functions.py](https://github.com/USEPA/StreamCat/blob/master/StreamCat_functions.py). 

To generate the riparian buffers we used in [StreamCat](ftp://newftp.epa.gov/EPADataCommons/ORD/NHDPlusLandscapeAttributes/StreamCat/Documentation/ReadMe.html) we used the code in [RiparianBuffers.py](https://github.com/USEPA/StreamCat/blob/master/RiparianBuffer.py) 

Examples of control tables used in scripts are:
+ [RasterControlTable](https://github.com/USEPA/StreamCat/blob/master/RasterControlTable.csv)
+ [ReclassTable](https://github.com/USEPA/StreamCat/blob/master/ReclassTable.csv)
+ [FieldCalcTable.](https://github.com/USEPA/StreamCat/blob/master/FieldCalcTable.csv)
+ [Lithology_lookup](https://github.com/USEPA/StreamCat/blob/master/Lithology_lookup.csv)
+ [NLCD2006_lookup](https://github.com/USEPA/StreamCat/blob/master/NLCD2006_lookup.csv)
+ [ControlTable_StreamCat](https://github.com/USEPA/StreamCat/blob/master/ControlTable_StreamCat.csv)
+ [MakeFinalTables](https://github.com/USEPA/StreamCat/blob/master/MakeFinalTables.csv)

## EPA Disclaimer
The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer has responsibility to protect the integrity , confidentiality, or availability of the information.  Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or the United States Government.

