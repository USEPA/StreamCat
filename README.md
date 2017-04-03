# StreamCat

## Description: 
The StreamCat Dataset (http://www2.epa.gov/national-aquatic-resource-surveys/streamcat) provides summaries of natural and anthropogenic landscape features for ~2.65 million streams, and their associated catchments, within the conterminous USA. This repo contains code used in StreamCat to process a suite of landscape rasters to watersheds for streams and their associated catchments (local reach contributing area) within the conterminous USA using the [NHDPlus Version 2](http://www.horizon-systems.com/NHDPlus/NHDPlusV2_data.php) as the geospatial framework.

##Necessary Python Packages and Installation Tips
The scripts for StreamCat rely on several python modules a user will need to install such as numpy, pandas, gdal, fiona, rasterio, geopandas, shapely, pysal, and ArcPy with an ESRI license (minimal steps still using ArcPy).  We highly recommend using a scientific python distribution such as [Anaconda](https://www.continuum.io/downloads) or [Enthought Canopy](https://www.enthought.com/products/canopy/).  We used the conda package manager to install necessary python modules. Our essential packages and versions used are listed below (Windows 64 and Python 2.7.11):

| Package       | Version       | 
| ------------- |--------------:|
| fiona         | 1.6.3         | 
| gdal          | 1.11.4        | 
| geopandas     | 0.2.0.dev     |  
| geos          | 3.4.2         |
| libgdal       | 2.0.0         |
| numpy         | 1.10.1        |
| pandas        | 0.18.1        |
| pyproj        | 1.9.5         |
| pysal         | 1.10.0        |
| rasterio      | 0.34.0        |
| shapely       | 1.5.15        |

If you are using Anaconda, creating a new, clean 'streamcat' environment with these needed packages can be done easily and simply one of two ways:

* In your conda shell, add one necessary channel and then download the streamcat environment from the Anaconda cloud:
  + conda config --add channels ioos
  + conda env create mweber36/streamcat
  
* Alternatively, using the streamcat.txt file in this repository, in your conda shell cd to the directory where your streamcat.txt file is located and run:
  + conda create --name streamcat --file streamcat.txt

* To activate this new environment and open Spyder, type the following at the conda prompt
  + activate streamcat
  
  Then

  + spyder

Finally, to use arcpy in this new environment, you will need to copy your Arc .pth file into your new environment.  Copy the .pth file for your install of ArcGIS located in a directory like:

+ C:\Python27\ArcGISx6410.3\Lib\site-packages\DTBGGP64.pth

To your environment directory which should look something like:

+ C:\Anaconda\envs\streamcat\Lib\site-packages\DTBGGP64.pth

Note that the exact paths may vary depending on the version of ArcGIS and Anaconda you have installed and the configuration of your computer

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

###Running StreamCat.py to generate new StreamCat metrics

After editing the control tables to provide necessary information, such as directory paths, the following stesps will excecute processes to generate new watershed metrics for the conterminous US. All examples in the control table are for layers (e.g., STATSGO % clay content of soils) that were processed as part of the StreamCat Dataset. This example assumes run in Anaconda within Conda shell.

1. Edit [ControlTable_StreamCat](https://github.com/USEPA/StreamCat/blob/master/ControlTable_StreamCat.csv) and set desired layer's "run" column to 1. All other columns should be set to 0
2. Open a Conda shell and type "activate StreamCat" 
3. At the Conda shell type: "Python<space>"
4. Drag and drop "StreamCat.py" to the Conda shell from a file manager followed by another space
5. Drag and drop the control table to the Conda shell

Final text in Conda shell should resemble this: python C:\some_path\StreamCat.py  C:\some_other_path\ControlTable.csv


## EPA Disclaimer
The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer has responsibility to protect the integrity , confidentiality, or availability of the information.  Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or the United States Government.

