# StreamCat

## Description: 
The StreamCat Dataset (http://www2.epa.gov/national-aquatic-resource-surveys/streamcat) provides summaries of natural and anthropogenic landscape features for ~2.65 million streams, and their associated catchments, within the conterminous USA. This repo contains code used in StreamCat to process a suite of landscape rasters to watersheds for streams and their associated catchments (local reach contributing area) within the conterminous USA using the [NHDPlus Version 2](http://www.horizon-systems.com/NHDPlus/NHDPlusV2_data.php) as the geospatial framework.

## Necessary Python Packages and Installation Tips
The scripts for StreamCat rely on several python modules a user will need to install such as numpy, gdal, rasterio, geopandas, shapely and ArcPy with an ESRI license (minimal steps still using ArcPy).  We highly recommend using a scientific python distribution such as [Anaconda](https://www.continuum.io/downloads) or [Enthought Canopy](https://www.enthought.com/products/canopy/).  We used the conda package manager to install necessary python modules. Note that package configurations and dependencies are sensitive and can change - in particular, setting up an environment with a working version of both `geopandas` and `arcpy` can be challenging. Our working version of the conda environment is contained in the StreamCat.yml file in the repository, and our essential packages and versions when code was last used are listed below - note that other configurations may work, we simply have verified this particular combination (Windows 64 and Python 3.7.10):

| Package       | Version       | 
| ------------- |--------------:|
| python        | 3.7.10        | 
| fiona         | 1.8.18        | 
| gdal          | 3.1.4=py37    | 
| geopandas     | 0.9.0         |  
| geos          | 3.9.1         |
| libgdal       | 3.1.4         |
| numpy         | 1.19.5        |
| pandas        | 1.2.5         |
| pyproj        | 3.1.0         |
| rasterio      | 1.2.1=py37    |
| shapely       | 1.7.1         |

If you are using Anaconda, creating a new, clean 'StreamCat' environment with these needed packages can be done one of several ways:

* In your conda shell, add one necessary channel and then download the streamcat environment from the Anaconda cloud:
  + conda config --add channels conda-forge
  + conda env create mweber36/StreamCat
  
* Alternatively, using the streamcat.yml file in this repository, in your conda shell cd to the directory where your streamcat.yml file is located and run:
  + conda env create -f StreamCat.yml
  
* To build environment yourself, we [followed the steps suggest here](https://www.e-education.psu.edu/geog489/node/2348) which are:
  + conda create -n StreamCat -c conda-forge python=3.7 anaconda gdal=3.1.4  jupyter pandas geopandas matplotlib cartopy shapely rasterio numpy=1.19.5 spyder

* Activate the new environment:

  + conda activate StreamCat

* 
* To open Spyder, type the following at the conda prompt
  + activate StreamCat
  
  Then

  + Spyder

Finally, to use arcpy in this new environment, you will need to copy several ArcPro files and folders to your new environment as follows:

+ C:/Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3/Lib/site-packages/Arcgisscripting 

+ C:/Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3/Lib/site-packages/arcpy_wmx

+ C:/Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3/Lib/site-packages/gapy

+ C:/Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3/Lib/site-packages/bapy

To your environment directory which should look something like:

+ C:/Users/mweber/AppData/Local/Continuum/anaconda3/envs/StreamCat/Lib/site-packages

In order to use `arcpy`, at the python command prompt or in your script, you need to run:

+ import os,sys
+ os.environ["PATH"] += r";C:\Program Files\ArcGIS\Pro\bin"
+ sys.path.append(r"C:\Program Files\ArcGIS\Pro\Resources\ArcPy")
+ import arcpy

Note that the exact paths may vary depending on the version of ArcGIS and Anaconda you have installed and the configuration of your computer

## How to Run Scripts
### The scripts make use of 'control tables' to pass all the particular parameters to the three primary scripts: 

+ [StreamCat_PreProcessing.py](https://github.com/USEPA/StreamCat/blob/master/StreamCat_PreProcessing.py)
+ [StreamCat.py](https://github.com/USEPA/StreamCat/blob/master/StreamCat.py)
+ [MakeFinalTables.py](https://github.com/USEPA/StreamCat/blob/master/StreamCat_functions.py).  

In turn, these scripts rely on a generic functions in [StreamCat_functions.py](https://github.com/USEPA/StreamCat/blob/master/StreamCat_functions.py). 

To generate the riparian buffers we used in [StreamCat](ftp://newftp.epa.gov/EPADataCommons/ORD/NHDPlusLandscapeAttributes/StreamCat/Documentation/ReadMe.html) we used the code in [RiparianBuffers.py](https://github.com/USEPA/StreamCat/blob/master/RiparianBuffer.py) 

To generate percent full for catchments on the US border for point features, we used the code in [border.py](https://github.com/USEPA/StreamCat/blob/master/border.py)

Examples of control tables used in scripts are:
+ [RasterControlTable](https://github.com/USEPA/StreamCat/blob/master/RasterControlTable.csv)
+ [ReclassTable](https://github.com/USEPA/StreamCat/blob/master/ReclassTable.csv)
+ [FieldCalcTable.](https://github.com/USEPA/StreamCat/blob/master/FieldCalcTable.csv)
+ [Lithology_lookup](https://github.com/USEPA/StreamCat/blob/master/Lithology_lookup.csv)
+ [NLCD2006_lookup](https://github.com/USEPA/StreamCat/blob/master/NLCD2006_lookup.csv)
+ [ControlTable_StreamCat](https://github.com/USEPA/StreamCat/blob/master/ControlTable_StreamCat.csv)
+ [MakeFinalTables](https://github.com/USEPA/StreamCat/blob/master/MakeFinalTables.csv)

### Running StreamCat.py to generate new StreamCat metrics

After editing the control tables to provide necessary information, such as directory paths, the following stesps will excecute processes to generate new watershed metrics for the conterminous US. All examples in the control table are for layers (e.g., STATSGO % clay content of soils) that were processed as part of the StreamCat Dataset. This example assumes run in Anaconda within Conda shell.

1. Edit [ControlTable_StreamCat](https://github.com/USEPA/StreamCat/blob/master/ControlTable_StreamCat.csv) and set desired layer's "run" column to 1. All other columns should be set to 0
2. Open a Conda shell and type "activate StreamCat" 
3. At the Conda shell type: "Python<space>"
4. Drag and drop "StreamCat.py" to the Conda shell from a file manager followed by another space
5. Drag and drop the control table to the Conda shell

Final text in Conda shell should resemble this: python C:\some_path\StreamCat.py  C:\some_other_path\ControlTable.csv


## EPA Disclaimer
The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer has responsibility to protect the integrity , confidentiality, or availability of the information.  Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or the United States Government.
