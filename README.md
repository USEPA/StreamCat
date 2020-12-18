# StreamCat

## Description: 
The StreamCat Dataset (http://www2.epa.gov/national-aquatic-resource-surveys/streamcat) provides summaries of natural and anthropogenic landscape features for ~2.65 million streams, and their associated catchments, within the conterminous USA. This repo contains code used in StreamCat to process a suite of landscape rasters to watersheds for streams and their associated catchments (local reach contributing area) within the conterminous USA using the [NHDPlus Version 2](http://www.horizon-systems.com/NHDPlus/NHDPlusV2_data.php) as the geospatial framework. See [Running-StreamCat-Scripts](https://github.com/USEPA/StreamCat/wiki/Running-StreamCat-Scripts) for details on running the scripts to produce StreamCat data.

## Necessary Python Packages and Installation Tips
The scripts for StreamCat rely on several python modules a user will need to install such as numpy, pandas, gdal, fiona, rasterio, geopandas, shapely, pysal, and ArcPy with an ESRI license (minimal steps still using ArcPy).  We highly recommend using a scientific python distribution such as [Anaconda](https://www.continuum.io/downloads) or [Enthought Canopy](https://www.enthought.com/products/canopy/).  We used the conda package manager to install necessary python modules. Our essential packages and versions used are listed below (Windows 64 and Python 2.7.11):

| Package       | Version       | 
| ------------- |--------------:|
| fiona         | 1.7.7         | 
| gdal          | 2.2.0         | 
| geopandas     | 0.2.1         |  
| geos          | 3.5.1         |
| libgdal       | 2.0.0         |
| numpy         | 1.12.1        |
| pandas        | 0.20.2        |
| pyproj        | 1.9.5.1       |
| pysal         | 1.13.0        |
| rasterio      | 1.0a9         |
| shapely       | 1.5.17        |

If you are using Anaconda, creating a new, clean 'StreamCat' environment with these needed packages can be done easily and simply one of several ways:

* In your conda shell, add one necessary channel and then download the streamcat environment from the Anaconda cloud:
  + conda config --add channels conda-forge
  + conda env create mweber36/streamcat
  
* Alternatively, using the streamcat.yml file in this repository, in your conda shell cd to the directory where your streamcat.yml file is located and run:
  + conda env create -f StreamCat.yml
  
* To build environment yourself, do:
  + conda env create -n StreamCat rasterio geopandas
  + pip install georasters

* To activate this new environment and open Spyder, type the following at the conda prompt
  + activate Streamcat
  
  Then

  + Spyder

Finally, to use arcpy in this new environment, you will need to copy your Arc .pth file into your new environment.  Copy the .pth file for your install of ArcGIS located in a directory like:

+ C:\Python27\ArcGISx6410.3\Lib\site-packages\DTBGGP64.pth

To your environment directory which should look something like:

+ C:\Anaconda\envs\streamcat\Lib\site-packages\DTBGGP64.pth

Note that the exact paths may vary depending on the version of ArcGIS and Anaconda you have installed and the configuration of your computer


## EPA Disclaimer
The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer has responsibility to protect the integrity , confidentiality, or availability of the information.  Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or the United States Government.

