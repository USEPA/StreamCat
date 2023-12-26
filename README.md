[![DOI Badge](https://zenodo.org/badge/45130222.svg)](https://zenodo.org/record/8141137)

# StreamCat

## Description: 
[The StreamCat Dataset](https://www.epa.gov/national-aquatic-resource-surveys/streamcat-dataset) provides summaries of natural and anthropogenic landscape features for ~2.65 million streams, and their associated catchments, within the conterminous USA. This repo contains code used in StreamCat to process a suite of landscape rasters to watersheds for streams and their associated catchments (local reach contributing area) within the conterminous USA using the [NHDPlus Version 2](https://www.epa.gov/waterdata/nhdplus-national-hydrography-dataset-plus) as the geospatial framework.

## [Getting Started](https://github.com/USEPA/StreamCat/wiki)
Users will need the following programs installed in order to run the code in the StreamCat GitHub repository:

**Programs:**
Python,  ArcGIS Pro (used to run ZonalStatisticsAsTable and TabulateArea tools with arcpy)

We recommend using the specific conda package management that comes with ArcGIS Pro. You can use the package manager in ArcGIS Pro or conda package management available via the python command prompt with ArcGIS Pro (Start > ArcGIS > Python command prompt). Using either approach, clone the base ArcGIS Pro environment, then install geopandas and rasterio (and optionally spyder) to the new environment. *NOTE - the following steps have been verified to work using ArcGIS Pro 3.2.1*.  In the python command prompt use the following steps (replacing 'myclone' with the environment name of your choice): 

     1. conda create --clone arcgispro-py3 -p %localappdata%\esri\conda\envs\myclone --pinned
     2. activate myclone
     3. conda install geopandas rasterio 
          * optional: conda install spyder
     
We list these specific Python packages needed in the StreamCat code are listed in the [spec-file.txt](https://github.com/USEPA/StreamCat/blob/master/spec-file.txt) in the StreamCat GitHub repository.  Users can use this .txt file to create an environment with the necessary Python libraries by running the following lines at a conda prompt:

1.  Change directory to where you have downloaded the [spec-file.txt](https://github.com/USEPA/StreamCat/blob/master/spec-file.txt) file:
     - for instance: cd C:/UserName/StreamCat
2.  Use the .txt file to create a new environment
     - conda create --name myenv --file spec-file.txt
3.  Use the spec file to install all of its listed packages into this environment
     - conda install --name myenv --file spec-file.txt

**Local directories and files:**
Create a local directory for your working files.  

Make local copies of the [NHDPlusV2 hydrology data](https://www.epa.gov/waterdata/nhdplus-national-hydrography-dataset-plus) and the [StreamCat repository](https://github.com/USEPA/StreamCat.git) and store these in directories on your local machine.

The StreamCat GitHub repository includes a control table, a configuration file,  and Python scripts needed for running metrics.


## [Processing Steps](https://github.com/USEPA/StreamCat/wiki/1.-Landscape-Layer-Processing)

* Download data into dedicated location.
* For raster datasets, save as .tif files (saving from ArcPro, using gdal, or using rasterio in Python)
* In ArcPro use the "Project Raster" tool and set "Output Coordinate System" to "USGS Albers Equal Area Conic"
* Perform a visual inspection of dataset for gaps, edges, and other anomalous features. Verify how "no-data" values are represented and record values as no-data where appropriate (for instance, if "no-data" locations are represented by 0's or -9999,  convert to no-data value such as "null").
* Isolate catchments that exist on US border and clip them to the areas that exist within the US to calculate the percent full for these catchments.
* Record Data source, date, units, and resolution into project tracking spreadsheet (Control Table)
## [How to Run Scripts](https://github.com/USEPA/StreamCat/wiki/2.-Running-StreamCat-Scripts)
### The scripts make use of a [Control Table](https://github.com/USEPA/StreamCat/blob/master/ControlTable_StreamCat.csv) to pass all the particular parameters to the two primary scripts: 
+ [StreamCat.py](https://github.com/USEPA/StreamCat/blob/master/StreamCat.py)
+ [MakeFinalTables.py](https://github.com/USEPA/StreamCat/blob/master/MakeFinalTables.py)  

In turn, these scripts rely on a generic functions in [StreamCat_functions.py](https://github.com/USEPA/StreamCat/blob/master/StreamCat_functions.py). 
And pathways as described by [stream_cat_config.py](https://github.com/USEPA/StreamCat/blob/master/stream_cat_config.py.template) , which will need to be formated and saved as .py to fit your directories

To generate the riparian buffers we used in [StreamCat](https://www.epa.gov/national-aquatic-resource-surveys/streamcat-dataset)
we used the code in [RiparianBuffers.py](https://github.com/USEPA/StreamCat/blob/master/RiparianBuffer.py) 

To generate percent full for catchments on the US border for point features, we used the code in [border.py](https://github.com/USEPA/StreamCat/blob/master/border.py)


### Running StreamCat.py to generate new StreamCat metrics
After editing the control tables to provide necessary information, such as directory paths, the following steps will execute processes to generate new watershed metrics for the conterminous US. This example uses Conda format within Spyder IDE.

1. Edit [ControlTable_StreamCat](https://github.com/USEPA/StreamCat/blob/master/ControlTable_StreamCat.csv) and set desired layer's "run" column to 1. All other rows should be set to 0 in run column.
2. Open a conda shell and type "activate StreamCat".
3. At the conda shell type "spyder" to activate Spyder IDE.
4. Open file selection in Spyder and select your project location
5. Open "StreamCat.py" in the code editor
6. Open "StreamCat_functions.py" and "stream_cat_config.py" as well

### StreamCat Config
1. Save [stream_cat_config.py.template](https://github.com/USEPA/StreamCat/blob/master/stream_cat_config.py.template) as .py in project folder
2. Set LOCAL_DIR to the proper directory. (This will be your project folder)
3.	Ensure the LYR_DIR has proper directory (Normally the QA Complete Rasters).  This will be .tif files that you have stored on local drive.
4.	Check the STREAMCAT_DIR is running through Streamcat Allocations_and_Accumulation folder. * This runs data through Streamcat first in the Allocation and Accumulation folder
5.	Check NHD_DIR is in your own local NHD folder
6.	Check STATES_FILE is your own local folder
7.	Set ACCUM_DIR to "(project file director)/accum_npy/"
    * The **first time** running [StreamCat.py](https://github.com/USEPA/StreamCat/blob/master/StreamCat.py), the ***accum_npy*** folder will need to be removed from project file. This folder will autopopulate with information and files
8.	OUT_DIR goes to local drive
9.	FINAL_DIR goes to FTP Staging Hydroregions

10. Double check that only the metrics you want to run have a 1, all others have a zero
11. Run [StreamCat.py](https://github.com/USEPA/StreamCat/blob/master/StreamCat.py) script


### Make Final Tables
Once [StreamCat.py](https://github.com/USEPA/StreamCat/blob/master/StreamCat.py) has run
1. Open [Make_Final_Tables.py](https://github.com/USEPA/StreamCat/blob/master/Make_Final_Tables.py) in editor
2. These final tables will show up in the OUT_DIR from [stream_cat_config.py](https://github.com/USEPA/StreamCat/blob/master/stream_cat_config.py.template)
3. Run [Make_Final_Tables.py](https://github.com/USEPA/StreamCat/blob/master/Make_Final_Tables.py)


## EPA Disclaimer
The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer has responsibility to protect the integrity , confidentiality, or availability of the information.  Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or the United States Government.
