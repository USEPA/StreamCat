library(arrow)
library(terra)
library(tictoc)
library(foreign)
library(tidyverse)

#-------------------------------------------------------
# Optional: threads
#-------------------------------------------------------
Sys.setenv(GDAL_NUM_THREADS = "ALL_CPUS")
Sys.setenv(OMP_NUM_THREADS  = as.character(parallel::detectCores()))
terraOptions(threads = parallel::detectCores(), memfrac = 0.9)

region_id <- "Region17"

#-------------------------------------------------------
# Prepare region catchment raster
#-------------------------------------------------------
source("./StreamCatR/zonal_stats_r/01.build_optimized_catchments.R")
catchment_path   <- "C:/Users/RHill04/WorkFolder/GIS/NHDPlusV21/NHDPlusPN/NHDPlus17/NHDPlusCatchment/cat"
tic()
prepare_optimized_catchments(
  catchment_path   = catchment_path,
  region_id        = region_id,
  blocksize        = 6144L,
  outdir           = "./StreamCatR/zonal_stats_r/cat_rasters",
  target_crs       = "EPSG:5070",
  overwrite_rasters = TRUE,
  build_windows     = TRUE,  
  progress_every   = 0L
)
toc()

#-------------------------------------------------------
# Optimize continuous raster (block size)
#-------------------------------------------------------
source("./StreamCatR/zonal_stats_r/02.optimize_continuous_raster.R")

indir <- "./StreamCatR/zonal_stats_r/landscape_rasters/"
#blocksize = "512"
inras <- "bfi"

optimize_continuous_raster(
  infile     = file.path("./StreamCatR/zonal_stats_r/landscape_rasters", "bfi.tif"),
  outdir     = "./StreamCatR/zonal_stats_r/landscape_rasters",
  target_crs = "EPSG:5070",     # skip or set NULL if no reprojection needed
  resample   = "bilinear",      # keep for continuous data
  datatype   = "FLT4S"          # float for continuous rasters
)

#-------------------------------------------------------
# Run zonal
#-------------------------------------------------------
source("./StreamCatR/zonal_stats_r/04.zonal_functions_true_blocking.R")
source("./StreamCatR/zonal_stats_r/05.run_region_accumulation.R")

# Thread options (optional)
Sys.setenv(GDAL_NUM_THREADS = "ALL_CPUS")
Sys.setenv(OMP_NUM_THREADS  = as.character(parallel::detectCores()))
if (!dir.exists("./terra")) dir.create("./terra", recursive = TRUE, showWarnings = FALSE)
terra::terraOptions(threads = parallel::detectCores(),
                    tempdir = "./StreamCatR/zonal_stats_r/terra",
                    memfrac = 0.9)

tictoc::tic()
dt2 <- run_region_accumulation(
  region_id = region_id,
  zone_dir = "./StreamCatR/zonal_stats_r/cat_rasters",                 # folder with Zidx and parquet files
  predictor_path = "./StreamCatR/zonal_stats_r/landscape_rasters/bfi_512.tif",           # full path to the predictor raster (any tiling/size)
  blocksize = 6144,                # e.g., 6144L (used only for Zidx/windows)
  method = "near",
  stats  = "sum",
  progress_every = 0L,
)
tictoc::toc()
head(dt2)

#-------------------------------------------------------
# QA
#-------------------------------------------------------

dbf_folder <- "//aa/ord/ORD/DATA/LAB/COR/Geospatial_Library_Projects/StreamCat/Allocation_and_Accumulation/DBF_stash/"

test <- 
  foreign::read.dbf(paste0(dbf_folder, "zonalstats_bfi16.dbf")) %>% 
  dplyr::select(VALUE, SUM, COUNT) %>% 
  dplyr::rename(SUM_ORIG = SUM,
                COUNT_ORIG = COUNT)


dt2 <- dt2 %>% 
  dplyr::rename(VALUE = GRIDCODE,
                SUM_NEW = sum,
                COUNT_NEW = n)

test2 <- test %>% 
  dplyr::left_join(dt2, 
                   by = "VALUE") %>% 
  dplyr::mutate(diff_sum = SUM_ORIG - SUM_NEW,
                diff_count = COUNT_ORIG - COUNT_NEW,
                mean_orig = SUM_ORIG / COUNT_ORIG,
                mean_new = SUM_NEW / COUNT_NEW,
                diff_mean = mean_orig - mean_new)

View(test2)

