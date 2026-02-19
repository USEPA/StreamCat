library(arrow)
library(terra)
library(tictoc)
library(foreign)
library(tidyverse)

source("./StreamCatR/zonal_stats_r/01.build_optimized_catchments.R")

#-------------------------------------------------------
# Optional: threads
#-------------------------------------------------------
Sys.setenv(GDAL_NUM_THREADS = "ALL_CPUS")
Sys.setenv(OMP_NUM_THREADS  = as.character(parallel::detectCores()))
terraOptions(threads = parallel::detectCores(), memfrac = 0.9)

root <- "C:/Users/RHill04/WorkFolder/GIS/NHDPlusV21"

cats <- list.files(root, recursive = TRUE, include.dirs = TRUE, full.names = TRUE)
cats <- cats[dir.exists(cats) & basename(cats) == "cat"]

region_folder <- basename(dirname(dirname(cats)))

# Build labels with optional suffix included
region_id <- sub("^NHDPlus(\\d+)([A-Za-z]*)$", "Region\\1\\2", region_folder)
#region_id
blocksize = 6144L

outdir <- file.path("StreamCatR", "zonal_stats_r", "cat_rasters")
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

for (i in seq_along(cats)) {
  rid <- region_id[i]  # or region_id_num[i] if files are numeric-only
  out <- file.path(outdir, sprintf("%s_nonempty_windows_%s.parquet", rid, blocksize))
  
  message("Region: ", rid)
  if (file.exists(out)) {
    message("Already present: ", basename(out), " — skipping.")
    next
  }
  
  tic()
  prepare_optimized_catchments(
    catchment_path    = cats[i],
    region_id         = rid,
    blocksize         = blocksize,
    outdir            = outdir,
    target_crs        = "EPSG:5070",
    overwrite_rasters = TRUE,
    build_windows     = TRUE,   # this will create the windows file if missing
    progress_every    = 0L
  )
  toc()
}


# Check nonempty parquet output
library(arrow)

check_one <- function(rid) {
  p <- file.path(outdir, sprintf("%s_nonempty_windows_%s.parquet", rid, blocksize))
  if (!file.exists(p)) return(sprintf("%s: missing parquet", rid))
  df <- tryCatch(read_parquet(p, as_data_frame = TRUE), error = function(e) NULL)
  if (is.null(df)) return(sprintf("%s: parquet unreadable", rid))
  sprintf("%s: %d rows", rid, nrow(df))
}

sapply(region_id, check_one, USE.NAMES = TRUE)

#-------------------------------------------------------
# Optimize categorical raster (block size)
#-------------------------------------------------------
source("./StreamCatR/zonal_stats_r/02b.optimize_categorical_raster.R")

indir <- "./StreamCatR/zonal_stats_r/landscape_rasters/"
#blocksize = "512"
inras <- "Annual_NLCD_LndCov_2017_CU_C1V0.tif"

optimize_categorical_raster(
  infile        = file.path("./StreamCatR/zonal_stats_r/landscape_rasters", inras),
  outdir        = "./StreamCatR/zonal_stats_r/landscape_rasters",
  resample      = "near",
  datatype      = "INT1U",
  gdal_compress = "ZSTD",
  zstd_level    = 9,
  nodata_value  = 0,
  blocksize     = blocksize
)

# --------------------------------------------------------
# Run 1 region
# --------------------------------------------------------
tic()
res_one <- run_region_accumulation(
  region_id      = regions[1],
  zone_dir       = "./StreamCatR/zonal_stats_r/cat_rasters",
  predictor_path = "./StreamCatR/zonal_stats_r/landscape_rasters/Annual_NLCD_LndCov_2017_CU_C1V0_cat_6144.tif",
  blocksize      = 6144L,
  method         = "near",
  mode           = "categorical",
  nodata_value   = 0
)
toc()
head(res_one)

# --------------------------------------------------------
# Run regions in parallel
# --------------------------------------------------------
scripts_dir <- normalizePath("./StreamCatR/zonal_stats_r", mustWork = TRUE)
source(file.path(scripts_dir, "04c.zonal_functions_true_blocking_categorical.R"))
source(file.path(scripts_dir, "05b.run_region_accumulation.R"))
source(file.path(scripts_dir, "06b.run_regions_parallel.R"))

regions <- region_id

tic()
res_cat <- run_regions_parallel(
  regions        = regions,
  zone_dir       = "./StreamCatR/zonal_stats_r/cat_rasters",
  predictor_path = "./StreamCatR/zonal_stats_r/landscape_rasters/Annual_NLCD_LndCov_2017_CU_C1V0_cat_6144.tif",
  blocksize      = 6144L,
  method         = "near",
  stats          = "sum",
  zonal_mode     = "categorical",
  nodata_value   = 0,
  add_area       = FALSE,
  add_prop       = FALSE,
  plan_strategy  = "multisession",
  reserve_cores  = 1L,
  prefer_threads_per_worker = 1L,
  memfrac_total  = 0.98,
  zonal_file     = file.path(scripts_dir, "04c.zonal_functions_true_blocking_categorical.R"),
  r_libs_user    = "C:/Users/RHill04/AppData/Local/R/libraries",
  verbose        = TRUE
)
toc()

# res_cat is a list or combined data.table depending on combine=TRUE
# Columns: GRIDCODE, CLASS, COUNT, (PROP, Area_m2, Area_km2 if requested)

raster_path <- "./StreamCatR/zonal_stats_r/cat_rasters"

tmp <- list.files(path = raster_path,
                  pattern = '.tif')

for(i in 1:length(tmp)){
  r <- rast(file.path(raster_path, tmp[i]))
  print(r)
}


tmp <- list.files(path = raster_path,
                  pattern = "gridcode_index.parquet",
                  full.names = TRUE)

for(i in 1:length(tmp)){
  pq <- arrow::read_parquet(tmp[i])
  summary(pq) %>% print()
}
