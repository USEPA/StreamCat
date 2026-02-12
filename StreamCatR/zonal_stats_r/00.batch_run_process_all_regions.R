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

# --------------------------------------------------------
# Run regions in series
# --------------------------------------------------------
source("./StreamCatR/zonal_stats_r/04.zonal_functions_true_blocking.R")
source("./StreamCatR/zonal_stats_r/05.run_region_accumulation.R")

# Thread options (optional)
Sys.setenv(GDAL_NUM_THREADS = "ALL_CPUS")
Sys.setenv(OMP_NUM_THREADS  = as.character(parallel::detectCores()))
if (!dir.exists("./terra")) dir.create("./terra", recursive = TRUE, showWarnings = FALSE)
terra::terraOptions(threads = parallel::detectCores(),
                    tempdir = "./StreamCatR/zonal_stats_r/terra",
                    memfrac = 0.9)

out <- list()

for(i in 1:length(region_id)){
  tictoc::tic()
  print(region_id[i])
  out[[i]] <- run_region_accumulation(
    region_id = region_id[i],
    zone_dir = "./StreamCatR/zonal_stats_r/cat_rasters", # folder with Zidx and parquet files
    predictor_path = "./StreamCatR/zonal_stats_r/landscape_rasters/bfi_512.tif", # full path to the predictor raster (any tiling/size)
    blocksize = 6144,                # e.g., 6144L (used only for Zidx/windows)
    method = "near",
    stats  = "sum",
    progress_every = 0L,
  )
  print(head(out[[i]]))
  tictoc::toc()
}
#head(dt2)

# --------------------------------------------------------
# Run regions in parallel
# --------------------------------------------------------
scripts_dir <- normalizePath("./StreamCatR/zonal_stats_r", mustWork = TRUE)
zonal_file  <- file.path(scripts_dir, "04.zonal_functions_true_blocking.R")

# Optional preflight
has_pkg <- requireNamespace("scaccum", quietly = TRUE)
has_fallback <- file.exists(file.path(scripts_dir, "03.rcpp_accum.R")) &&
  file.exists(file.path(scripts_dir, "cpp", "rcpp_accum.cpp"))
if (!has_pkg && !has_fallback) {
  stop("Neither scaccum nor fallback Rcpp sources are available.")
}

# Source definitions
source(file.path(scripts_dir, "04.zonal_functions_true_blocking.R"))
source(file.path(scripts_dir, "05.run_region_accumulation.R"))
source(file.path(scripts_dir, "06.run_regions_parallel.R"))

regions <- region_id

# CPU‑only limiting, many 1‑thread workers
tic()
res <- run_regions_parallel(
  regions        = regions,
  zone_dir       = "./StreamCatR/zonal_stats_r/cat_rasters",
  predictor_path = "./StreamCatR/zonal_stats_r/landscape_rasters/bfi_512.tif",
  blocksize      = 6144L,
  method         = "near",
  stats          = "sum",
  plan_strategy  = "multisession",
  reserve_cores  = 1L,                 # leave 1 core for the OS
  prefer_threads_per_worker = 1L,      # 1 thread/worker
  memfrac_total  = 0.98,                # allow more RAM in total
  approx_mem_per_worker_gb = NULL,     # disable RAM-based worker cap
  zonal_file     = zonal_file,
  r_libs_user    = "C:/Users/RHill04/AppData/Local/R/libraries",
  verbose = FALSE
)
toc()


