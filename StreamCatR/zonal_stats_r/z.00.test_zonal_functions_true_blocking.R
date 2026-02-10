library(arrow)
library(data.table)
library(tictoc)
library(terra)

Sys.setenv(GDAL_NUM_THREADS = "ALL_CPUS")
Sys.setenv(OMP_NUM_THREADS  = as.character(parallel::detectCores()))
terra::terraOptions(threads = parallel::detectCores(), 
                    tempdir = "./terra",
                    memfrac = 0.9)

Zidx <- rast("./cat_rasters/Region17_zone_index_block6144.tif")
ids  <- read_parquet("./cat_rasters/Region17_gridcode_index.parquet", as_data_frame = TRUE)$GRIDCODE

# -----------------------------------------------------------
# ------- BFI test ------------------------------------------
# -----------------------------------------------------------
indir <- "./landscape_rasters/"
blocksize = "6144"
inras <- "bfi"

R <- rast(paste0("./landscape_rasters/", inras, "_", blocksize, ".tif"))


wins_dt <- arrow::read_parquet("./cat_rasters/Region17_nonempty_windows_6144.parquet",
                               as_data_frame = TRUE)

dt2 <- zonal_window_resample_accum_idx_fast2_rcpp3(
  P800 = R,
  Zidx = Zidx,
  ids  = ids,
  windows = wins_dt,
  method = "near",
  stats = "sum",
  progress_every = 0L
)

