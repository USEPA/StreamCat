library(terra)
library(sf)
library(tidyverse)
library(tictoc)

Sys.setenv(GDAL_NUM_THREADS = "ALL_CPUS")
Sys.setenv(OMP_NUM_THREADS  = as.character(parallel::detectCores()))
terraOptions(threads = parallel::detectCores(), memfrac = 0.9)

tic()
# Standardize catchment raster
catchment_path   <- "C:/Users/RHill04/WorkFolder/GIS/NHDPlusV21/NHDPlusGB/NHDPlus16/NHDPlusCatchment/cat"
cat <-
  terra::rast(catchment_path) %>%
  terra::project(crs("EPSG:5070"), method = "near")

writeRaster(
  cat,
  filename = "./cat_rasters/Region16_catchments.tif",
  overwrite = TRUE,
  datatype = "INT4S",  # or INT8U/INT4U/INT8S depending on your IDs
  gdal = c(
    "TILED=YES",
    "COMPRESS=ZSTD",      # if supported; otherwise use LZW or DEFLATE
    "ZSTD_LEVEL=9",       # optional
    "BIGTIFF=YES",
    "NUM_THREADS=ALL_CPUS",
    "BLOCKXSIZE=6144",
    "BLOCKYSIZE=6144"
  )
)

cat <- 
  terra::rast('./cat_rasters/Region18_catchments.tif') %>% 
  terra::trim()

writeRaster(
  cat,
  filename = "./cat_rasters/Region18_catchments_block6144.tif",
  overwrite = TRUE,
  datatype = "INT4S",  # or INT8U/INT4U/INT8S depending on your IDs
  gdal = c(
    "TILED=YES",
    "COMPRESS=ZSTD",      # if supported; otherwise use LZW or DEFLATE
    "ZSTD_LEVEL=9",       # optional
    "BIGTIFF=YES",
    "NUM_THREADS=ALL_CPUS",
    "BLOCKXSIZE=6144",
    "BLOCKYSIZE=6144"
  )
)

library(terra)
library(data.table)
library(arrow)

build_gridcode_index_parquet_stream <- 
  function(Z, out_file, progress_every = 0L) {
    stopifnot(inherits(Z, "SpatRaster"))
    bs <- terra::blocks(Z)
    n_blocks <- if (!is.null(nrow(bs))) nrow(bs) else length(bs$row)
    if (is.null(n_blocks) || n_blocks <= 0L) stop("Unexpected blocks() structure")
    
    # Use an environment as a hash set to avoid repeated union() costs
    seen <- new.env(parent = emptyenv())
    
    terra::readStart(Z)
    on.exit(terra::readStop(Z), add = TRUE)
    
    for (i in seq_len(n_blocks)) {
      b_row   <- bs$row[i]
      b_nrows <- bs$nrows[i]
      
      z <- terra::readValues(Z, row = b_row, nrows = b_nrows, mat = FALSE)
      z <- z[!is.na(z)]
      if (!length(z)) next
      
      # Add uniques from this block into the hash set
      uz <- unique(z)
      for (v in uz) {
        seen[[as.character(v)]] <- TRUE
      }
      
      if (progress_every > 0L && (i %% progress_every == 0L)) {
        message(sprintf("  scanned block %d / %d", i, n_blocks))
      }
    }
    
    ids <- as.integer(ls(seen))
    ids <- sort(ids)
    
    dt <- data.table(GRIDCODE = ids, idx = seq_along(ids))
    arrow::write_parquet(dt, out_file, compression = "zstd")
    invisible(out_file)
  }

build_gridcode_index_parquet_stream(rast("./cat_rasters/Region18_catchments_block6144.tif"),
                                    "./cat_rasters/Region18_gridcode_index.parquet")

ids <- arrow::read_parquet("./cat_rasters/Region18_gridcode_index.parquet",
                           as_data_frame = TRUE)$GRIDCODE

zone_index_path <- "./cat_rasters/Region18_zone_index_block6144.tif"

# Build Zidx: each cell = index (1..K) into ids
Zidx <- terra::app(cat, fun = function(x) {
  out <- match(x, ids)
  out[is.na(x)] <- NA_integer_
  out
})

writeRaster(
  Zidx, zone_index_path, overwrite = TRUE, datatype = "INT4S",
  gdal = c("TILED=YES","COMPRESS=ZSTD","ZSTD_LEVEL=9",
           "BIGTIFF=YES","BLOCKXSIZE=6144","BLOCKYSIZE=6144")
)
toc()
# zone_index_path <- "./cat_rasters/Region17_zone_index_block6144.tif"
# writeRaster(
#   Zidx, zone_index_path, overwrite = TRUE, datatype = "INT4S",
#   gdal = c("TILED=YES","COMPRESS=ZSTD","ZSTD_LEVEL=9",
#            "BIGTIFF=YES","BLOCKXSIZE=6144","BLOCKYSIZE=6144")
# )


# Standardize prism raster

prism <-
  terra::rast('./landscape_rasters/prism_tmean_us_30s_202506.tif') %>%
  terra::project(crs(cat), res = 800, method = "bilinear")

writeRaster(
  prism,
  filename = "./landscape_rasters/prism_tmean.tif",
  overwrite = TRUE,
  datatype = "INT4S",  # or INT8U/INT4U/INT8S depending on your IDs
  gdal = c(
    "TILED=YES",
    "COMPRESS=ZSTD",      # if supported; otherwise use LZW or DEFLATE
    "ZSTD_LEVEL=9",       # optional
    "BIGTIFF=YES",
    "NUM_THREADS=ALL_CPUS",
    "BLOCKXSIZE=512",
    "BLOCKYSIZE=512"
  )
)

prism <-
  terra::rast("./landscape_rasters/prism_tmean.tif") %>% 
  trim()

writeRaster(
  prism,
  filename = "./landscape_rasters/prism_tmean_block6144b.tif",
  overwrite = TRUE,
  datatype = "INT4S",  # or INT8U/INT4U/INT8S depending on your IDs
  gdal = c(
    "TILED=YES",
    "COMPRESS=ZSTD",      # if supported; otherwise use LZW or DEFLATE
    "ZSTD_LEVEL=9",       # optional
    "BIGTIFF=YES",
    "NUM_THREADS=ALL_CPUS",
    "BLOCKXSIZE=6144",
    "BLOCKYSIZE=6144"
  )
)

# Standardize nlcd raster

nlcd <- 
  terra::rast('./landscape_rasters/Annual_NLCD_LndCov_2021_CU_C1V1.tif') 

nlcd_5070 <- project(
  nlcd,
  cat,                      # <-- template raster: matches extent/res/origin
  method   = "near",
  filename = "nlcd_5070.tif",
  overwrite = TRUE
)


writeRaster(
  nlcd_5070,
  filename = "./landscape_rasters/nlcd.tif",
  overwrite = TRUE,
  datatype = "INT4S",  # or INT8U/INT4U/INT8S depending on your IDs
  gdal = c(
    "TILED=YES",
    "COMPRESS=ZSTD",      # if supported; otherwise use LZW or DEFLATE
    "ZSTD_LEVEL=9",       # optional
    "BIGTIFF=YES",
    "NUM_THREADS=ALL_CPUS",
    "BLOCKXSIZE=512",
    "BLOCKYSIZE=512"
  )
)

writeRaster(
  nlcd_5070,
  filename = "./landscape_rasters/nlcd.mrf",
  overwrite = TRUE,
  filetype = "MRF",
  datatype = "INT4S",
  gdal = c(
    "COMPRESS=ZSTD",     # MRF supports several compressions depending on build
    "BLOCKSIZE=512"
  )
)

nlcd <-
  terra::rast("./landscape_rasters/nlcd.tif")

cat_list <- c("catchments_block10124_optimized.tif",
              "catchments_optimized.tif",
              "catchments.cog.tif",
              "catchments.mrf",
              "catchments_block1024.mrf")

force_scan <- function(path) {
  r <- rast(path)
  # This forces GDAL to read all blocks; keep result small so RAM isn't the bottleneck
  global(r, fun = "sum", na.rm = TRUE)
}

for(i in 1:length(cat_list)){
  print(cat_list[i])
  tic()
  x <- force_scan(paste0("./cat_rasters/", cat_list[i]))
  toc()
  print(x)
}


writeRaster(
  cat,
  filename = "./cat_rasters/catchments_optimized.tif",
  overwrite = TRUE,
  datatype = "INT4S",  # or INT8U/INT4U/INT8S depending on your IDs
  gdal = c(
    "TILED=YES",
    "COMPRESS=ZSTD",      # if supported; otherwise use LZW or DEFLATE
    "ZSTD_LEVEL=9",       # optional
    "BIGTIFF=YES",
    "NUM_THREADS=ALL_CPUS",
    "BLOCKXSIZE=1024",
    "BLOCKYSIZE=1024"
  )
)

writeRaster(
  cat,
  filename = "./cat_rasters/catchments.cog.tif",
  overwrite = TRUE,
  filetype = "COG",
  datatype = "INT4S",
  gdal = c(
    "COMPRESS=ZSTD",      # or LZW/DEFLATE
    "LEVEL=9",            # for DEFLATE; ZSTD uses ZSTD_LEVEL
    "BIGTIFF=YES",
    "NUM_THREADS=ALL_CPUS"
  )
)

writeRaster(
  cat,
  filename = "./cat_rasters/catchments_block1024.mrf",
  overwrite = TRUE,
  filetype = "MRF",
  datatype = "INT4S",
  gdal = c(
    "COMPRESS=ZSTD",     # MRF supports several compressions depending on build
    "BLOCKSIZE=512"
  )
)


