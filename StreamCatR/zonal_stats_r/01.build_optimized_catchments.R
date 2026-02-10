library(terra)
library(arrow)

# ------------- helper: unique GRIDCODE -> idx parquet (streamed) -------------
build_gridcode_index_parquet_stream <- function(Z, out_file, progress_every = 0L) {
  stopifnot(inherits(Z, "SpatRaster"))
  bs <- terra::blocks(Z)
  n_blocks <- if (!is.null(nrow(bs))) nrow(bs) else length(bs$row)
  if (is.null(n_blocks) || n_blocks <= 0L) stop("Unexpected blocks() structure")
  
  # Use an environment as a hash set to avoid repeated unions
  seen <- new.env(parent = emptyenv())
  
  terra::readStart(Z)
  on.exit(terra::readStop(Z), add = TRUE)
  
  for (i in seq_len(n_blocks)) {
    b_row   <- bs$row[i]
    b_nrows <- bs$nrows[i]
    z <- terra::readValues(Z, row = b_row, nrows = b_nrows, mat = FALSE)
    z <- z[!is.na(z)]
    if (!length(z)) next
    uz <- unique(z)
    for (v in uz) seen[[as.character(v)]] <- TRUE
    
    if (progress_every > 0L && (i %% progress_every == 0L)) {
      message(sprintf("  scanned block %d / %d", i, n_blocks))
    }
  }
  
  ids <- sort(as.integer(ls(seen)))
  dt  <- data.frame(GRIDCODE = ids, idx = seq_along(ids))
  arrow::write_parquet(dt, out_file, compression = "zstd")
  invisible(out_file)
}

# ------------- helper: build non-empty windows parquet -----------------------
# Scans Zidx in tiles of size win_nrows x win_ncols; records windows with any non-NA.
build_nonempty_windows <- function(Zidx,
                                   win_nrows = 512L,
                                   win_ncols = 512L,
                                   out_parquet = NULL,
                                   progress_every = 500L) {
  stopifnot(inherits(Zidx, "SpatRaster"))
  stopifnot(terra::nlyr(Zidx) == 1L)
  
  nr <- terra::nrow(Zidx)
  nc <- terra::ncol(Zidx)
  
  # window grid
  row_starts <- seq.int(1L, nr, by = win_nrows)
  col_starts <- seq.int(1L, nc, by = win_ncols)
  
  # all windows as (row, col, nrows, ncols)
  win <- data.table::CJ(row = row_starts, col = col_starts)
  win[, nrows := pmin(win_nrows, nr - row + 1L)]
  win[, ncols := pmin(win_ncols, nc - col + 1L)]
  win[, win_id := .I]
  
  keep <- logical(nrow(win))
  
  terra::readStart(Zidx)
  on.exit(terra::readStop(Zidx), add = TRUE)
  
  for (i in seq_len(nrow(win))) {
    v <- terra::readValues(
      Zidx,
      row = win$row[i], nrows = win$nrows[i],
      col = win$col[i], ncols = win$ncols[i],
      mat = FALSE
    )
    keep[i] <- any(!is.na(v))
    
    if (progress_every > 0L && (i %% progress_every == 0L)) {
      message(sprintf("  scanned windows %d / %d", i, nrow(win)))
    }
  }
  
  out <- win[keep]
  
  if (!is.null(out_parquet)) {
    arrow::write_parquet(out, out_parquet, compression = "zstd")
  }
  
  out
}

# ------------- main: prepare catchment artifacts -----------------------------
# prepare_optimized_catchments <- function(
#     catchment_path,       # path to NHDPlus catchment raster (e.g., ".../cat")
#     region_id,            # e.g., "Region18"
#     blocksize,            # e.g., 6144L
#     outdir,               # output folder
#     target_crs = "EPSG:5070",
#     overwrite_rasters = TRUE,
#     progress_every = 0L
# ) {
#   dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
#   blocksize <- as.integer(blocksize)
#   
#   # Output names (index parquet is generic to any block size)
#   cat_path        <- file.path(outdir, sprintf("%s_catchments_%d.tif", region_id, blocksize))
#   grid_index_path <- file.path(outdir, sprintf("%s_gridcode_index.parquet", region_id))
#   zidx_path       <- file.path(outdir, sprintf("%s_zone_index_block%d.tif", region_id, blocksize))
#   wins_path       <- file.path(outdir, sprintf("%s_nonempty_windows_%d.parquet", region_id, blocksize))
#   
#   # 1) Read + standardize catchments
#   cat <- terra::rast(catchment_path)
#   if (!terra::same.crs(cat, target_crs)) {
#     cat <- terra::project(cat, target_crs, method = "near")
#   }
#   cat <- terra::trim(cat)
#   
#   # 2) Write standardized catchments
#   terra::writeRaster(
#     cat, filename = cat_path, overwrite = overwrite_rasters, datatype = "INT4S",
#     gdal = c(
#       "TILED=YES", "COMPRESS=ZSTD", "ZSTD_LEVEL=9", "BIGTIFF=YES",
#       "NUM_THREADS=ALL_CPUS",
#       sprintf("BLOCKXSIZE=%d", blocksize),
#       sprintf("BLOCKYSIZE=%d", blocksize)
#     )
#   )
#   
#   # 3) Build GRIDCODE -> idx parquet once (skip if exists)
#   if (!file.exists(grid_index_path)) {
#     build_gridcode_index_parquet_stream(cat, grid_index_path, progress_every = progress_every)
#   } else {
#     message(sprintf("Index exists, skipping: %s", grid_index_path))
#   }
#   
#   # 4) Load mapping and create Zidx (raster of idx 1..K)
#   idx_tbl <- arrow::read_parquet(grid_index_path)
#   if (!all(c("GRIDCODE", "idx") %in% names(idx_tbl))) {
#     stop("gridcode index parquet is missing required columns: GRIDCODE, idx")
#   }
#   # exact-match mapping via classify: [id-0.5, id+0.5) -> idx
#   ids_map <- as.numeric(idx_tbl$GRIDCODE)
#   idx_map <- as.numeric(idx_tbl$idx)
#   rcl <- cbind(ids_map - 0.5, ids_map + 0.5, idx_map)
#   Zidx <- terra::classify(cat, rcl = rcl, right = FALSE, include.lowest = TRUE, others = NA)
#   
#   # 5) Write Zidx
#   terra::writeRaster(
#     Zidx, filename = zidx_path, overwrite = overwrite_rasters, datatype = "INT4S",
#     gdal = c(
#       "TILED=YES", "COMPRESS=ZSTD", "ZSTD_LEVEL=9", "BIGTIFF=YES",
#       "NUM_THREADS=ALL_CPUS",
#       sprintf("BLOCKXSIZE=%d", blocksize),
#       sprintf("BLOCKYSIZE=%d", blocksize)
#     )
#   )
#   
#   # 6) Build and cache non-empty windows for this block size
#   wins <- build_nonempty_windows(
#     Zidx, win_nrows = blocksize, win_ncols = blocksize,
#     out_parquet = wins_path, progress_every = progress_every
#   )
#   
#   invisible(list(
#     cat_path        = cat_path,
#     grid_index_path = grid_index_path,
#     zidx_path       = zidx_path,
#     wins_path       = wins_path
#   ))
# }


# --------------------------------
# Revised code

library(terra)
library(arrow)

# Assumes build_nonempty_windows() and build_gridcode_index_parquet_stream() are defined
# exactly as in your script (streamed scanning with readStart/readValues).

prepare_optimized_catchments <- function(
    catchment_path,        # path to NHDPlus catchment raster (e.g., ESRI GRID folder "…/cat")
    region_id,             # e.g., "Region18"
    blocksize,             # e.g., 6144L
    outdir,                # folder where the three outputs are written
    target_crs        = "EPSG:5070",
    overwrite_rasters = TRUE,
    build_windows     = TRUE,      # windows parquet is needed by run_region_accumulation
    progress_every    = 0L
) {
  dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
  blocksize <- as.integer(blocksize)
  
  # Outputs actually used by run_region_accumulation()
  grid_index_path <- file.path(outdir, sprintf("%s_gridcode_index.parquet",    region_id))
  zidx_path       <- file.path(outdir, sprintf("%s_zone_index_block%d.tif",    region_id, blocksize))
  wins_path       <- file.path(outdir, sprintf("%s_nonempty_windows_%d.parquet", region_id, blocksize))
  
  # Helper: remove target and common sidecars when overwriting
  safe_unlink <- function(path, do = TRUE) {
    if (!do) return(invisible(FALSE))
    sidecars <- c("", ".aux.xml", ".ovr", ".msk", ".msk.aux.xml")
    files <- file.path(dirname(path), paste0(basename(path), sidecars))
    suppressWarnings(file.remove(files[file.exists(files)]))
    invisible(TRUE)
  }
  
  # 1) Load source; reproject only if needed (avoid identity reprojection)
  cat_src <- terra::rast(catchment_path)
  if (!terra::same.crs(cat_src, target_crs)) {
    # Write to a temp file so downstream reads are fast and not memory-bound
    tmp_proj <- tempfile(pattern = "cat_proj_", fileext = ".tif")
    cat_src <- terra::project(cat_src, target_crs, method = "near",
                              filename = tmp_proj, overwrite = TRUE)
  }
  
  # 2) Build GRIDCODE -> idx index parquet (skip if exists unless overwrite)
  if (file.exists(grid_index_path) && !overwrite_rasters) {
    message("Index exists, skipping: ", grid_index_path)
  } else {
    if (file.exists(grid_index_path)) safe_unlink(grid_index_path, TRUE)
    build_gridcode_index_parquet_stream(cat_src, grid_index_path, progress_every = progress_every)
  }
  
  # 3) Build Zidx (1..K) with app + match, and write it
  ids_tbl <- arrow::read_parquet(grid_index_path, as_data_frame = TRUE)
  if (!"GRIDCODE" %in% names(ids_tbl)) stop("Index parquet lacks GRIDCODE column: ", grid_index_path)
  ids <- ids_tbl$GRIDCODE
  
  Zidx <- terra::app(cat_src, fun = function(x) {
    out <- match(x, ids)
    out[is.na(x)] <- NA_integer_
    as.integer(out)
  })
  
  if (file.exists(zidx_path) && !overwrite_rasters) {
    message("Zidx exists, skipping: ", zidx_path)
  } else {
    if (file.exists(zidx_path)) safe_unlink(zidx_path, TRUE)
    terra::writeRaster(
      Zidx, zidx_path, overwrite = TRUE, datatype = "INT4S",
      gdal = c(
        "TILED=YES", "COMPRESS=ZSTD", "ZSTD_LEVEL=9",
        "BIGTIFF=YES", "NUM_THREADS=ALL_CPUS",
        sprintf("BLOCKXSIZE=%d", blocksize),
        sprintf("BLOCKYSIZE=%d", blocksize)
      )
    )
  }
  
  # 4) Non-empty windows parquet (skip if exists unless overwrite)
  if (isTRUE(build_windows)) {
    if (file.exists(wins_path) && !overwrite_rasters) {
      message("Windows parquet exists, skipping: ", wins_path)
    } else {
      if (file.exists(wins_path)) safe_unlink(wins_path, TRUE)
      # Read Zidx from disk to match downstream usage
      Zidx_disk <- terra::rast(zidx_path)
      build_nonempty_windows(
        Zidx_disk, win_nrows = blocksize, win_ncols = blocksize,
        out_parquet = wins_path, progress_every = progress_every
      )
    }
  }
  
  invisible(list(
    grid_index_path = grid_index_path,
    zidx_path       = zidx_path,
    wins_path       = if (build_windows) wins_path else NULL
  ))
}
