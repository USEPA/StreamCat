library(terra)
library(arrow)
library(data.table)

run_region_accumulation <- function(
    region_id,
    zone_dir,                 # folder with Zidx and parquet files
    predictor_path,           # full path to the predictor raster (any tiling/size)
    blocksize,                # e.g., 6144L (used only for Zidx/windows)
    method = "near",
    stats  = "sum",
    progress_every = 0L,
    project_predictor_if_needed = FALSE  # set TRUE to auto-project to Zidx CRS
) {
  blocksize <- as.integer(blocksize)
  
  # Construct expected zone artifact paths
  zidx_path <- file.path(zone_dir, sprintf("%s_zone_index_block%d.tif", region_id, blocksize))
  ids_path  <- file.path(zone_dir, sprintf("%s_gridcode_index.parquet", region_id))
  wins_path <- file.path(zone_dir, sprintf("%s_nonempty_windows_%d.parquet", region_id, blocksize))
  
  # Validate inputs
  if (!file.exists(zidx_path)) stop("Missing Zidx raster: ", zidx_path)
  if (!file.exists(ids_path))  stop("Missing ID index parquet: ", ids_path)
  if (!file.exists(wins_path)) stop("Missing nonempty windows parquet: ", wins_path)
  if (!file.exists(predictor_path)) stop("Missing predictor raster: ", predictor_path)
  
  # Load artifacts
  Zidx <- terra::rast(zidx_path)
  
  ids_tbl <- arrow::read_parquet(ids_path, as_data_frame = TRUE)
  if (!"GRIDCODE" %in% names(ids_tbl)) stop("Index parquet lacks GRIDCODE column: ", ids_path)
  ids <- if ("idx" %in% names(ids_tbl)) ids_tbl$GRIDCODE[order(ids_tbl$idx)] else ids_tbl$GRIDCODE
  
  wins_dt <- arrow::read_parquet(wins_path, as_data_frame = TRUE)
  
  # Predictor
  R <- terra::rast(predictor_path)
  
  # Optional: project predictor to zone CRS once if needed
  if (!terra::same.crs(R, Zidx)) {
    if (!project_predictor_if_needed) {
      stop(
        "CRS mismatch between predictor and Zidx. ",
        "Either supply a predictor in the same CRS as Zidx (recommended), ",
        "or set project_predictor_if_needed = TRUE to project on the fly."
      )
    } else {
      message("Projecting predictor to match Zidx CRS (one-time)…")
      # Use a temporary file to avoid memory-only cost for large rasters
      tmp_pred <- tempfile(fileext = ".tif")
      R <- terra::project(R, Zidx, method = method, filename = tmp_pred, overwrite = TRUE)
    }
  }
  
  # Run accumulation
  out <- zonal_window_resample_accum_idx_fast2_rcpp3(
    P800           = R,
    Zidx           = Zidx,
    ids            = ids,
    windows        = wins_dt,
    method         = method,
    stats          = stats,
    progress_every = progress_every
  )
  
  # Provenance
  attr(out, "region_id")      <- region_id
  attr(out, "predictor_path") <- predictor_path
  attr(out, "blocksize")      <- blocksize
  attr(out, "paths")          <- list(
    zidx = zidx_path, ids = ids_path, wins = wins_path, predictor = predictor_path
  )
  
  out
}