# 05b.run_region_accumulation.R

library(terra)
library(arrow)
library(data.table)

run_region_accumulation <- function(
    region_id,
    zone_dir,
    predictor_path,
    blocksize,
    method = "near",
    stats  = "sum",
    zonal_mode = c("continuous", "categorical"),  # renamed from 'mode'
    progress_every = 0L,
    project_predictor_if_needed = FALSE,
    nodata_value = NA_integer_,
    add_area     = FALSE,   # counts-only default
    add_prop     = FALSE    # counts-only default
) {
  zonal_mode <- match.arg(zonal_mode)
  blocksize <- as.integer(blocksize)
  
  # Paths
  zidx_path <- file.path(zone_dir, sprintf("%s_zone_index_block%d.tif", region_id, blocksize))
  ids_path  <- file.path(zone_dir, sprintf("%s_gridcode_index.parquet", region_id))
  wins_path <- file.path(zone_dir, sprintf("%s_nonempty_windows_%d.parquet", region_id, blocksize))
  
  # Validate inputs
  if (!file.exists(zidx_path)) stop("Missing Zidx raster: ", zidx_path)
  if (!file.exists(ids_path))  stop("Missing ID index parquet: ", ids_path)
  if (!file.exists(wins_path)) stop("Missing nonempty windows parquet: ", wins_path)
  if (!file.exists(predictor_path)) stop("Missing predictor raster: ", predictor_path)
  
  # Load artifacts needed for both branches
  Zidx <- terra::rast(zidx_path)
  
  ids_tbl <- arrow::read_parquet(ids_path, as_data_frame = TRUE)
  if (!"GRIDCODE" %in% names(ids_tbl)) stop("Index parquet lacks GRIDCODE column: ", ids_path)
  ids <- if ("idx" %in% names(ids_tbl)) ids_tbl$GRIDCODE[order(ids_tbl$idx)] else ids_tbl$GRIDCODE
  
  wins_dt <- arrow::read_parquet(wins_path, as_data_frame = TRUE)
  
  # Predictor (project if needed)
  R <- terra::rast(predictor_path)
  if (!terra::same.crs(R, Zidx)) {
    if (!project_predictor_if_needed) {
      stop("CRS mismatch between predictor and Zidx. Provide predictor in same CRS or set project_predictor_if_needed = TRUE.")
    } else {
      message("Projecting predictor to match Zidx CRS (one-time)…")
      tmp_pred <- tempfile(fileext = ".tif")
      R <- terra::project(R, Zidx, method = "near", filename = tmp_pred, overwrite = TRUE)
    }
  }
  
  # Branch: continuous vs categorical
  if (identical(zonal_mode, "continuous")) {
    out <- zonal_window_resample_accum_idx_fast2_rcpp3(
      P800           = R,
      Zidx           = Zidx,
      ids            = ids,
      windows        = wins_dt,
      method         = method,
      stats          = stats,
      progress_every = progress_every
    )
  } else {
    out <- zonal_window_resample_categorical_idx_fast(
      Pcat           = R,
      Zidx           = Zidx,
      ids            = ids,
      windows        = wins_dt,
      method         = "near",
      nodata_value   = nodata_value,
      add_area       = add_area,
      add_prop       = add_prop,
      progress_every = progress_every
    )
  }
  
  # Provenance
  attr(out, "region_id")      <- region_id
  attr(out, "predictor_path") <- predictor_path
  attr(out, "blocksize")      <- blocksize
  attr(out, "paths")          <- list(
    zidx = zidx_path, ids = ids_path, wins = wins_path, predictor = predictor_path
  )
  
  out
}