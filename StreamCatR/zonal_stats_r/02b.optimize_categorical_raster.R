library(terra)

optimize_categorical_raster <- function(
    infile,
    outdir        = dirname(infile),
    outname       = NULL,
    blocksize     = 512L,
    target_crs    = "EPSG:5070",   # NLCD is typically in EPSG:5070; reprojection if needed
    resample      = "near",         # nearest neighbor for categorical data
    trim_edges    = TRUE,           # trim NA-only borders
    datatype      = "INT1U",        # unsigned 8-bit integers (0..255) suitable for NLCD classes
    overwrite     = TRUE,
    gdal_compress = "ZSTD",         # ZSTD (fast + small); LZW is also fine
    zstd_level    = 9,
    bigtiff       = TRUE,
    threads       = "ALL_CPUS",
    gdal_extra    = NULL,           # e.g., use c("PREDICTOR=2") if LZW/DEFLATE (ignored by ZSTD in some GDAL builds)
    nodata_value  = 0,              # NLCD commonly uses 0 as NoData; set NULL if no nodata
    target_res    = NULL,           # set numeric c(xres, yres) if you need a specific resolution in 5070
    majority_agg  = FALSE,          # if TRUE and changing resolution without reprojection, aggregate by modal class
    preserve_levels = TRUE,         # keep category metadata (labels, colortable) if present
    colortab      = NULL            # optional color table (list or matrix of RGBA for class IDs)
) {
  stopifnot(file.exists(infile))
  blocksize <- as.integer(blocksize)
  
  # Load
  R <- terra::rast(infile)
  
  # Declare nodata (so 0 is treated as NA without rewriting values)
  if (!is.null(nodata_value)) {
    try(terra::NAflag(R) <- nodata_value, silent = TRUE)
  }
  
  # Preserve levels/colortable if desired and present
  if (isTRUE(preserve_levels)) {
    lev <- try(terra::levels(R), silent = TRUE)
    ct  <- try(terra::coltab(R),  silent = TRUE)
  } else {
    lev <- ct <- NULL
  }
  
  # Decide operation: reprojection vs same-CRS resolution change
  same_crs <- !is.null(target_crs) && terra::same.crs(R, target_crs)
  need_res <- !is.null(target_res) && length(target_res) == 2
  
  if (!same_crs) {
    # Reproject to target CRS (categorical => nearest neighbor)
    R <- if (need_res) {
      terra::project(R, target_crs, method = resample, res = target_res)
    } else {
      terra::project(R, target_crs, method = resample)
    }
  } else if (need_res) {
    # Same CRS but change resolution
    if (isTRUE(majority_agg)) {
      # Use modal aggregation for categorical if making coarser resolution
      # Compute integer aggregation factor from current resolution
      r0 <- terra::res(R)
      fact <- round(target_res / r0)
      if (any(fact < 1)) stop("target_res must be >= current resolution for majority_agg.")
      # Aggregate by modal class per block; preserves categorical nature
      R <- terra::aggregate(R, fact = fact, fun = modal, expand = TRUE, na.rm = TRUE)
      # Ensure new resolution matches target_res exactly
      terra::res(R) <- target_res
    } else {
      # Use nearest neighbor resampling to the new resolution grid
      # Create an empty template with desired res aligned to original extent
      tmpl <- R
      terra::res(tmpl) <- target_res
      R <- terra::resample(R, tmpl, method = resample)
    }
  } else {
    # Identity: CRS already equivalent and resolution unchanged
    # Optionally set CRS label to target_crs without resampling
    try(suppressWarnings(terra::crs(R) <- target_crs), silent = TRUE)
  }
  
  # Re-apply preserved levels/colortable
  if (isTRUE(preserve_levels) && !inherits(lev, "try-error") && length(lev) > 0) {
    try(terra::levels(R) <- lev, silent = TRUE)
  }
  if (isTRUE(preserve_levels) && !inherits(ct, "try-error") && length(ct) > 0) {
    try(terra::coltab(R) <- ct, silent = TRUE)
  }
  if (!is.null(colortab)) {
    # If you provide a color table explicitly
    try(terra::coltab(R) <- colortab, silent = TRUE)
  }
  
  # Trim NA-only edges (skip gracefully if not applicable)
  if (isTRUE(trim_edges)) {
    R_trim <- try(terra::trim(R), silent = TRUE)
    if (!inherits(R_trim, "try-error")) {
      R <- R_trim
    } else {
      message("trim() skipped (no NA-only edges or trim not applicable).")
    }
  }
  
  # Output name and folder
  if (is.null(outname)) {
    base <- tools::file_path_sans_ext(basename(infile))
    outname <- sprintf("%s_cat_%d.tif", base, blocksize)
  }
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  outfile <- file.path(outdir, outname)
  
  # GDAL options
  gdal_opts <- c(
    "TILED=YES",
    paste0("COMPRESS=", toupper(gdal_compress)),
    if (toupper(gdal_compress) == "ZSTD") paste0("ZSTD_LEVEL=", zstd_level) else NULL,
    paste0("BIGTIFF=", if (bigtiff) "YES" else "IF_NEEDED"),
    paste0("NUM_THREADS=", threads),
    paste0("BLOCKXSIZE=", blocksize),
    paste0("BLOCKYSIZE=", blocksize),
    if (!is.null(nodata_value)) paste0("NODATA=", nodata_value) else NULL,
    # PREDICTOR is useful with LZW/DEFLATE; often ignored with ZSTD depending on GDAL build
    if (!is.null(gdal_extra)) gdal_extra
  )
  
  # Write (categorical => integer type)
  terra::writeRaster(
    R,
    filename  = outfile,
    overwrite = overwrite,
    datatype  = datatype,   # e.g., "INT1U" for NLCD
    gdal      = gdal_opts
  )
  
  invisible(outfile)
}