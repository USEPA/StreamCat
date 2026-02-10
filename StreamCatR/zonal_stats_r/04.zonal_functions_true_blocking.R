library(terra)
library(arrow)
library(data.table)

source("./StreamCatR/zonal_stats_r/03.rcpp_accum.R")

zonal_window_resample_accum_idx_fast2 <- function(P800, Zidx, ids, windows,
                                                  method = "near",
                                                  stats = c("sum","mean"),
                                                  progress_every = 0L) {
  if (!requireNamespace("collapse", quietly = TRUE)) {
    stop("Need collapse. install.packages('collapse')")
  }
  
  # windows can be a parquet path or a data.frame
  if (is.character(windows) && length(windows) == 1L) {
    windows <- arrow::read_parquet(windows, as_data_frame = TRUE)
  }
  windows <- data.table::as.data.table(windows)
  stopifnot(all(c("row","col","nrows","ncols") %in% names(windows)))
  
  # IO-friendly order
  data.table::setorder(windows, row, col)
  
  stats <- unique(stats)
  want_sum  <- "sum"  %in% stats
  want_mean <- "mean" %in% stats
  
  K <- length(ids)
  sumv <- numeric(K)
  n    <- integer(K)
  
  # Open rasters for readValues speed (simple fast path)
  terra::readStart(Zidx)
  terra::readStart(P800)
  on.exit({
    suppressWarnings(terra::readStop(Zidx))
    suppressWarnings(terra::readStop(P800))
  }, add = TRUE)
  
  t_total <- proc.time()[3]
  t_idx <- 0; t_resamp <- 0; t_agg <- 0; t_upd <- 0
  
  # Cache geometry ONCE
  exZ  <- terra::ext(Zidx)
  resZ <- terra::res(Zidx)
  rx   <- resZ[1]
  ry   <- resZ[2]
  crsZ <- terra::crs(Zidx)
  
  eP   <- terra::ext(P800)
  Pxmin <- eP$xmin; Pxmax <- eP$xmax; Pymin <- eP$ymin; Pymax <- eP$ymax
  
  # ---- Precompute window extents (numeric) ----
  # Avoids ext()/relate()/intersect() overhead inside loop
  windows[, `:=`(
    xmin = exZ$xmin + (col - 1L) * rx,
    xmax = exZ$xmin + (col - 1L) * rx + ncols * rx,
    ymax = exZ$ymax - (row - 1L) * ry,
    ymin = exZ$ymax - (row - 1L) * ry - nrows * ry
  )]
  
  # Precompute overlap with predictor extent
  # overlap if rectangles intersect in both x and y
  windows[, overlap := (xmax > Pxmin) & (xmin < Pxmax) & (ymax > Pymin) & (ymin < Pymax)]
  
  # If nothing overlaps, return zeros quickly
  if (!any(windows$overlap)) {
    out <- data.table::data.table(GRIDCODE = ids, n = 0L)
    if (want_sum)  out[, sum  := 0]
    if (want_mean) out[, mean := NA_real_]
    return(out[])
  }
  
  win_i <- which(windows$overlap)
  
  for (jj in seq_along(win_i)) {
    ii <- win_i[jj]
    
    r0 <- windows$row[ii];   nr <- windows$nrows[ii]
    c0 <- windows$col[ii];   nc <- windows$ncols[ii]
    
    # read idx window
    t0 <- proc.time()[3]
    idx <- terra::readValues(Zidx, row = r0, nrows = nr, col = c0, ncols = nc, mat = FALSE)
    t_idx <- t_idx + (proc.time()[3] - t0)
    
    ok_idx <- !is.na(idx)
    if (!any(ok_idx)) next
    
    # window extent (already computed)
    xmin <- windows$xmin[ii]; xmax <- windows$xmax[ii]
    ymin <- windows$ymin[ii]; ymax <- windows$ymax[ii]
    
    # Build full window template (30m grid)
    ewin <- terra::ext(xmin, xmax, ymin, ymax)
    Zwin <- terra::rast(ewin, resolution = resZ, crs = crsZ)
    
    # Compute intersection extent numerically (no terra::intersect)
    ixmin <- if (xmin > Pxmin) xmin else Pxmin
    ixmax <- if (xmax < Pxmax) xmax else Pxmax
    iymin <- if (ymin > Pymin) ymin else Pymin
    iymax <- if (ymax < Pymax) ymax else Pymax
    
    if (ixmin >= ixmax || iymin >= iymax) next
    ewin2 <- terra::ext(ixmin, ixmax, iymin, iymax)
    
    # crop + resample
    t0 <- proc.time()[3]
    Pcrop <- terra::crop(P800, ewin2)
    Pwin  <- terra::resample(Pcrop, Zwin, method = method)
    v     <- terra::values(Pwin, mat = FALSE)
    t_resamp <- t_resamp + (proc.time()[3] - t0)
    
    # single-pass filter mask
    keep <- ok_idx & !is.na(v)
    if (!any(keep)) next
    
    v_ok   <- as.numeric(v[keep])
    idx_ok <- as.integer(idx[keep])
    
    # aggregate
    t0 <- proc.time()[3]
    s   <- collapse::fsum(v_ok, idx_ok)
    cts <- collapse::qtab(idx_ok)
    t_agg <- t_agg + (proc.time()[3] - t0)
    
    # update
    t0 <- proc.time()[3]
    gi_s <- as.integer(names(s))
    sumv[gi_s] <- sumv[gi_s] + as.numeric(s)
    
    gi_n <- as.integer(names(cts))
    n[gi_n] <- n[gi_n] + as.integer(cts)
    t_upd <- t_upd + (proc.time()[3] - t0)
    
    if (progress_every > 0L && (jj %% progress_every == 0L)) {
      message(sprintf("  processed overlapping windows %d / %d", jj, length(win_i)))
    }
  }
  
  out <- data.table::data.table(GRIDCODE = ids, n = n)
  if (want_sum)  out[, sum  := sumv]
  if (want_mean) out[, mean := sumv / pmax(n, 1L)]
  
  elapsed <- proc.time()[3] - t_total
  message(sprintf("← zonal_window_resample_accum_idx_fast2(): total %.2f sec", elapsed))
  message(sprintf("    read idx(): %.2f sec", t_idx))
  message(sprintf("    window resample(): %.2f sec", t_resamp))
  message(sprintf("    agg(): %.2f sec", t_agg))
  message(sprintf("    update(): %.2f sec", t_upd))
  
  out[]
}



# ------------------------------------------------------------------------------
# Two-pass / pre-filtered variant of your fast2 window-resample accumulator.
# Pass 1: precompute each window's extent and keep only those intersecting P800.
# Pass 2: process only kept windows.
#
# Assumes:
# - Zidx is a SpatRaster (30 m zone-index raster)
# - P800 is a SpatRaster (predictor, e.g. PRISM 800 m)
# - windows has cols: row, col, nrows, ncols (and optionally others)
# - ids length K, with idx values in Zidx ranging 1..K
# ------------------------------------------------------------------------------

zonal_window_resample_accum_idx_fast2_twopass <- function(
    P800, Zidx, ids, windows,
    method = "near",
    stats = c("sum", "mean"),
    progress_every = 0L,
    prefilter_progress_every = 0L,   # separate progress for pass 1
    keep_extents = FALSE             # if TRUE, store computed extents back onto 'windows'
) {
  if (!requireNamespace("collapse", quietly = TRUE)) {
    stop("Need collapse. install.packages('collapse')")
  }
  
  # windows can be a parquet path or a data.frame
  if (is.character(windows) && length(windows) == 1L) {
    windows <- arrow::read_parquet(windows, as_data_frame = TRUE)
  }
  stopifnot(is.data.frame(windows))
  stopifnot(all(c("row","col","nrows","ncols") %in% names(windows)))
  
  stats <- unique(stats)
  want_sum  <- "sum"  %in% stats
  want_mean <- "mean" %in% stats
  
  K <- length(ids)
  sumv <- numeric(K)
  n    <- integer(K)
  
  # ---- open rasters (fast path; suppress the readStop warning like you did) ----
  terra::readStart(Zidx)
  terra::readStart(P800)
  on.exit({
    suppressWarnings(terra::readStop(Zidx))
    suppressWarnings(terra::readStop(P800))
  }, add = TRUE)
  
  t_total <- proc.time()[3]
  t_pref  <- 0
  t_idx   <- 0
  t_res   <- 0
  t_agg   <- 0
  t_upd   <- 0
  
  # ---- cache geometry ONCE ----
  exZ  <- terra::ext(Zidx)
  resZ <- terra::res(Zidx)
  rx   <- resZ[1]
  ry   <- resZ[2]
  crsZ <- terra::crs(Zidx)
  eP   <- terra::ext(P800)
  
  nrw <- nrow(windows)
  
  # ----------------------------------------------------------------------------
  # PASS 1: prefilter windows that intersect predictor extent
  # ----------------------------------------------------------------------------
  t0 <- proc.time()[3]
  
  # Compute extents as numeric columns (fast) to avoid repeatedly calling ext()
  # These are the same formulas you've been using.
  xmin <- exZ$xmin + (windows$col - 1L) * rx
  xmax <- xmin + windows$ncols * rx
  ymax <- exZ$ymax - (windows$row - 1L) * ry
  ymin <- ymax - windows$nrows * ry
  
  keep <- logical(nrw)
  
  # Keep the overlap test cheap: rectangle intersection with P800 extent (numeric)
  pxmin <- eP$xmin; pxmax <- eP$xmax; pymin <- eP$ymin; pymax <- eP$ymax
  keep <- (xmax > pxmin) & (xmin < pxmax) & (ymax > pymin) & (ymin < pymax)
  
  t_pref <- t_pref + (proc.time()[3] - t0)
  
  if (!any(keep)) {
    warning("No windows overlap predictor extent; returning all-zero output.")
    out <- data.table::data.table(GRIDCODE = ids, n = 0L)
    if (want_sum)  out[, sum := 0]
    if (want_mean) out[, mean := NA_real_]
    return(out[])
  }
  
  win_i <- which(keep)
  
  # Optionally keep computed extents for debugging / reuse
  if (isTRUE(keep_extents)) {
    windows$xmin <- xmin
    windows$xmax <- xmax
    windows$ymin <- ymin
    windows$ymax <- ymax
  }
  
  if (prefilter_progress_every > 0L) {
    message(sprintf("prefilter kept %d / %d windows (%.1f%%)",
                    length(win_i), nrw, 100 * length(win_i) / nrw))
  }
  
  # ----------------------------------------------------------------------------
  # PASS 2: process only kept windows
  # ----------------------------------------------------------------------------
  for (jj in seq_along(win_i)) {
    ii <- win_i[jj]
    
    r0 <- windows$row[ii];   nr <- windows$nrows[ii]
    c0 <- windows$col[ii];   nc <- windows$ncols[ii]
    
    # read idx window
    t0 <- proc.time()[3]
    idx <- terra::readValues(Zidx, row = r0, nrows = nr, col = c0, ncols = nc, mat = FALSE)
    t_idx <- t_idx + (proc.time()[3] - t0)
    
    ok_idx <- !is.na(idx)
    if (!any(ok_idx)) next
    
    # build ewin quickly using precomputed numeric extents (no repeated math)
    ewin <- terra::ext(xmin[ii], xmax[ii], ymin[ii], ymax[ii])
    
    # template raster for this window (use cached res/crs)
    Zwin <- terra::rast(ewin, resolution = resZ, crs = crsZ)
    
    # crop + resample
    t0 <- proc.time()[3]
    # intersect guard (handles edge weirdness; cheap)
    ewin2 <- terra::intersect(ewin, eP)
    if (is.null(ewin2)) next
    
    Pcrop <- terra::crop(P800, ewin2)
    Pwin  <- terra::resample(Pcrop, Zwin, method = method)
    v     <- terra::values(Pwin, mat = FALSE)
    t_res <- t_res + (proc.time()[3] - t0)
    
    # filter (two-stage like your fast2 work)
    v_ok   <- v[ok_idx]
    idx_ok <- idx[ok_idx]
    
    ok2 <- !is.na(v_ok)
    if (!any(ok2)) next
    
    v_ok   <- as.numeric(v_ok[ok2])
    idx_ok <- as.integer(idx_ok[ok2])
    
    # aggregate (fsum + qtab has been the sweet spot)
    t0 <- proc.time()[3]
    s   <- collapse::fsum(v_ok, idx_ok)
    cts <- collapse::qtab(idx_ok)
    t_agg <- t_agg + (proc.time()[3] - t0)
    
    # update global
    t0 <- proc.time()[3]
    gi_s <- as.integer(names(s))
    sumv[gi_s] <- sumv[gi_s] + as.numeric(s)
    
    gi_n <- as.integer(names(cts))
    n[gi_n] <- n[gi_n] + as.integer(cts)
    t_upd <- t_upd + (proc.time()[3] - t0)
    
    if (progress_every > 0L && (jj %% progress_every == 0L)) {
      message(sprintf("  processed kept windows %d / %d", jj, length(win_i)))
    }
  }
  
  out <- data.table::data.table(GRIDCODE = ids, n = n)
  if (want_sum)  out[, sum  := sumv]
  if (want_mean) out[, mean := sumv / pmax(n, 1L)]
  
  elapsed <- proc.time()[3] - t_total
  message(sprintf("← zonal_window_resample_accum_idx_fast2_twopass(): total %.2f sec", elapsed))
  message(sprintf("    prefilter():       %.2f sec", t_pref))
  message(sprintf("    read idx():         %.2f sec", t_idx))
  message(sprintf("    window resample():  %.2f sec", t_res))
  message(sprintf("    agg():              %.2f sec", t_agg))
  message(sprintf("    update():           %.2f sec", t_upd))
  
  out[]
}

zonal_window_resample_accum_idx_fast2_rcpp <- function(
    P800, Zidx, ids, windows,
    method = "near",
    stats = c("sum", "mean"),
    progress_every = 0L) 
{
  stopifnot(exists("acc_sum_n_idx1K", mode = "function"))
  
  # windows can be a parquet path or a data.frame/data.table
  if (is.character(windows) && length(windows) == 1L) {
    windows <- arrow::read_parquet(windows, as_data_frame = TRUE)
  }
  stopifnot(is.data.frame(windows))
  stopifnot(all(c("row", "col", "nrows", "ncols") %in% names(windows)))
  
  stats <- unique(stats)
  want_sum  <- "sum"  %in% stats
  want_mean <- "mean" %in% stats
  
  K <- length(ids)
  sumv <- numeric(K)   # double
  n    <- integer(K)   # int
  
  # --- open rasters (fast path) ---
  terra::readStart(Zidx)
  terra::readStart(P800)
  on.exit({
    suppressWarnings(terra::readStop(Zidx))
    suppressWarnings(terra::readStop(P800))
  }, add = TRUE)
  
  # --- timing ---
  t_total <- proc.time()[3]
  t_idx <- 0; t_resamp <- 0; t_acc <- 0
  
  # --- cache geometry ONCE ---
  exZ  <- terra::ext(Zidx)
  resZ <- terra::res(Zidx)
  rx   <- resZ[1]
  ry   <- resZ[2]
  crsZ <- terra::crs(Zidx)
  eP   <- terra::ext(P800)
  
  for (ii in seq_len(nrow(windows))) {
    r0 <- windows$row[ii];   nr <- windows$nrows[ii]
    c0 <- windows$col[ii];   nc <- windows$ncols[ii]
    
    # ---- read idx window ----
    t0 <- proc.time()[3]
    idx <- terra::readValues(Zidx, row = r0, nrows = nr, col = c0, ncols = nc, mat = FALSE)
    t_idx <- t_idx + (proc.time()[3] - t0)
    
    # quick skip if all NA
    if (!any(!is.na(idx))) next
    
    # IMPORTANT: kernel expects IntegerVector
    # For Int32 rasters, terra often returns integer already; if not, coerce:
    if (!is.integer(idx)) idx <- as.integer(idx)
    
    # ---- window extent from cached geometry ----
    xmin <- exZ$xmin + (c0 - 1L) * rx
    xmax <- xmin + nc * rx
    ymax <- exZ$ymax - (r0 - 1L) * ry
    ymin <- ymax - nr * ry
    ewin <- terra::ext(xmin, xmax, ymin, ymax)
    
    # skip if no overlap with predictor
    if (!terra::relate(ewin, eP, "intersects")) next
    
    # template raster for this window (30 m grid)
    Zwin <- terra::rast(ewin, resolution = resZ, crs = crsZ)
    
    # ---- crop + resample predictor for just this window ----
    t0 <- proc.time()[3]
    ewin2 <- terra::intersect(ewin, eP)
    if (is.null(ewin2)) next
    
    Pcrop <- terra::crop(P800, ewin2)
    Pwin  <- terra::resample(Pcrop, Zwin, method = method)
    v     <- terra::values(Pwin, mat = FALSE)   # numeric vector
    t_resamp <- t_resamp + (proc.time()[3] - t0)
    
    # ---- C++ accumulation (NO v_ok / idx_ok allocations) ----
    t0 <- proc.time()[3]
    acc_sum_n_idx1K(idx, v, sumv, n)
    t_acc <- t_acc + (proc.time()[3] - t0)
    
    if (progress_every > 0L && (ii %% progress_every == 0L)) {
      message(sprintf("  processed windows %d / %d", ii, nrow(windows)))
    }
  }
  
  out <- data.table::data.table(GRIDCODE = ids, n = n)
  if (want_sum)  out[, sum  := sumv]
  if (want_mean) out[, mean := sumv / pmax(n, 1L)]
  
  elapsed <- proc.time()[3] - t_total
  message(sprintf("← zonal_window_resample_accum_idx_fast2_rcpp(): total %.2f sec", elapsed))
  message(sprintf("    read idx():        %.2f sec", t_idx))
  message(sprintf("    window resample(): %.2f sec", t_resamp))
  message(sprintf("    C++ accumulate():  %.2f sec", t_acc))
  
  out[]
}


zonal_window_resample_accum_idx_fast2_rcpp2 <- function(
    P800, Zidx, ids, windows,
    method = "near",
    stats = c("sum", "mean"),
    progress_every = 0L) 
{
  stopifnot(exists("acc_sum_n_idx1K", mode = "function"))
  
  # windows can be a parquet path or a data.frame/data.table
  if (is.character(windows) && length(windows) == 1L) {
    windows <- arrow::read_parquet(windows, as_data_frame = TRUE)
  }
  stopifnot(is.data.frame(windows))
  stopifnot(all(c("row", "col", "nrows", "ncols") %in% names(windows)))
  
  stats <- unique(stats)
  want_sum  <- "sum"  %in% stats
  want_mean <- "mean" %in% stats
  
  K <- length(ids)
  sumv <- numeric(K)   # double
  n    <- integer(K)   # int
  
  # --- open rasters (fast path) ---
  terra::readStart(Zidx)
  terra::readStart(P800)
  on.exit({
    suppressWarnings(terra::readStop(Zidx))
    suppressWarnings(terra::readStop(P800))
  }, add = TRUE)
  
  # --- timing ---
  t_total <- proc.time()[3]
  t_idx <- 0; t_resamp <- 0; t_acc <- 0
  
  # --- cache geometry ONCE ---
  exZ  <- terra::ext(Zidx)
  resZ <- terra::res(Zidx)
  rx   <- resZ[1]
  ry   <- resZ[2]
  crsZ <- terra::crs(Zidx)
  eP   <- terra::ext(P800)
  
  Zwin <- terra::rast(Zidx)
  
  for (ii in seq_len(nrow(windows))) {
    r0 <- windows$row[ii];   nr <- windows$nrows[ii]
    c0 <- windows$col[ii];   nc <- windows$ncols[ii]
    
    # ---- read idx window ----
    t0 <- proc.time()[3]
    idx <- terra::readValues(Zidx, row = r0, nrows = nr, col = c0, ncols = nc, mat = FALSE)
    t_idx <- t_idx + (proc.time()[3] - t0)
    
    # quick skip if all NA
    if (!any(!is.na(idx))) next
    
    # IMPORTANT: kernel expects IntegerVector
    # For Int32 rasters, terra often returns integer already; if not, coerce:
    if (!is.integer(idx)) idx <- as.integer(idx)
    
    # ---- window extent from cached geometry ----
    xmin <- exZ$xmin + (c0 - 1L) * rx
    xmax <- xmin + nc * rx
    ymax <- exZ$ymax - (r0 - 1L) * ry
    ymin <- ymax - nr * ry
    ewin <- terra::ext(xmin, xmax, ymin, ymax)
    
    # skip if no overlap with predictor
    if (!terra::relate(ewin, eP, "intersects")) next
    
    # template raster for this window (30 m grid)
    #Zwin <- terra::rast(ewin, resolution = resZ, crs = crsZ)
    Zwin <- terra::rast(nrows = nr, ncols = nc, ext = ewin, crs = crsZ)
    terra::res(Zwin) <- resZ
    
    # ---- crop + resample predictor for just this window ----
    t0 <- proc.time()[3]
    ewin2 <- terra::intersect(ewin, eP)
    if (is.null(ewin2)) next
    
    Pcrop <- terra::crop(P800, ewin2)
    Pwin  <- terra::resample(Pcrop, Zwin, method = method)
    v     <- terra::values(Pwin, mat = FALSE)   # numeric vector
    t_resamp <- t_resamp + (proc.time()[3] - t0)
    
    # ---- C++ accumulation (NO v_ok / idx_ok allocations) ----
    t0 <- proc.time()[3]
    acc_sum_n_idx1K(idx, v, sumv, n)
    t_acc <- t_acc + (proc.time()[3] - t0)
    
    if (progress_every > 0L && (ii %% progress_every == 0L)) {
      message(sprintf("  processed windows %d / %d", ii, nrow(windows)))
    }
  }
  
  out <- data.table::data.table(GRIDCODE = ids, n = n)
  if (want_sum)  out[, sum  := sumv]
  if (want_mean) out[, mean := sumv / pmax(n, 1L)]
  
  elapsed <- proc.time()[3] - t_total
  message(sprintf("← zonal_window_resample_accum_idx_fast2_rcpp(): total %.2f sec", elapsed))
  message(sprintf("    read idx():        %.2f sec", t_idx))
  message(sprintf("    window resample(): %.2f sec", t_resamp))
  message(sprintf("    C++ accumulate():  %.2f sec", t_acc))
  
  out[]
}


# Fix window padding to no lose predictor pixels ------------------

zonal_window_resample_accum_idx_fast2_rcpp3 <- function(
    P800, Zidx, ids, windows,
    method = "near",
    stats = c("sum", "mean"),
    progress_every = 0L
) {
  stopifnot(exists("acc_sum_n_idx1K", mode = "function"))
  
  # windows can be a parquet path or a data.frame/data.table
  if (is.character(windows) && length(windows) == 1L) {
    windows <- arrow::read_parquet(windows, as_data_frame = TRUE)
  }
  stopifnot(is.data.frame(windows))
  stopifnot(all(c("row", "col", "nrows", "ncols") %in% names(windows)))
  
  stats <- unique(stats)
  want_sum  <- "sum"  %in% stats
  want_mean <- "mean" %in% stats
  
  K    <- length(ids)
  sumv <- numeric(K)
  n    <- integer(K)
  
  # --- open rasters (fast path) ---
  terra::readStart(Zidx)
  terra::readStart(P800)
  on.exit({
    suppressWarnings(terra::readStop(Zidx))
    suppressWarnings(terra::readStop(P800))
  }, add = TRUE)
  
  # --- timing ---
  t_total <- proc.time()[3]
  t_idx <- 0; t_resamp <- 0; t_acc <- 0
  
  # --- cache geometry ONCE ---
  exZ  <- terra::ext(Zidx)
  resZ <- terra::res(Zidx); rx <- resZ[1]; ry <- resZ[2]
  crsZ <- terra::crs(Zidx)
  eP   <- terra::ext(P800)
  
  # padding based on predictor resolution and interpolation kernel
  resP <- terra::res(P800)
  pad_cells <- 1L
  padx <- resP[1] * pad_cells
  pady <- resP[2] * pad_cells
  
  for (ii in seq_len(nrow(windows))) {
    r0 <- windows$row[ii]; nr <- windows$nrows[ii]
    c0 <- windows$col[ii]; nc <- windows$ncols[ii]
    
    # ---- read idx window ----
    t0 <- proc.time()[3]
    idx <- terra::readValues(Zidx, row = r0, nrows = nr, col = c0, ncols = nc, mat = FALSE)
    t_idx <- t_idx + (proc.time()[3] - t0)
    
    # quick skip if all NA
    if (!any(!is.na(idx))) next
    
    # ensure integer labels for C++
    if (!is.integer(idx)) idx <- as.integer(idx)
    
    # ---- window extent from cached geometry ----
    xmin <- exZ$xmin + (c0 - 1L) * rx
    xmax <- xmin + nc * rx
    ymax <- exZ$ymax - (r0 - 1L) * ry
    ymin <- ymax - nr * ry
    ewin <- terra::ext(xmin, xmax, ymin, ymax)
    
    # ---- pad by >= one source cell so resample has neighbors at edges ----
    ewin_pad <- terra::ext(xmin - padx, xmax + padx, ymin - pady, ymax + pady)
    
    # skip if no overlap with predictor
    if (!terra::relate(ewin_pad, eP, "intersects")) next
    
    # template grid for this 30 m window (exact window; padding only for crop)
    Zwin <- terra::rast(ext = ewin, resolution = resZ, crs = crsZ)
    
    # ---- crop + resample predictor for this padded window ----
    t0 <- proc.time()[3]
    ewin2 <- terra::intersect(ewin_pad, eP)
    if (is.null(ewin2)) next
    
    Pcrop <- terra::crop(P800, ewin2)
    Pwin  <- terra::resample(Pcrop, Zwin, method = method)
    v     <- terra::values(Pwin, mat = FALSE)
    t_resamp <- t_resamp + (proc.time()[3] - t0)
    
    # safety check: vectors must align
    if (length(v) != length(idx)) {
      stop(sprintf("Length mismatch at window %d: idx=%d, v=%d", ii, length(idx), length(v)))
    }
    
    # ---- C++ accumulation ----
    t0 <- proc.time()[3]
    acc_sum_n_idx1K(idx, v, sumv, n)
    t_acc <- t_acc + (proc.time()[3] - t0)
    
    if (progress_every > 0L && (ii %% progress_every == 0L)) {
      message(sprintf("  processed windows %d / %d", ii, nrow(windows)))
    }
  }
  
  out <- data.table::data.table(GRIDCODE = ids, n = n)
  if (want_sum)  out[, sum  := sumv]
  if (want_mean) out[, mean := sumv / pmax(n, 1L)]
  
  elapsed <- proc.time()[3] - t_total
  message(sprintf("← zonal_window_resample_accum_idx_fast2_rcpp(): total %.2f sec", elapsed))
  message(sprintf("    read idx():        %.2f sec", t_idx))
  message(sprintf("    window resample(): %.2f sec", t_resamp))
  message(sprintf("    C++ accumulate():  %.2f sec", t_acc))
  
  out[]
}
