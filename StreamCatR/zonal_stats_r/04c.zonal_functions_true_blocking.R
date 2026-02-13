# 04.zonal_functions_true_blocking.R

library(terra)
library(arrow)
library(data.table)

# Optional: collapse is used below; fail early if missing
if (!requireNamespace("collapse", quietly = TRUE)) {
  stop("Need collapse. install.packages('collapse')")
}

# Path helpers (use absolute paths to be robust in workers)
.zr_dir <- normalizePath("./StreamCatR/zonal_stats_r", mustWork = FALSE)
.rcpp_file <- file.path(.zr_dir, "cpp", "rcpp_accum.cpp")    # adjust if different
.rcpp_script <- file.path(.zr_dir, "03b.rcpp_accum.R")        # adjust if different

init_accumulator <- function() {
  if (requireNamespace("scaccum", quietly = TRUE)) {
    if (is.function(scaccum::acc_sum_n_idx1K)) {
      acc_sum_n_idx1K <<- scaccum::acc_sum_n_idx1K
    }
    if (isTRUE("acc_sum_n_map_centers1K" %in% getNamespaceExports("scaccum"))) {
      acc_sum_n_map_centers1K <<- scaccum::acc_sum_n_map_centers1K
      return(invisible("scaccum"))
    }
    # fall through to compile if map-centers isn’t in the package yet
  }
  if (file.exists(.rcpp_script)) {
    # Source helper into the global environment
    sys.source(.rcpp_script, envir = .GlobalEnv)
    if (exists("ensure_rcpp_accum", envir = .GlobalEnv, mode = "function")) {
      # Compile C++ into the global environment
      ensure_rcpp_accum(.rcpp_file, env = .GlobalEnv)
    }
  }
  if (!exists("acc_sum_n_idx1K", mode = "function", inherits = TRUE)) {
    stop("acc_sum_n_idx1K is not available. Install scaccum or ensure 03b.rcpp_accum.R and its .cpp are present.")
  }
  invisible("fallback")
}

# Call once when this file is sourced
init_accumulator()

# Fix window padding to no lose predictor pixels ------------------

zonal_window_resample_accum_idx_fast2_rcpp3 <- function(
    P800, Zidx, ids, windows,
    method = "near",
    stats = c("sum", "mean"),
    progress_every = 0L
) {
  # Must have accumulator; mapper is optional (we fall back if missing)
  stopifnot(exists("acc_sum_n_idx1K", mode = "function", inherits = TRUE))
  use_cpp_map <- identical(method, "near") &&
    exists("acc_sum_n_map_centers1K", mode = "function", inherits = TRUE)
  
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
    
    # ---- pad by >= one source cell so resample has neighbors at edges ----
    ewin_pad <- terra::ext(xmin - padx, xmax + padx, ymin - pady, ymax + pady)
    
    # skip if no overlap with predictor
    if (!terra::relate(ewin_pad, eP, "intersects")) next
    
    # ---- crop + read predictor for this padded window ----
    t0 <- proc.time()[3]
    ewin2 <- terra::intersect(ewin_pad, eP)
    if (is.null(ewin2)) next
    
    # Crop predictor to window+pad
    Pcrop <- terra::crop(P800, ewin2)
    
    # Read Pcrop values and its geometry
    valsP <- terra::values(Pcrop, mat = FALSE)   # row-major
    exP2  <- terra::ext(Pcrop)
    resP2 <- terra::res(Pcrop)
    ncP   <- terra::ncol(Pcrop)
    nrP   <- terra::nrow(Pcrop)
    t_resamp <- t_resamp + (proc.time()[3] - t0)
    
    # ---- C++ mapping + accumulation (near/contains) or fallback ----
    t0 <- proc.time()[3]
    if (use_cpp_map) {
      acc_sum_n_map_centers1K(
        idx,
        xmin, ymax, rx, ry, nc, nr,
        valsP,
        exP2$xmin, exP2$ymax, resP2[1], resP2[2], ncP, nrP,
        sumv, n
      )
    } else {
      # R fallback mapping you validated
      xs <- seq(xmin + rx/2, xmax - rx/2, by = rx)
      ys <- seq(ymax - ry/2, ymin + ry/2, by = -ry)
      col_vec <- terra::colFromX(Pcrop, xs)
      row_vec <- terra::rowFromY(Pcrop, ys)
      rows_rep <- rep(row_vec, each = length(xs))
      cols_rep <- rep(col_vec, times = length(ys))
      cells <- (rows_rep - 1L) * ncP + cols_rep
      v <- rep(NA_real_, length(cells))
      okc <- !is.na(cells)
      v[okc] <- valsP[cells[okc]]
      
      if (length(v) != length(idx)) {
        stop(sprintf("Length mismatch at window %d: idx=%d, v=%d", ii, length(idx), length(v)))
      }
      acc_sum_n_idx1K(idx, v, sumv, n)
    }
    t_acc <- t_acc + (proc.time()[3] - t0)
    
    if (progress_every > 0L && (ii %% progress_every == 0L)) {
      message(sprintf("  processed windows %d / %d", ii, nrow(windows)))
    }
  }
  
  out <- data.table::data.table(GRIDCODE = ids, n = n)
  if (want_sum)  out[, sum  := sumv]
  if (want_mean) out[, mean := sumv / pmax(n, 1L)]
  
  elapsed <- proc.time()[3] - t_total
  #message(sprintf("← zonal_window_resample_accum_idx_fast2_rcpp(): total %.2f sec", elapsed))
  #message(sprintf("    read idx():        %.2f sec", t_idx))
  #message(sprintf("    window resample(): %.2f sec", t_resamp))
  #message(sprintf("    C++ accumulate():  %.2f sec", t_acc))
  
  out[]
}
