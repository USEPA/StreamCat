# 04c.zonal_functions_true_blocking_categorical.R
library(terra)
library(data.table)

zonal_window_resample_categorical_idx_fast <- function(
    Pcat, Zidx, ids, windows,
    method = "near",
    nodata_value = NA_integer_,
    add_area     = FALSE,
    add_prop     = FALSE,
    progress_every = 0L
) {
  stopifnot(method == "near")
  
  if (is.character(windows) && length(windows) == 1L) {
    windows <- arrow::read_parquet(windows, as_data_frame = TRUE)
  }
  stopifnot(is.data.frame(windows))
  stopifnot(all(c("row", "col", "nrows", "ncols") %in% names(windows)))
  
  terra::readStart(Zidx)
  terra::readStart(Pcat)
  on.exit({
    suppressWarnings(terra::readStop(Zidx))
    suppressWarnings(terra::readStop(Pcat))
  }, add = TRUE)
  
  exZ  <- terra::ext(Zidx)
  resZ <- terra::res(Zidx); rx <- resZ[1]; ry <- resZ[2]
  eP   <- terra::ext(Pcat)
  resP <- terra::res(Pcat)
  padx <- resP[1] * 1L
  pady <- resP[2] * 1L
  
  cell_area_m2 <- abs(resP[1] * resP[2])
  if (!is.finite(cell_area_m2) || cell_area_m2 <= 0) {
    add_area <- FALSE
  }
  
  chunks <- vector("list", length = nrow(windows))
  ck <- 0L
  
  for (ii in seq_len(nrow(windows))) {
    r0 <- windows$row[ii]; nr <- windows$nrows[ii]
    c0 <- windows$col[ii]; nc <- windows$ncols[ii]
    
    idx <- terra::readValues(Zidx, row = r0, nrows = nr, col = c0, ncols = nc, mat = FALSE)
    if (!any(!is.na(idx))) next
    if (!is.integer(idx)) idx <- as.integer(idx)
    
    xmin <- exZ$xmin + (c0 - 1L) * rx
    xmax <- xmin + nc * rx
    ymax <- exZ$ymax - (r0 - 1L) * ry
    ymin <- ymax - nr * ry
    
    ewin_pad <- terra::ext(xmin - padx, xmax + padx, ymin - pady, ymax + pady)
    if (!terra::relate(ewin_pad, eP, "intersects")) next
    ewin2 <- terra::intersect(ewin_pad, eP)
    if (is.null(ewin2)) next
    
    Pcrop <- terra::crop(Pcat, ewin2)
    valsP <- terra::values(Pcrop, mat = FALSE)
    exP2  <- terra::ext(Pcrop)
    resP2 <- terra::res(Pcrop)
    ncP   <- terra::ncol(Pcrop)
    nrP   <- terra::nrow(Pcrop)
    
    xs <- seq(xmin + rx/2, xmax - rx/2, by = rx)
    ys <- seq(ymax - ry/2, ymin + ry/2, by = -ry)
    col_vec <- terra::colFromX(Pcrop, xs)
    row_vec <- terra::rowFromY(Pcrop, ys)
    rows_rep <- rep(row_vec, each = length(xs))
    cols_rep <- rep(col_vec, times = length(ys))
    cells <- (rows_rep - 1L) * ncP + cols_rep
    
    v <- rep(NA_integer_, length(cells))
    okc <- !is.na(cells)
    v[okc] <- valsP[cells[okc]]
    
    if (length(v) != length(idx)) {
      stop(sprintf("Length mismatch at window %d: idx=%d, v=%d", ii, length(idx), length(v)))
    }
    
    # Valid pairs; drop nodata category and any out-of-range idx
    ok <- !is.na(idx) & !is.na(v)
    if (!is.na(nodata_value)) ok <- ok & (v != as.integer(nodata_value))
    if (any(ok)) {
      idx_ok <- idx[ok]
      class_ok <- as.integer(v[ok])
      # guard: idx within [1, length(ids)]
      in_range <- idx_ok >= 1L & idx_ok <= length(ids)
      if (!all(in_range)) {
        bad <- sum(!in_range)
        warning(sprintf("Dropped %d out-of-range zone indices (window %d).", bad, ii))
        idx_ok   <- idx_ok[in_range]
        class_ok <- class_ok[in_range]
      }
      if (length(idx_ok)) {
        dt <- data.table::data.table(
          IDX   = idx_ok,          # zone index, not final GRIDCODE
          CLASS = class_ok
        )
        dtc <- dt[, .(COUNT = .N), by = .(IDX, CLASS)]
        ck <- ck + 1L
        chunks[[ck]] <- dtc
      }
    }
    
    if (progress_every > 0L && (ii %% progress_every == 0L)) {
      message(sprintf("  processed windows %d / %d", ii, nrow(windows)))
    }
  }
  
  if (ck == 0L) {
    # Return an empty histogram; caller can merge later
    return(data.table::data.table(GRIDCODE = integer(0), CLASS = integer(0), COUNT = integer(0)))
  }
  
  counts <- data.table::rbindlist(chunks[seq_len(ck)], use.names = TRUE)
  counts <- counts[, .(COUNT = sum(COUNT)), by = .(IDX, CLASS)]
  
  # Map IDX -> true GRIDCODE using ids vector (ids[IDX] is the COMID/GRIDCODE)
  counts[, GRIDCODE := ids[IDX]]
  counts[, IDX := NULL]  # drop index column
  
  if (isTRUE(add_prop) || isTRUE(add_area)) {
    totals <- counts[, .(N_zone = sum(COUNT)), by = GRIDCODE]
    counts <- counts[totals, on = "GRIDCODE"]
    if (isTRUE(add_prop)) {
      counts[, PROP := ifelse(N_zone > 0, COUNT / N_zone, NA_real_)]
    }
    if (isTRUE(add_area)) {
      counts[, `:=`(
        Area_m2  = COUNT * cell_area_m2,
        Area_km2 = (COUNT * cell_area_m2) / 1e6
      )]
    }
  }
  
  data.table::setkey(counts, GRIDCODE, CLASS)
  counts[]
}