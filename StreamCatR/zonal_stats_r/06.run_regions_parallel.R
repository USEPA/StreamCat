# 06.run_regions_parallel.R
# Definitions only. No side effects when sourcing.

auto_parallel_layout <- function(n_tasks,
                                 reserve_cores = 2L,
                                 prefer_threads_per_worker = 2L,
                                 max_workers = NULL,
                                 memfrac_total = 0.8,
                                 approx_mem_per_worker_gb = NULL) {
  total_cores  <- parallel::detectCores(logical = TRUE)
  usable_cores <- max(1L, total_cores - as.integer(reserve_cores))
  
  threads_per_worker <- max(1L, as.integer(prefer_threads_per_worker))
  workers_by_cpu <- floor(usable_cores / threads_per_worker)
  if (!is.null(max_workers)) workers_by_cpu <- min(workers_by_cpu, as.integer(max_workers))
  workers_by_cpu <- max(1L, workers_by_cpu)
  
  # Optional RAM cap (guarded; works even if ps doesn't export virtual_memory)
  workers_by_ram <- Inf
  available_gb <- NA_real_
  
  if (requireNamespace("ps", quietly = TRUE)) {
    ex <- getNamespaceExports("ps")
    mem_fun <- NULL
    if ("ps_system_memory" %in% ex) {
      mem_fun <- ps::ps_system_memory
    } else if ("virtual_memory" %in% ex) {
      mem_fun <- getExportedValue("ps", "virtual_memory")
    }
    if (!is.null(mem_fun)) {
      vm <- tryCatch(mem_fun(), error = function(e) NULL)
      if (is.list(vm)) {
        bytes <- vm$available
        if (is.null(bytes)) bytes <- vm$free
        if (is.numeric(bytes) && is.finite(bytes)) {
          available_gb <- as.numeric(bytes) / 1024^3
        }
      }
    }
  }
  
  if (!is.null(approx_mem_per_worker_gb) && is.finite(available_gb)) {
    max_workers_from_ram <- floor((available_gb * memfrac_total) / approx_mem_per_worker_gb)
    workers_by_ram <- max(1L, max_workers_from_ram)
  }
  
  workers <- min(n_tasks, workers_by_cpu, workers_by_ram)
  workers <- max(1L, as.integer(workers))
  
  terra_memfrac <- max(0.05, min(0.9, memfrac_total / workers))
  list(
    workers = workers,
    threads_per_worker = as.integer(threads_per_worker),
    gdal_threads  = as.integer(threads_per_worker),
    omp_threads   = as.integer(threads_per_worker),
    terra_threads = as.integer(threads_per_worker),
    terra_memfrac = terra_memfrac
  )
}

run_regions_parallel <- function(regions,
                                 zone_dir,
                                 predictor_path,
                                 blocksize = 6144L,
                                 method = "near",
                                 stats = "sum",
                                 plan_strategy = c("multisession", "multicore", "cluster"),
                                 reserve_cores = 2L,
                                 prefer_threads_per_worker = 2L,
                                 max_workers = NULL,
                                 memfrac_total = 0.8,
                                 approx_mem_per_worker_gb = NULL,
                                 out_terra_temp_base = file.path(tempdir(), "terra_parallel"),
                                 zonal_file = file.path("StreamCatR","zonal_stats_r","04.zonal_functions_true_blocking.R"),
                                 r_libs_user = NULL,  # optional: e.g., "C:/Users/ME/AppData/Local/R/libraries"
                                 combine = TRUE,
                                 verbose = TRUE) {
  stopifnot(length(regions) > 0)
  plan_strategy <- match.arg(plan_strategy)
  
  # Ensure future/future.apply available
  if (!requireNamespace("future", quietly = TRUE) ||
      !requireNamespace("future.apply", quietly = TRUE)) {
    stop("Please install.packages(c('future','future.apply'))")
  }
  
  # Resolve zonal file path now to fail fast
  zf <- tryCatch(normalizePath(zonal_file, mustWork = TRUE),
                 error = function(e) stop("zonal_file not found: ", zonal_file))
  
  layout <- auto_parallel_layout(
    n_tasks = length(regions),
    reserve_cores = reserve_cores,
    prefer_threads_per_worker = prefer_threads_per_worker,
    max_workers = max_workers,
    memfrac_total = memfrac_total,
    approx_mem_per_worker_gb = approx_mem_per_worker_gb
  )
  if (verbose) {
    message(sprintf("Parallel layout: workers=%d, threads/worker=%d, terra_memfrac=%.3f",
                    layout$workers, layout$threads_per_worker, layout$terra_memfrac))
  }
  
  # Set future plan
  if (identical(plan_strategy, "multicore") && !future::supportsMulticore()) {
    warning("multicore not supported; falling back to multisession")
    plan_strategy <- "multisession"
  }
  if (identical(plan_strategy, "multisession")) {
    future::plan(future::multisession, workers = layout$workers)
  } else if (identical(plan_strategy, "multicore")) {
    future::plan(future::multicore, workers = layout$workers)
  } else {
    future::plan(future::cluster, workers = layout$workers)
  }
  on.exit(future::plan(future::sequential), add = TRUE)
  
  # Temp dirs for workers
  dir.create(out_terra_temp_base, recursive = TRUE, showWarnings = FALSE)
  
  # Optional: pre-check required window/parquet files
  win_path <- function(rid) file.path(zone_dir, sprintf("%s_nonempty_windows_%s.parquet", rid, blocksize))
  has_win <- vapply(regions, function(r) file.exists(win_path(r)), logical(1))
  if (!all(has_win)) {
    warning("Skipping regions without windows/parquet: ", paste(regions[!has_win], collapse = ", "))
    regions <- regions[has_win]
  }
  if (!length(regions)) stop("No regions to process after pre-checks.")
  
  # res_list <- future.apply::future_lapply(
  #   regions,
  #   FUN = function(rid) {
  #     # Ensure workers use your preferred library (optional)
  #     if (!is.null(r_libs_user)) .libPaths(c(r_libs_user, .libPaths()))
  #     
  #     # Per-worker thread/env settings
  #     Sys.setenv(GDAL_NUM_THREADS = as.character(layout$gdal_threads),
  #                OMP_NUM_THREADS  = as.character(layout$omp_threads))
  #     wd <- file.path(out_terra_temp_base, paste0("worker_", Sys.getpid()))
  #     dir.create(wd, recursive = TRUE, showWarnings = FALSE)
  #     terra::terraOptions(threads = layout$terra_threads,
  #                         memfrac = layout$terra_memfrac,
  #                         tempdir = wd)
  #     
  #     # Load required packages (don’t hard-require scaccum; 04 handles fallback)
  #     suppressPackageStartupMessages({
  #       library(terra); library(arrow); library(data.table)
  #       if (!requireNamespace("collapse", quietly = TRUE)) {
  #         stop("Need collapse. install.packages('collapse')")
  #       }
  #     })
  #     # Optional: attach scaccum if present (init in 04 will also handle it)
  #     if (requireNamespace("scaccum", quietly = TRUE)) {
  #       # library(scaccum) # not strictly necessary if 04 binds the symbol
  #     }
  #     
  #     # Source zonal functions; this should init acc_sum_n_idx1K (package or fallback)
  #     sys.source(zf, envir = environment())
  #     
  #     if (isTRUE(verbose)) message(sprintf("[Worker %d] %s", Sys.getpid(), rid))
  #     
  #     out <- run_region_accumulation(
  #       region_id       = rid,
  #       zone_dir        = zone_dir,
  #       predictor_path  = predictor_path,
  #       blocksize       = blocksize,
  #       method          = method,
  #       stats           = stats,
  #       progress_every  = 0L
  #     )
  #     list(region = rid, result = out)
  #   },
  #   future.packages = c("terra","arrow","data.table")  # core deps only
  # )
  res_list <- future.apply::future_lapply(
    regions,
    FUN = function(rid) {
      if (!is.null(r_libs_user)) .libPaths(c(r_libs_user, .libPaths()))
      
      Sys.setenv(GDAL_NUM_THREADS = as.character(layout$gdal_threads),
                 OMP_NUM_THREADS  = as.character(layout$omp_threads))
      Sys.setenv(OPENBLAS_NUM_THREADS   = as.character(layout$threads_per_worker),
                 MKL_NUM_THREADS        = as.character(layout$threads_per_worker),
                 BLIS_NUM_THREADS       = as.character(layout$threads_per_worker),
                 VECLIB_MAXIMUM_THREADS = as.character(layout$threads_per_worker))
      Sys.setenv(GDAL_CACHEMAX = "2048")
      
      wd <- file.path(out_terra_temp_base, paste0("worker_", Sys.getpid()))
      dir.create(wd, recursive = TRUE, showWarnings = FALSE)
      terra::terraOptions(threads = layout$terra_threads,
                          memfrac = layout$terra_memfrac,
                          tempdir = wd)
      
      suppressPackageStartupMessages({
        library(terra); library(arrow); library(data.table)
        # Prefer to load scaccum; init_accumulator() will then find the fast path
        if (requireNamespace("scaccum", quietly = TRUE)) {
          # library(scaccum) # optional
        }
      })
      
      # Source zonal functions; this calls init_accumulator() internally
      sys.source(zonal_file, envir = environment())
      
      out <- run_region_accumulation(
        region_id       = rid,
        zone_dir        = zone_dir,
        predictor_path  = predictor_path,
        blocksize       = blocksize,
        method          = method,
        stats           = stats,
        progress_every  = 0L
      )
      list(region = rid, result = out)
    },
    future.packages = c("terra", "arrow", "data.table", "scaccum")
  )
  
  names(res_list) <- vapply(res_list, `[[`, "", "region")
  
  if (!combine) return(res_list)
  
  all_df <- all(vapply(res_list, function(x) inherits(x$result, "data.frame"), logical(1)))
  if (all_df && requireNamespace("data.table", quietly = TRUE)) {
    return(data.table::rbindlist(lapply(res_list, `[[`, "result"), fill = TRUE, use.names = TRUE))
  } else if (all_df) {
    out <- do.call(rbind, lapply(res_list, `[[`, "result"))
    rownames(out) <- NULL
    return(out)
  } else {
    return(res_list)
  }
}