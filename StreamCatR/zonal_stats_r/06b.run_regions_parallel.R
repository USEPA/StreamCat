# 06b.run_regions_parallel.R

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
  
  workers <- max(1L, as.integer(min(n_tasks, workers_by_cpu, workers_by_ram)))
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

# run_regions_parallel <- function(regions,
#                                  zone_dir,
#                                  predictor_path,
#                                  blocksize = 6144L,
#                                  method = "near",
#                                  stats = "sum",
#                                  zonal_mode = c("continuous", "categorical"),  # renamed
#                                  nodata_value = NA_integer_,
#                                  add_area = FALSE,    # counts-only default
#                                  add_prop = FALSE,    # counts-only default
#                                  plan_strategy = c("multisession", "multicore", "cluster"),
#                                  reserve_cores = 2L,
#                                  prefer_threads_per_worker = 2L,
#                                  max_workers = NULL,
#                                  memfrac_total = 0.8,
#                                  approx_mem_per_worker_gb = NULL,
#                                  out_terra_temp_base = file.path(tempdir(), "terra_parallel"),
#                                  zonal_file = file.path("StreamCatR","zonal_stats_r","04c.zonal_functions_true_blocking_categorical.R"),
#                                  r_libs_user = NULL,
#                                  combine = TRUE,
#                                  verbose = TRUE) {
#   stopifnot(length(regions) > 0)
#   zonal_mode <- match.arg(zonal_mode)
#   plan_strategy <- match.arg(plan_strategy)
#   
#   if (!requireNamespace("future", quietly = TRUE) ||
#       !requireNamespace("future.apply", quietly = TRUE)) {
#     stop("Please install.packages(c('future','future.apply'))")
#   }
#   
#   zf <- tryCatch(normalizePath(zonal_file, mustWork = TRUE),
#                  error = function(e) stop("zonal_file not found: ", zonal_file))
#   
#   par_layout <- auto_parallel_layout(
#     n_tasks = length(regions),
#     reserve_cores = reserve_cores,
#     prefer_threads_per_worker = prefer_threads_per_worker,
#     max_workers = max_workers,
#     memfrac_total = memfrac_total,
#     approx_mem_per_worker_gb = approx_mem_per_worker_gb
#   )
#   if (verbose) {
#     message(sprintf("Parallel layout: workers=%d, threads/worker=%d, terra_memfrac=%.3f",
#                     par_layout$workers, par_layout$threads_per_worker, par_layout$terra_memfrac))
#   }
#   
#   if (identical(plan_strategy, "multicore") && !future::supportsMulticore()) {
#     warning("multicore not supported; falling back to multisession")
#     plan_strategy <- "multisession"
#   }
#   if (identical(plan_strategy, "multisession")) {
#     future::plan(future::multisession, workers = par_layout$workers)
#   } else if (identical(plan_strategy, "multicore")) {
#     future::plan(future::multicore, workers = par_layout$workers)
#   } else {
#     future::plan(future::cluster, workers = par_layout$workers)
#   }
#   on.exit(future::plan(future::sequential), add = TRUE)
#   
#   dir.create(out_terra_temp_base, recursive = TRUE, showWarnings = FALSE)
#   
#   res_list <- future.apply::future_lapply(
#     regions,
#     FUN = function(rid) {
#       if (!is.null(r_libs_user)) .libPaths(c(r_libs_user, .libPaths()))
#       
#       Sys.setenv(GDAL_NUM_THREADS = as.character(par_layout$gdal_threads),
#                  OMP_NUM_THREADS  = as.character(par_layout$omp_threads))
#       Sys.setenv(OPENBLAS_NUM_THREADS   = as.character(par_layout$threads_per_worker),
#                  MKL_NUM_THREADS        = as.character(par_layout$threads_per_worker),
#                  BLIS_NUM_THREADS       = as.character(par_layout$threads_per_worker),
#                  VECLIB_MAXIMUM_THREADS = as.character(par_layout$threads_per_worker))
#       Sys.setenv(GDAL_CACHEMAX = "2048")
#       
#       wd <- file.path(out_terra_temp_base, paste0("worker_", Sys.getpid()))
#       dir.create(wd, recursive = TRUE, showWarnings = FALSE)
#       terra::terraOptions(threads = par_layout$terra_threads,
#                           memfrac = par_layout$terra_memfrac,
#                           tempdir = wd)
#       
#       suppressPackageStartupMessages({
#         library(terra); library(arrow); library(data.table)
#       })
#       
#       # Source zonal functions
#       sys.source(zf, envir = environment())
#       
#       out <- run_region_accumulation(
#         region_id       = rid,
#         zone_dir        = zone_dir,
#         predictor_path  = predictor_path,
#         blocksize       = blocksize,
#         method          = method,
#         stats           = stats,
#         zonal_mode      = zonal_mode,   # renamed
#         progress_every  = 0L,
#         nodata_value    = nodata_value,
#         add_area        = add_area,
#         add_prop        = add_prop
#       )
#       list(region = rid, result = out)
#     },
#     future.packages = c("terra", "arrow", "data.table"),
#     # If globals ever fail to serialize, you can force-export:
#     # future.globals = c("zone_dir","predictor_path","blocksize","method","stats","zonal_mode",
#     #                    "nodata_value","add_area","add_prop","zf","par_layout","out_terra_temp_base",
#     #                    "r_libs_user")
#   )
#   
#   names(res_list) <- vapply(res_list, `[[`, "", "region")
#   
#   if (!combine) return(res_list)
#   
#   all_df <- all(vapply(res_list, function(x) inherits(x$result, "data.frame"), logical(1)))
#   if (all_df && requireNamespace("data.table", quietly = TRUE)) {
#     return(data.table::rbindlist(lapply(res_list, `[[`, "result"), fill = TRUE, use.names = TRUE))
#   } else if (all_df) {
#     out <- do.call(rbind, lapply(res_list, `[[`, "result"))
#     rownames(out) <- NULL
#     return(out)
#   } else {
#     return(res_list)
#   }
# }

run_regions_parallel <- function(regions,
                                 zone_dir,
                                 predictor_path,
                                 blocksize = 6144L,
                                 method = "near",
                                 stats = "sum",
                                 zonal_mode = c("continuous", "categorical"),
                                 nodata_value = NA_integer_,
                                 add_area = FALSE,
                                 add_prop = FALSE,
                                 plan_strategy = c("sequential", "multisession", "multicore", "cluster"),  # add sequential
                                 reserve_cores = 2L,
                                 prefer_threads_per_worker = 2L,
                                 max_workers = NULL,
                                 memfrac_total = 0.8,
                                 approx_mem_per_worker_gb = NULL,
                                 out_terra_temp_base = file.path(tempdir(), "terra_parallel"),
                                 zonal_file = file.path("StreamCatR","zonal_stats_r","04c.zonal_functions_true_blocking_categorical.R"),
                                 r_libs_user = NULL,
                                 combine = TRUE,
                                 verbose = TRUE) {
  stopifnot(length(regions) > 0)
  zonal_mode    <- match.arg(zonal_mode)
  plan_strategy <- match.arg(plan_strategy)
  
  if (!requireNamespace("future", quietly = TRUE) ||
      !requireNamespace("future.apply", quietly = TRUE)) {
    stop("Please install.packages(c('future','future.apply'))")
  }
  
  # Resolve zonal file path now to fail fast
  zf <- tryCatch(normalizePath(zonal_file, mustWork = TRUE),
                 error = function(e) stop("zonal_file not found: ", zonal_file))
  
  # Build parallel layout (renamed to avoid graphics::layout collision)
  par_layout <- auto_parallel_layout(
    n_tasks = length(regions),
    reserve_cores = reserve_cores,
    prefer_threads_per_worker = prefer_threads_per_worker,
    max_workers = max_workers,
    memfrac_total = memfrac_total,
    approx_mem_per_worker_gb = approx_mem_per_worker_gb
  )
  if (verbose) {
    message(sprintf("Parallel layout: workers=%d, threads/worker=%d, terra_memfrac=%.3f",
                    par_layout$workers, par_layout$threads_per_worker, par_layout$terra_memfrac))
  }
  
  # SEQUENTIAL DEBUGGING PATH: place right here, before any future::plan
  if (identical(plan_strategy, "sequential")) {
    # No future; run serial for debugging
    res_list <- lapply(
      regions,
      FUN = function(rid) {
        if (!is.null(r_libs_user)) .libPaths(c(r_libs_user, .libPaths()))
        # Optional: mirror per-worker terra options
        wd <- file.path(out_terra_temp_base, paste0("worker_", Sys.getpid()))
        dir.create(wd, recursive = TRUE, showWarnings = FALSE)
        terra::terraOptions(threads = par_layout$terra_threads,
                            memfrac = par_layout$terra_memfrac,
                            tempdir = wd)
        
        # Load required packages (quietly) and source the zonal functions
        suppressPackageStartupMessages({
          library(terra); library(arrow); library(data.table)
        })
        sys.source(zf, envir = environment())
        
        out <- run_region_accumulation(
          region_id       = rid,
          zone_dir        = zone_dir,
          predictor_path  = predictor_path,
          blocksize       = blocksize,
          method          = method,
          stats           = stats,
          zonal_mode      = zonal_mode,
          progress_every  = 0L,
          nodata_value    = nodata_value,
          add_area        = add_area,
          add_prop        = add_prop
        )
        list(region = rid, result = out)
      }
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
  
  # Otherwise proceed with future-based parallel path
  if (identical(plan_strategy, "multicore") && !future::supportsMulticore()) {
    warning("multicore not supported; falling back to multisession")
    plan_strategy <- "multisession"
  }
  if (identical(plan_strategy, "multisession")) {
    future::plan(future::multisession, workers = par_layout$workers)
  } else if (identical(plan_strategy, "multicore")) {
    future::plan(future::multicore, workers = par_layout$workers)
  } else {
    future::plan(future::cluster, workers = par_layout$workers)
  }
  on.exit(future::plan(future::sequential), add = TRUE)
  
  dir.create(out_terra_temp_base, recursive = TRUE, showWarnings = FALSE)
  
  res_list <- future.apply::future_lapply(
    regions,
    FUN = function(rid) {
      if (!is.null(r_libs_user)) .libPaths(c(r_libs_user, .libPaths()))
      
      Sys.setenv(GDAL_NUM_THREADS = as.character(par_layout$gdal_threads),
                 OMP_NUM_THREADS  = as.character(par_layout$omp_threads))
      Sys.setenv(OPENBLAS_NUM_THREADS   = as.character(par_layout$threads_per_worker),
                 MKL_NUM_THREADS        = as.character(par_layout$threads_per_worker),
                 BLIS_NUM_THREADS       = as.character(par_layout$threads_per_worker),
                 VECLIB_MAXIMUM_THREADS = as.character(par_layout$threads_per_worker))
      Sys.setenv(GDAL_CACHEMAX = "2048")
      
      wd <- file.path(out_terra_temp_base, paste0("worker_", Sys.getpid()))
      dir.create(wd, recursive = TRUE, showWarnings = FALSE)
      terra::terraOptions(threads = par_layout$terra_threads,
                          memfrac = par_layout$terra_memfrac,
                          tempdir = wd)
      
      suppressPackageStartupMessages({
        library(terra); library(arrow); library(data.table)
      })
      sys.source(zf, envir = environment())
      
      out <- run_region_accumulation(
        region_id       = rid,
        zone_dir        = zone_dir,
        predictor_path  = predictor_path,
        blocksize       = blocksize,
        method          = method,
        stats           = stats,
        zonal_mode      = zonal_mode,
        progress_every  = 0L,
        nodata_value    = nodata_value,
        add_area        = add_area,
        add_prop        = add_prop
      )
      list(region = rid, result = out)
    },
    future.packages = c("terra", "arrow", "data.table")
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