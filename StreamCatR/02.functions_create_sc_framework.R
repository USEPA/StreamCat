# ---- Dependencies ----
suppressPackageStartupMessages({
  library(igraph)
  library(dplyr)
  library(arrow)
  library(tictoc)
  library(parallel)
  library(ps)
  library(DBI)
  library(duckdb)
})

options(arrow.use_threads = TRUE)

# ---- Auto-tune resources (with explicit overrides) ----
choose_params <- function(profile = c("auto", "small", "large"),
                          target_frac_ram = NULL,
                          default_ram_gb = 8,
                          row_bytes_est = 24L,
                          ram_override_gb = NULL,
                          cores_override  = NULL) {
  profile <- match.arg(profile)
  
  # Detect resources
  avail_gb <- tryCatch(ps::ps_virtual_memory()$available / 2^30, error = function(e) NA_real_)
  cores    <- tryCatch(parallel::detectCores(logical = TRUE),  error = function(e) NA_integer_)
  if (!is.null(ram_override_gb)) avail_gb <- ram_override_gb
  if (!is.null(cores_override))  cores    <- cores_override
  if (is.na(avail_gb)) avail_gb <- default_ram_gb
  if (is.na(cores)) cores <- 4L
  
  if (is.null(target_frac_ram)) {
    target_frac_ram <- switch(
      profile,
      small = 0.10,
      large = 0.30,
      auto  = if (avail_gb >= 64) 0.25 else if (avail_gb >= 32) 0.20 else 0.15
    )
  }
  
  # Budget for largest in-memory df of pairs
  budget_gb         <- max(0.5, avail_gb * target_frac_ram)
  budget_bytes      <- budget_gb * 2^30
  max_rows_per_part <- as.integer(floor(budget_bytes / row_bytes_est))
  
  # Set vertex_batch_size and codec
  if (profile == "small") {
    vertex_batch_size <- 5000L
    compression_codec <- if (cores <= 4) "snappy" else "zstd"
    use_mini_batches  <- TRUE
    mini_batch_size   <- 2000L
  } else if (profile == "large") {
    vertex_batch_size <- 150000L
    compression_codec <- "zstd"
    use_mini_batches  <- FALSE
    mini_batch_size   <- NA_integer_
  } else {
    vertex_batch_size <- if (avail_gb < 16) 10000L else if (avail_gb < 64) 50000L else 100000L
    compression_codec <- if (cores <= 4 && avail_gb < 16) "snappy" else "zstd"
    use_mini_batches  <- avail_gb < 16
    mini_batch_size   <- if (use_mini_batches) 2000L else NA_integer_
  }
  
  list(
    avail_gb          = avail_gb,
    cores             = cores,
    budget_gb         = budget_gb,
    vertex_batch_size = vertex_batch_size,
    max_rows_per_part = max_rows_per_part,
    compression_codec = compression_codec,
    compression_level = 4L,
    use_mini_batches  = use_mini_batches,
    mini_batch_size   = mini_batch_size
  )
}

# ---- Load coastline COMIDs from ftype_conus (FTYPE == "Coastline", case-insensitive) ----
load_coastline_comids <- function(ftype_parquet, coast_values = c("COASTLINE")) {
  tbl <- arrow::read_parquet(ftype_parquet)
  names(tbl) <- toupper(names(tbl))
  if (!all(c("COMID", "FTYPE") %in% names(tbl))) {
    stop("ftype_parquet must contain COMID and FTYPE columns.")
  }
  tbl %>%
    mutate(FTYPE = toupper(as.character(FTYPE))) %>%
    filter(FTYPE %in% toupper(coast_values)) %>%
    pull(COMID) %>%
    as.integer() %>%
    unique()
}

# ---- Load sink COMIDs from gridcode_comid_translation_conus (FEATUREID includes negatives) ----
# IMPORTANT: keep negative IDs as-is; do NOT take absolute values.
load_sink_comids <- function(gridcode_parquet, feature_col = "FEATUREID") {
  tbl <- arrow::read_parquet(gridcode_parquet)
  names(tbl) <- toupper(names(tbl))
  target <- toupper(feature_col)
  if (!target %in% names(tbl)) {
    stop(sprintf("gridcode_parquet must contain column '%s'.", target))
  }
  vals <- as.integer(tbl[[target]])
  # Sinks indicated by negative FEATUREID; keep them negative
  sink_ids <- unique(vals[!is.na(vals) & vals < 0L])
  sink_ids <- sink_ids[sink_ids != 0L]
  sink_ids
}

# ---- Load special FROMCOMID removals (case-insensitive column: removeFROMCOMID) ----
load_special_fromcomid_removals <- function(special_parquet, column = "removeFROMCOMID") {
  tbl <- arrow::read_parquet(special_parquet)
  names(tbl) <- toupper(names(tbl))
  target <- toupper(column)
  if (!target %in% names(tbl)) {
    stop(sprintf("special_parquet must contain column '%s'.", target))
  }
  vals <- as.integer(tbl[[target]])
  unique(vals[!is.na(vals) & vals != 0L])
}

# ---- Build graph: coastal FROM removed; special FROM removed; sink TO removed; sinks added as vertices ----
build_terminal_coast_graph <- function(flow_parquet, coast_ids, sink_ids, extra_remove_from = integer(0L)) {
  coast_ids <- as.integer(coast_ids)
  sink_ids  <- as.integer(sink_ids)  # may be negative; that's fine
  extra_remove_from <- as.integer(extra_remove_from)
  extra_remove_from <- unique(extra_remove_from[!is.na(extra_remove_from) & extra_remove_from != 0L])
  
  # Read ALL edges (unfiltered) to derive the vertex set
  flow_all <- arrow::read_parquet(flow_parquet) %>%
    dplyr::select(FROMCOMID, TOCOMID) %>%
    dplyr::distinct()
  
  n_edges_before <- nrow(flow_all)
  
  # Build the vertex set from all COMIDs seen in the raw flow (excluding 0),
  # plus explicit coastline and sink COMIDs (negative sinks are included)
  all_ids <- unique(c(flow_all$FROMCOMID, flow_all$TOCOMID))
  all_ids <- all_ids[!is.na(all_ids) & all_ids != 0L]
  v_ids   <- sort(unique(c(all_ids, coast_ids, sink_ids)))
  vertices <- data.frame(name = as.character(v_ids), stringsAsFactors = FALSE)
  
  # Filter edges:
  # - exclude zeros
  # - remove edges where FROM is coastline OR in special removal set
  # - remove edges where TO is a sink (sinks are headwaters: no incoming edges)
  remove_from_set <- unique(c(coast_ids, extra_remove_from))
  edges_filtered <- flow_all %>%
    dplyr::filter(FROMCOMID != 0L, TOCOMID != 0L) %>%
    dplyr::filter(!(FROMCOMID %in% remove_from_set)) %>%
    dplyr::filter(!(TOCOMID %in% sink_ids)) %>%
    dplyr::distinct()
  
  message(sprintf("Edges before: %d; after coastal+special-FROM and sink-TO filter: %d; removed: %d",
                  n_edges_before, nrow(edges_filtered), n_edges_before - nrow(edges_filtered)))
  
  edges <- edges_filtered %>%
    dplyr::transmute(FROMCOMID = as.character(FROMCOMID),
                     TOCOMID   = as.character(TOCOMID))
  
  rm(flow_all, all_ids, edges_filtered); gc()
  
  g <- igraph::graph_from_data_frame(d = edges, vertices = vertices, directed = TRUE)
  rm(edges, vertices); gc()
  
  # QA
  coast_in_graph <- igraph::V(g)$name %in% as.character(coast_ids)
  n_coast_total  <- sum(coast_in_graph)
  n_coast_terms  <- sum(coast_in_graph & igraph::degree(g, mode = "out") == 0L)
  message(sprintf("Coastline vertices in graph: %d; terminals (out-degree 0): %d",
                  n_coast_total, n_coast_terms))
  
  sink_in_graph <- igraph::V(g)$name %in% as.character(sink_ids)
  n_sinks_total <- sum(sink_in_graph)
  n_sinks_headwaters <- sum(sink_in_graph & igraph::degree(g, mode = "in") == 0L)
  message(sprintf("Sink vertices in graph (from gridcode translation): %d; headwaters (in-degree 0): %d",
                  n_sinks_total, n_sinks_headwaters))
  
  g
}

# ---- Write upstream pairs (batched) ----
# include_self=TRUE ensures local rows so sinks/coasts appear with COMID==UPCOMIDS
write_upstream_pairs <- function(g,
                                 out_dir,
                                 vertex_batch_size,
                                 max_rows_per_part,
                                 compression_codec = "zstd",
                                 compression_level = 4L,
                                 use_mini_batches = FALSE,
                                 mini_batch_size = NA_integer_,
                                 use_dictionary = TRUE,
                                 include_self = TRUE) {
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  
  num_names <- as.integer(V(g)$name)
  if (any(is.na(num_names))) stop("NA produced while converting vertex names to int32.")
  n <- vcount(g)
  
  part_counter <- 0L
  tictoc::tic("Compute & write upstream pairs")
  
  mindist_val <- if (isTRUE(include_self)) 0L else 1L
  order_val   <- n
  
  write_chunk <- function(COMIDs, UPCOMIDs, start_label, mb_label = NA) {
    df <- data.frame(COMID = COMIDs, UPCOMIDS = UPCOMIDs)
    part <<- (part_counter <<- part_counter + 1L)
    cat(sprintf("Start=%s%s part=%06d rows=%d size=%.1f MB\n",
                start_label,
                if (!is.na(mb_label)) paste0(" mb=", mb_label) else "",
                part_counter, nrow(df), as.numeric(object.size(df))/2^20))
    write_parquet(
      df,
      sink = file.path(out_dir, sprintf("part-%06d.parquet", part_counter)),
      compression = compression_codec,
      compression_level = compression_level,
      use_dictionary = use_dictionary
    )
    rm(df); gc()
  }
  
  if (!use_mini_batches) {
    for (start in seq(1L, n, by = vertex_batch_size)) {
      idxs <- start:min(start + vertex_batch_size - 1L, n)
      anc_batch <- ego(g, order = order_val, nodes = V(g)[idxs], mode = "in", mindist = mindist_val)
      rows_per_vertex <- lengths(anc_batch)
      
      i <- 1L
      while (i <= length(idxs)) {
        rows_acc <- 0L
        sub_start <- i
        while (i <= length(idxs) && rows_acc + rows_per_vertex[i] <= max_rows_per_part) {
          rows_acc <- rows_acc + rows_per_vertex[i]
          i <- i + 1L
        }
        sub_end <- i - 1L
        
        if (sub_end >= sub_start) {
          sub_anc  <- anc_batch[sub_start:sub_end]
          sub_idxs <- idxs[sub_start:sub_end]
          sub_rows <- rows_per_vertex[sub_start:sub_end]
          COMIDs   <- rep(num_names[sub_idxs], sub_rows)
          UPCOMIDs <- unlist(lapply(sub_anc, function(vs) num_names[as.integer(vs)]), use.names = FALSE)
          write_chunk(COMIDs, UPCOMIDs, start_label = start)
          rm(sub_anc, sub_idxs, sub_rows, COMIDs, UPCOMIDs); gc()
        }
        
        if (i <= length(idxs) && rows_per_vertex[i] > max_rows_per_part) {
          vs <- anc_batch[[i]]
          up_ids <- num_names[as.integer(vs)]
          comid_val <- num_names[idxs[i]]
          for (k in seq(1L, length(up_ids), by = max_rows_per_part)) {
            endk <- min(k + max_rows_per_part - 1L, length(up_ids))
            write_chunk(rep(comid_val, endk - k + 1L), up_ids[k:endk], start_label = start)
          }
          i <- i + 1L
        }
      }
      rm(anc_batch, rows_per_vertex); gc()
    }
  } else {
    if (is.na(mini_batch_size)) mini_batch_size <- 2000L
    for (start in seq(1L, n, by = vertex_batch_size)) {
      idxs <- start:min(start + vertex_batch_size - 1L, n)
      for (mb_start in seq(1L, length(idxs), by = mini_batch_size)) {
        mb_idxs <- idxs[mb_start:min(mb_start + mini_batch_size - 1L, length(idxs))]
        anc_mb  <- ego(g, order = order_val, nodes = V(g)[mb_idxs], mode = "in", mindist = mindist_val)
        rows_mb <- lengths(anc_mb)
        
        i <- 1L
        while (i <= length(mb_idxs)) {
          rows_acc  <- 0L
          sub_start <- i
          while (i <= length(mb_idxs) && rows_acc + rows_mb[i] <= max_rows_per_part) {
            rows_acc <- rows_acc + rows_mb[i]
            i <- i + 1L
          }
          sub_end <- i - 1L
          
          if (sub_end >= sub_start) {
            sub_anc  <- anc_mb[sub_start:sub_end]
            sub_idxs <- mb_idxs[sub_start:sub_end]
            sub_rows <- rows_mb[sub_start:sub_end]
            COMIDs   <- rep(num_names[sub_idxs], sub_rows)
            UPCOMIDs <- unlist(lapply(sub_anc, function(vs) num_names[as.integer(vs)]), use.names = FALSE)
            write_chunk(COMIDs, UPCOMIDs, start_label = start, mb_label = mb_start)
            rm(sub_anc, sub_idxs, sub_rows, COMIDs, UPCOMIDs); gc()
          }
          
          if (i <= length(mb_idxs) && rows_mb[i] > max_rows_per_part) {
            vs <- anc_mb[[i]]
            up_ids <- num_names[as.integer(vs)]
            comid_val <- num_names[mb_idxs[i]]
            for (k in seq(1L, length(up_ids), by = max_rows_per_part)) {
              endk <- min(k + max_rows_per_part - 1L, length(up_ids))
              write_chunk(rep(comid_val, endk - k + 1L), up_ids[k:endk], start_label = start, mb_label = mb_start)
            }
            i <- i + 1L
          }
        }
        rm(anc_mb, rows_mb); gc()
      }
    }
  }
  
  tictoc::toc()
  invisible(part_counter)
}

# ---- Build a stable DuckDB database with persistent pairs table ----
build_duckdb_from_pairs <- function(pairs_dir,
                                    duckdb_path = "conus_pairs.duckdb",
                                    table_name = "pairs",
                                    threads = parallel::detectCores(),
                                    memory_limit_gb = 64) {
  con <- dbConnect(duckdb(), duckdb_path)
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  
  dbExecute(con, sprintf("PRAGMA threads=%d;", threads))
  dbExecute(con, sprintf("PRAGMA memory_limit='%dGB';", as.integer(memory_limit_gb)))
  dbExecute(con, "PRAGMA enable_object_cache=true;")
  
  sql <- sprintf("
    CREATE OR REPLACE TABLE %s AS
    SELECT CAST(COMID AS INTEGER)    AS COMID,
           CAST(UPCOMIDS AS INTEGER) AS UPCOMIDS
    FROM read_parquet('%s/**/*.parquet');
  ", DBI::dbQuoteIdentifier(con, table_name), normalizePath(pairs_dir, winslash = "/"))
  dbExecute(con, sql)
  dbExecute(con, sprintf("ANALYZE %s;", DBI::dbQuoteIdentifier(con, table_name)))
  
  message(sprintf("DuckDB table '%s' created at %s", table_name, duckdb_path))
  invisible(TRUE)
}

# ---- High-level driver: terminal coasts + sinks (from gridcode translation), no consolidation ----
generate_upstream_pairs_terminal_coasts <- function(flow_parquet,
                                                    ftype_parquet,    # path to ftype_conus.parquet (COMID, FTYPE)
                                                    gridcode_parquet, # path to gridcode_comid_translation_conus.parquet (GRIDCODE, FEATUREID)
                                                    pairs_out_dir,
                                                    profile = c("auto", "small", "large"),
                                                    # Optional overrides (NULL = auto)
                                                    vertex_batch_size = NULL,
                                                    max_rows_per_part = NULL,
                                                    compression_codec = NULL,
                                                    compression_level = 4L,
                                                    use_mini_batches = NULL,
                                                    mini_batch_size = NULL,
                                                    # Explicit resource overrides
                                                    ram_override_gb = NULL,
                                                    cores_override  = NULL,
                                                    # Optional extras
                                                    write_headwaters = FALSE,
                                                    headwaters_out = NULL,
                                                    include_self = TRUE,   # include local values by default
                                                    build_duckdb = FALSE,
                                                    duckdb_path = "conus_pairs.duckdb",
                                                    duckdb_table = "pairs",
                                                    # Special FROMCOMID removals parquet (e.g., 'special_comid_handling.parquet')
                                                    special_parquet = NULL) {
  profile <- match.arg(profile)
  
  # Auto-tune (with explicit overrides if provided)
  p <- choose_params(profile = profile,
                     ram_override_gb = ram_override_gb,
                     cores_override  = cores_override)
  
  # Apply explicit overrides
  if (!is.null(vertex_batch_size)) p$vertex_batch_size <- vertex_batch_size
  if (!is.null(max_rows_per_part)) p$max_rows_per_part <- max_rows_per_part
  if (!is.null(compression_codec)) p$compression_codec <- compression_codec
  if (!is.null(use_mini_batches))  p$use_mini_batches  <- use_mini_batches
  if (!is.null(mini_batch_size))   p$mini_batch_size   <- mini_batch_size
  p$compression_level <- compression_level
  
  message(sprintf("Params -> RAM_avail=%.1f GB, cores=%d, batch=%d, rows/part=%d, codec=%s, mini=%s",
                  p$avail_gb, p$cores, p$vertex_batch_size, p$max_rows_per_part,
                  p$compression_codec, p$use_mini_batches))
  
  # Load coastline COMIDs and sink COMIDs (sinks kept negative)
  coast_ids <- load_coastline_comids(ftype_parquet, coast_values = c("COASTLINE"))
  sink_ids  <- load_sink_comids(gridcode_parquet, feature_col = "FEATUREID")
  
  # Optional: load special FROMCOMID removals
  special_remove_ids <- integer(0L)
  if (!is.null(special_parquet)) {
    if (!file.exists(special_parquet)) {
      warning(sprintf("Special removals parquet not found: %s; skipping.", special_parquet))
    } else {
      special_remove_ids <- load_special_fromcomid_removals(special_parquet)
      message(sprintf("Loaded %d special FROMCOMID removals.", length(special_remove_ids)))
    }
  }
  
  # Build graph (coastal FROM removed; special FROM removed; sink TO removed; sinks added as vertices)
  g <- build_terminal_coast_graph(
    flow_parquet,
    coast_ids = coast_ids,
    sink_ids  = sink_ids,
    extra_remove_from = special_remove_ids
  )
  
  # Optional: write headwaters (in-degree == 0). This will include sinks (headwaters by design).
  if (write_headwaters) {
    if (is.null(headwaters_out)) {
      headwaters_out <- file.path(dirname(pairs_out_dir), "headwaters_terminal_coasts.parquet")
    }
    headwater_ids <- as.integer(V(g)$name[degree(g, mode = "in") == 0L])
    write_parquet(data.frame(HEADWATER_COMID = headwater_ids),
                  headwaters_out, compression = "zstd")
    message(sprintf("Headwaters written: %s (n=%d)", headwaters_out, length(headwater_ids)))
  }
  
  # Write upstream pairs
  dir.create(pairs_out_dir, showWarnings = FALSE, recursive = TRUE)
  write_upstream_pairs(
    g,
    out_dir           = pairs_out_dir,
    vertex_batch_size = p$vertex_batch_size,
    max_rows_per_part = p$max_rows_per_part,
    compression_codec = p$compression_codec,
    compression_level = p$compression_level,
    use_mini_batches  = p$use_mini_batches,
    mini_batch_size   = p$mini_batch_size,
    use_dictionary    = TRUE,
    include_self      = include_self
  )
  
  # Optional DuckDB materialization
  if (build_duckdb) {
    build_duckdb_from_pairs(
      pairs_dir       = pairs_out_dir,
      duckdb_path     = duckdb_path,
      table_name      = duckdb_table,
      threads         = p$cores,
      memory_limit_gb = max(16, floor(p$avail_gb * 0.5))
    )
  }
  
  invisible(TRUE)
}

# ------------------------------------------
# Example usage
# ------------------------------------------

# Big machine (e.g., 128 GB RAM, 24 cores)
# generate_upstream_pairs_terminal_coasts(
#   flow_parquet       = "full_from_to_conus.parquet",
#   ftype_parquet      = "ftype_conus.parquet",                      # COMID, FTYPE
#   gridcode_parquet   = "gridcode_comid_translation_conus.parquet", # GRIDCODE, FEATUREID (negatives indicate sinks)
#   pairs_out_dir      = "CONUS_pairs_highperf",
#   profile            = "large",
#   ram_override_gb    = 128,
#   cores_override     = 24,
#   vertex_batch_size  = 200000L,
#   max_rows_per_part  = 400000000L,
#   include_self       = TRUE,
#   special_parquet    = "special_comid_handling.parquet"  # remove special FROMCOMIDs
# )

# Small machine (auto-tune for low RAM)
# generate_upstream_pairs_terminal_coasts(
#   flow_parquet      = "full_from_to_conus.parquet",
#   ftype_parquet     = "ftype_conus.parquet",
#   gridcode_parquet  = "gridcode_comid_translation_conus.parquet",
#   pairs_out_dir     = "CONUS_pairs_lowram",
#   profile           = "small",
#   include_self      = TRUE,
#   build_duckdb      = FALSE,
#   special_parquet   = "special_comid_handling.parquet"
# )