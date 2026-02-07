library(DBI)
library(duckdb)
library(parallel)

compute_ws_union_duckdb <- function(db_path,
                                    pairs_table      = "pairs",
                                    zonal_parquet,
                                    mean_base        = "MEAN",
                                    feature_col      = "FEATUREID",
                                    sum_col          = "SUM",
                                    count_col        = "COUNT",
                                    pixel_area_m2    = 900,
                                    result_table     = NULL,   # set to a name to materialize result in DuckDB
                                    threads          = parallel::detectCores(),
                                    memory_limit_gb  = 64,
                                    enable_cache     = TRUE,
                                    return_df        = TRUE) {
  
  con <- dbConnect(duckdb(), db_path)
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  
  # Performance knobs
  dbExecute(con, sprintf("PRAGMA threads=%d;", as.integer(threads)))
  dbExecute(con, sprintf("PRAGMA memory_limit='%dGB';", as.integer(memory_limit_gb)))
  if (isTRUE(enable_cache)) dbExecute(con, "PRAGMA enable_object_cache=true;")
  
  # Quote identifiers safely
  pairs_q  <- DBI::dbQuoteIdentifier(con, pairs_table)
  feat_q   <- DBI::dbQuoteIdentifier(con, feature_col)
  sum_q    <- DBI::dbQuoteIdentifier(con, sum_col)
  count_q  <- DBI::dbQuoteIdentifier(con, count_col)
  
  # Normalize path for DuckDB and escape single quotes
  zpath <- normalizePath(zonal_parquet, winslash = "/", mustWork = FALSE)
  zpath <- gsub("'", "''", zpath, fixed = TRUE)
  
  # Factor to convert COUNT to km^2
  area_factor <- pixel_area_m2 / 1e6
  
  # Build SQL that:
  # 1) Reads zonal Parquet and normalizes types
  # 2) Aggregates watershed sums/counts (join pairs -> zonal)
  # 3) Aggregates catchment (local) sums/counts from zonal by FEATUREID
  # 4) Produces COMID, CatAreaSqKm, WsAreaSqKm, mean_cat, mean_ws
  sql <- sprintf("
    WITH z AS (
      SELECT
        CAST(%s AS INTEGER) AS FEATUREID,
        CAST(%s AS DOUBLE)  AS SUMVAL,
        CAST(%s AS DOUBLE)  AS CNTVAL
      FROM read_parquet('%s')
    ),
    ws AS (
      SELECT
        p.COMID::INTEGER AS COMID,
        SUM(z.SUMVAL)    AS ws_sum,
        SUM(z.CNTVAL)    AS ws_count
      FROM %s AS p
      LEFT JOIN z
        ON p.UPCOMIDS = z.FEATUREID
      GROUP BY p.COMID
    ),
    cat AS (
      SELECT
        FEATUREID AS COMID,
        SUM(SUMVAL) AS cat_sum,
        SUM(CNTVAL) AS cat_count
      FROM z
      GROUP BY FEATUREID
    )
    SELECT
      w.COMID,
      COALESCE(c.cat_count, 0) * %.12f AS CatAreaSqKm,
      w.ws_count               * %.12f AS WsAreaSqKm,
      CASE WHEN c.cat_count > 0 THEN c.cat_sum / c.cat_count ELSE NULL END AS mean_cat,
      CASE WHEN w.ws_count > 0 THEN w.ws_sum / w.ws_count ELSE NULL END AS mean_ws
    FROM ws w
    LEFT JOIN cat c ON w.COMID = c.COMID
  ",
  as.character(feat_q), as.character(sum_q), as.character(count_q),
  zpath,
  as.character(pairs_q),
  area_factor, area_factor
  )
  
  # If result_table is provided, materialize result in DuckDB
  if (!is.null(result_table)) {
    rt_q <- DBI::dbQuoteIdentifier(con, result_table)
    dbExecute(con, sprintf("CREATE OR REPLACE TABLE %s AS %s;", as.character(rt_q), sql))
    dbExecute(con, sprintf("ANALYZE %s;", as.character(rt_q)))
    if (!return_df) return(invisible(TRUE))
    df <- dbGetQuery(con, sprintf("SELECT * FROM %s;", as.character(rt_q)))
  } else {
    # Otherwise, just return the result as a data.frame
    df <- dbGetQuery(con, sql)
  }
  
  # Rename mean columns to requested names and order columns
  mean_cat_name <- paste0(mean_base, "Cat")
  mean_ws_name  <- paste0(mean_base, "Ws")
  names(df)[names(df) == "mean_cat"] <- mean_cat_name
  names(df)[names(df) == "mean_ws"]  <- mean_ws_name
  
  # Ensure column order: COMID, CatAreaSqKm, WsAreaSqKm, <mean_base>Cat, <mean_base>Ws
  df <- df[, c("COMID", "CatAreaSqKm", "WsAreaSqKm", mean_cat_name, mean_ws_name), drop = FALSE]
  
  df
}

# Usage:

# tictoc::tic()
# res_duck <- compute_ws_union_duckdb(
#   db_path        = "conus_pairs.duckdb",
#   pairs_table    = "pairs",
#   zonal_parquet  = "zonal2.parquet",
#   mean_base      = "BFI",       # will produce BFICat and BFIWs
#   feature_col    = "FEATUREID",
#   sum_col        = "SUM",
#   count_col      = "COUNT",
#   pixel_area_m2  = 900,         # 30m x 30m pixels
#   result_table   = NULL,        # or e.g., "bfi_ws" to persist in DuckDB
#   threads        = parallel::detectCores(),
#   memory_limit_gb= 64,
#   enable_cache   = TRUE,
#   return_df      = TRUE
# )
# tictoc::toc()
# # Inspect
# head(res_duck)
# summary(res_duck)
