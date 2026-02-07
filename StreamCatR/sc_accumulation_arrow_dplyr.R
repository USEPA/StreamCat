library(arrow)
library(dplyr)
library(rlang)

compute_ws_union_metrics <- function(pairs_dir,
                                     zonal_parquet,
                                     feature_id_col  = "FEATUREID",
                                     sum_col         = "SUM",
                                     count_col       = "COUNT",
                                     area_col        = "AREA",
                                     metric_name     = "BFI") {
  # Open Datasets lazily
  ds_pairs <- open_dataset(pairs_dir, format = "parquet", unify_schemas = TRUE)
  ds_zonal <- open_dataset(zonal_parquet, format = "parquet", unify_schemas = TRUE)
  
  # Symbols for NSE column references
  fid <- sym(feature_id_col)
  sc  <- sym(sum_col)
  cc  <- sym(count_col)
  ac  <- sym(area_col)
  
  # Dynamic output names (e.g., "BFICat", "BFIWs")
  metric_cat <- paste0(metric_name, "Cat")
  metric_ws  <- paste0(metric_name, "Ws")
  metric_cat_sym <- sym(metric_cat)
  metric_ws_sym  <- sym(metric_ws)
  
  # Watershed union: join pairs UPCOMIDS to zonal FEATUREID, then accumulate
  ws_tbl <- ds_pairs %>%
    mutate(
      UPCOMIDS = cast(UPCOMIDS, int32()),
      COMID    = cast(COMID,    int32())
    ) %>%
    left_join(
      ds_zonal %>% mutate(!!fid := cast(!!fid, int32())),
      by = c("UPCOMIDS" = feature_id_col)
    ) %>%
    group_by(COMID) %>%
    summarise(
      ws_sum   = sum(!!sc, na.rm = TRUE),
      ws_count = sum(!!cc, na.rm = TRUE),
      ws_area  = sum(!!ac, na.rm = TRUE)   # AREA in m²
    )
  
  # Local catchment stats 
  cat_tbl <- ds_zonal %>%
    mutate(!!fid := cast(!!fid, int32())) %>%
    select(!!fid, SUM, COUNT, AREA) %>%
    rename(COMID = !!fid,
           cat_sum = SUM,
           cat_count = COUNT,
           cat_area = AREA)
  
  # Final metrics and dynamic mean column names
  res <- ws_tbl %>%
    left_join(cat_tbl, by = "COMID") %>%
    mutate(
      # Areas from AREA (m² -> km²)
      CatAreaSqKm = coalesce(cat_area, 0) / 1e6,
      WsAreaSqKm  = coalesce(ws_area,  0) / 1e6,
      # Means using COUNT (SUM/COUNT), with dynamic names
      !!metric_cat_sym := if_else(!is.na(cat_count) & cat_count > 0, cat_sum / cat_count, NA_real_),
      !!metric_ws_sym  := if_else(ws_count > 0, ws_sum / ws_count, NA_real_)
    ) %>%
    select(COMID, CatAreaSqKm, WsAreaSqKm, !!metric_cat_sym, !!metric_ws_sym) %>%
    collect()
  
  res
}



