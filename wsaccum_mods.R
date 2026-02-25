library(arrow)
library(dplyr)
library(rlang)

# Function to perform watershed accumulated sum of catchment-level variables
# A list of specific variables from the input dataset can be provided in "var_cols"
# The function will run across all numeric columns if var_cols = NULL (except id_col)
# Number of threads to use to run the process in parallel can be specified in "num_threads"
# All available cores will be used to run the function if num_threads = NULL
# Provide catchment identifiers in id_cols

library(arrow)
library(dplyr)
library(rlang)

wS_accum_sum <- function(pairs_dir,
                                           zonal_parquet,
                                           pairs_id_col  = "COMID",     # downstream watershed ID in pairs
                                           zonal_id_col  = "COMID",     # catchment ID in zonal (e.g., FEATUREID)
                                           upstream_col  = "UPCOMIDS",  # upstream IDs in pairs (matches zonal_id_col)
                                           var_cols      = NULL,
                                           num_threads   = NULL) {
  # Configure Arrow parallelism
  avail <- parallel::detectCores()
  threads <- if (is.null(num_threads)) avail else as.integer(num_threads)
  if (is.na(threads) || threads < 1) threads <- 1
  threads <- min(threads, avail)
  options(arrow.use_threads = threads > 1)
  Sys.setenv(ARROW_NUM_THREADS = threads)
  
  # Open datasets lazily
  ds_pairs <- open_dataset(pairs_dir, format = "parquet", unify_schemas = TRUE)
  ds_zonal <- open_dataset(zonal_parquet, format = "parquet", unify_schemas = TRUE)
  
  # Tidy-eval symbols
  id_pairs_sym <- sym(pairs_id_col)
  id_zonal_sym <- sym(zonal_id_col)
  up_sym       <- sym(upstream_col)
  
  # Determine variables to accumulate (default: all numeric columns in zonal except IDs)
  sch <- ds_zonal$schema
  zonal_cols <- sch$names
  
  is_arrow_numeric <- function(type_str) {
    grepl("^(int|uint|float|double|decimal)", type_str)
  }
  
  if (is.null(var_cols)) {
    numeric_names <- vapply(
      sch$fields,
      function(f) if (is_arrow_numeric(f$type$ToString())) f$name else NA_character_,
      character(1)
    )
    numeric_names <- na.omit(numeric_names)
    # Exclude identifier and upstream columns even if numeric
    var_cols <- setdiff(numeric_names, c(zonal_id_col, upstream_col))
  } else {
    # Validate provided var_cols: must exist and be numeric; then drop identifiers/upstream
    type_map <- setNames(
      vapply(sch$fields, function(f) f$type$ToString(), character(1)),
      vapply(sch$fields, function(f) f$name, character(1))
    )
    var_cols <- intersect(var_cols, zonal_cols)
    var_cols <- var_cols[vapply(var_cols, function(v) is_arrow_numeric(type_map[[v]]), logical(1))]
    removed_ids <- intersect(var_cols, c(zonal_id_col, upstream_col))
    if (length(removed_ids) > 0) {
      var_cols <- setdiff(var_cols, removed_ids)
      message("Excluding identifier/upstream columns from accumulation: ",
              paste(removed_ids, collapse = ", "))
    }
  }
  
  if (length(var_cols) == 0) {
    stop("No valid numeric variable columns found to accumulate after excluding identifier/upstream columns. ",
         "Provide 'var_cols' or ensure numeric columns exist.")
  }
  
  # Pre-cast and select necessary columns to reduce I/O
  ds_pairs_cast <- ds_pairs %>%
    select(all_of(c(upstream_col, pairs_id_col))) %>%
    mutate(
      !!up_sym       := cast(!!up_sym,       int32()),
      !!id_pairs_sym := cast(!!id_pairs_sym, int32())
    )
  
  ds_zonal_id <- ds_zonal %>%
    mutate(!!id_zonal_sym := cast(!!id_zonal_sym, int32()))
  
  # Watershed accumulation: join UPCOMIDS (pairs) -> zonal_id_col (zonal), then sum
  ws_tbl <- ds_pairs_cast %>%
    inner_join(
      ds_zonal_id %>% select(!!id_zonal_sym, all_of(var_cols)),
      by = setNames(zonal_id_col, upstream_col)  # pairs[[UPCOMIDS]] matched to zonal[[zonal_id_col]]
    ) %>%
    group_by(!!id_pairs_sym) %>%
    summarise(across(all_of(var_cols), ~ sum(.x, na.rm = TRUE))) %>%
    rename(COMID = !!id_pairs_sym) %>%
    rename_with(~ paste0(.x, "Ws"), all_of(var_cols))
  
  # Catchment-level (local) values from zonal data
  cat_tbl <- ds_zonal_id %>%
    select(!!id_zonal_sym, all_of(var_cols)) %>%
    rename(COMID = !!id_zonal_sym) %>%
    rename_with(~ paste0(.x, "Cat"), all_of(var_cols))
  
  # Desired column order: COMID, then interleaved variableCat and variableWs for each var
  out_cols <- c("COMID", as.vector(rbind(paste0(var_cols, "Cat"), paste0(var_cols, "Ws"))))
  
  # Final result
  res <- cat_tbl %>%
    left_join(ws_tbl, by = "COMID") %>%
    select(all_of(out_cols)) %>%
    arrange(COMID) %>%
    collect()
  
  res
}



# Example run and QA using 4 previously accumulated wetland variables

tictoc::tic()
out <- wS_accum_sum(
  pairs_dir      = "./StreamCatR/streamcat_files",
  zonal_parquet  = "D:/NLCDAnnual/Figures/WetInt/Accumulated_Nitrogen_Metrics.parquet", # Wetland variable dataset
  pairs_id_col   = "COMID",
  zonal_id_col   = "COMID",
  upstream_col   = "UPCOMIDS",
  var_cols       = c("wetNagsurp1987Cat", "wetNdev1987Cat", "wetNatdep1987Cat", "wetNdev2017Cat")  # optional; or omit to auto-detect numeric cols
)
tictoc::toc()
# 15.51 sec elapsed
colnames(test)
test <- out %>%
  rename_with(~ sub("Cat", "", .x), .cols = everything()) |> #"Cat" was already in the input variable name
  rename_with(~ paste0(.x, '_wsacc'), .cols = -COMID)

QAdf <- arrow::read_parquet("D:/NLCDAnnual/Figures/WetInt/Accumulated_Nitrogen_Metrics.parquet") |> 
  select(COMID, contains(c("wetNagsurp1987", "wetNdev1987", "wetNatdep1987", "wetNdev2017"))) |> 
  rename_with(~ paste0(.x, '_sc'), .cols = -COMID) |> 
  left_join(test, by = "COMID")
colnames(QAdf)

# function to use in mutate() to calculate difference between multiple pairs of columns
compute_diff <- function(data, old_col) {
  new_col <- str_replace(old_col, "_sc$", "_wsacc")
  if (new_col %in% names(data)) {
    return(data[[new_col]] - data[[old_col]])
  } else {
    return(rep(NA_real_, nrow(data))) # Handle missing _new columns
  }
}

checkit <- QAdf %>% 
  mutate(across(
    ends_with("_sc"),
    ~ compute_diff(cur_data(), cur_column()),
    .names = "diff_{str_remove(.col, '_sc$')}"
  ))

summary(checkit)

# View columns for 1987 AgSurp to visually check
N87ag <- checkit |> 
  select(COMID, contains('wetNagsurp1987'))
View(N87ag)
