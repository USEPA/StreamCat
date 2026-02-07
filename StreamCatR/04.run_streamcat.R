
# ---- Generate StreamCat Files ----

source('./StreamCatR/02.functions_create_sc_framework.R')

generate_upstream_pairs_terminal_coasts(
  flow_parquet       = "./StreamCatR/full_from_to_conus.parquet",
  ftype_parquet      = "./StreamCatR/ftype_conus.parquet",                      # COMID, FTYPE
  gridcode_parquet   = "./StreamCatR/gridcode_comid_translation_conus.parquet", # GRIDCODE, FEATUREID (negatives indicate sinks)
  pairs_out_dir      = "./StreamCatR/streamcat_files",
  profile            = "large",
  ram_override_gb    = 128,
  cores_override     = 24,
  vertex_batch_size  = 200000L,
  max_rows_per_part  = 400000000L,
  include_self       = TRUE,
  special_parquet    = "./StreamCatR/special_comid_handling.parquet"  # remove special FROMCOMIDs
)


# ---- Run StreamCat Accumulation ----

source('./StreamCatR/sc_accumulation_arrow_dplyr.R')

pairs_dir <- "./StreamCatR/streamcat_files"
zonal_parquet <- "./StreamCatR/zonal2.parquet"

tictoc::tic()
res_arrow <- compute_ws_union_metrics(pairs_dir,
                                 zonal_parquet)
tictoc::toc()
summary(res_arrow)

# Need to filter to match just COMIDs in StreamCat

# ---- QA against StreamCat BFI parquet file ----

test <- res_arrow %>% 
  mutate(new_area = WsAreaSqKm, new_cat_area = CatAreaSqKm, new_wsmean = BFIWs, new_catmean = BFICat) %>% 
  dplyr::select(COMID, new_area, new_cat_area, new_wsmean, new_catmean)

bfi <- read_parquet("./StreamCatR/BFI.parquet") %>% 
  mutate(sc_area = WsAreaSqKm, sc_wsmean = BFIWs, sc_cat_area=CatAreaSqKm, sc_catmean = BFICat) %>% 
  dplyr::select(COMID, sc_area, sc_cat_area, sc_wsmean, sc_catmean)

test2 <- bfi %>% 
  left_join(test, by = 'COMID') %>% 
  mutate(diff = sc_area - new_area,
         diff2 = sc_cat_area - new_cat_area,
         diff3 = sc_wsmean - new_wsmean)

View(test2)


