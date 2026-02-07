library(sf)
library(dplyr)
library(arrow)

nhd_dir <- 'C:/Users/RHill04/WorkFolder/GIS/NHDPlusV21/NHDPlusNationalData/NHDPlusV21_National_Seamless_Flattened_Lower48.gdb'

flow <- st_read(dsn = nhd_dir, 'PlusFlow') %>% 
  dplyr::select(FROMCOMID, TOCOMID) %>% 
  distinct()

# Add catchments with now flow (islands)
flow <- rbind(flow,
              data.frame(FROMCOMID = c(0, 0), TOCOMID = c(10957920, 20131674)))

arrow::write_parquet(flow,
                     './StreamCatR/full_from_to_conus.parquet',
                     compression = 'zstd')

cats <- st_read(dsn = nhd_dir, 'Catchment') %>% 
  sf::st_drop_geometry() %>% 
  dplyr::select(GRIDCODE, FEATUREID) %>% 
  distinct()

arrow::write_parquet(cats,
                     './StreamCatR/gridcode_comid_translation_conus.parquet',
                     compression = 'zstd')

flowlines <- st_read(dsn = nhd_dir, 'NHDFlowline_Network') %>% 
  sf::st_drop_geometry() %>% 
  dplyr::select(COMID, FTYPE) %>% 
  distinct()

arrow::write_parquet(flowlines,
                     './StreamCatR/ftype_conus.parquet',
                     compression = 'zstd')

special <- data.frame(removeFROMCOMID = c(15714785, 24719331, 14353046, 10466473))

arrow::write_parquet(special,
                     './StreamCatR/special_comid_handling.parquet')

sc_cats <-
  read_parquet('./StreamCatR/BFI.parquet') %>% 
  dplyr::select(COMID)

arrow::write_parquet(sc_cats, './StreamCatR/sc_final_comids_conus.parquet')








  
