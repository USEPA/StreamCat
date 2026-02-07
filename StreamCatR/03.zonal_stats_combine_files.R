library(sf)
library(arrow)
library(tidyverse)
library(foreign)
library(tictoc)

zonal_dir <- file.path(getwd(), "zonal_files")

zfiles <- list.files(
  path = zonal_dir,
  pattern = ".dbf",
  recursive = TRUE,
  full.names = TRUE
)

tictoc::tic()
zonal <- 
  lapply(zfiles, foreign::read.dbf) %>% 
  bind_rows() %>%
  dplyr::select(VALUE, COUNT, SUM) %>%
  #distinct() %>% 
  dplyr::left_join(
    arrow::read_parquet('gridcode_comid_translation.parquet'),
    by = dplyr::join_by(x$VALUE == y$GRIDCODE)) %>% 
  dplyr::select(FEATUREID, SUM, COUNT) %>% 
  na.omit()
tictoc::toc()

nrow(zonal)
nrow(zonal %>% 
       dplyr::select(FEATUREID) %>% 
       distinct())

zonal2 <- cats %>% 
  dplyr::select(FEATUREID, AreaSqKM) %>% 
  dplyr::mutate(AREA = AreaSqKM*1e6) %>% 
  dplyr::select(FEATUREID, AREA) %>% 
  left_join(zonal, by = 'FEATUREID')



zonal2 <- zonal %>% 
  left_join(
    cats %>% 
      dplyr::select(FEATUREID, AreaSqKM) %>% 
      dplyr::mutate(AREA = AreaSqKM*1e6) %>% 
      dplyr::select(FEATUREID, AREA)
  )

write_parquet(zonal2, 'zonal2.parquet')


arrow::write_parquet(zonal, 'zonal.parquet')
