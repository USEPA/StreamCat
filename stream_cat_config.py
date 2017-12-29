# -*- coding: utf-8 -*-
import os
import numpy as np
from StreamCat_functions import nhd_dict

# location of the landscape layers to perform statistics on
LYR_DIR = ('L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat'
                '/LandscapeRasters/QAComplete')

# location of the NHDPlus
NHD_DIR = 'D:/NHDPlusV21'

# location to write out StreamCat data
OUT_DIR = ('L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat'
            '/Allocation_and_Accumulation')

# ordered dict of NHD VPUs using the 'Hydroseq' attribute from BoundaryUnit.dbf
# for presribing the order of accumulations
INPUTS = np.load('./accum_npy/vpus.npy').item() if os.path.exists('./accum_npyvpu_inputs.npy') else nhd_dict(NHD_DIR)






#final_tables_dir	L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat/FTP_Staging/StreamCat/HydroRegions
#
#pct_full_file	L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat/ControlTables/ALL_BORDER_CATS.csv
#StreamCat_repo	D:/Projects/StreamCat
#mask_dir_RP100	D:/Projects/Masks/RipBuf100
#pct_full_file_RP100	L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat/ControlTables/ALL_BORDER_CATS_Rp100.csv
#mask_dir_Slp10	D:/Projects/Masks/midslope
#mask_dir_Slp20	D:/Projects/Masks/highslope

