import os
import time
import numpy as np
import geopandas as gpd

NHD_DIR = "E:/NHDPlusV21"
INPUTS = np.load("accum_npy/vpu_inputs.npy", allow_pickle=True).item()

if not os.path.exists("nhd_catchments"):
    os.mkdir("nhd_catchments")
f = "{}/NHDPlus{}/NHDPlus{}/NHDPlusCatchment/Catchment.shp"
start_time = time.time()
for vpu, region in INPUTS.items():
    vpu_time = time.time()
    cats = gpd.read_file(f.format(NHD_DIR, region, vpu))
    cats.to_crs("epsg:5070", inplace=True)
    cats.to_file("nhd_catchments/catchments_%s.shp" % vpu)
    print("VPU {} finished in {} seconds".format(vpu, time.time()-vpu_time))
print("Total time: {} seconds".format(time.time()-start_time))
