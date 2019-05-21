import pandas as pd
import os
from ftplib import FTP


ftp = FTP('newftp.epa.gov')
ftp.login()
ftp.cwd("/EPADataCommons/ORD/NHDPlusLandscapeAttributes/StreamCat/HydroRegions/")

mismatches = dict()

region_list = [reg.split('.')[0].split('_')[-1] for reg in ftp.nlst() if reg.count('.zip')]
region_list = list(set(region_list)) # remove dups
region_list = [reg for reg in region_list if not reg.count('v2')]


filelist = ftp.nlst()
# cheating and pointing to local staging directory 
hydrodir = 'L:/Priv/CORFiles/Geospatial_Library_Projects/StreamCat/FTP_Staging/StreamCat/HydroRegions/'
for region in region_list:
    region_recordlens = dict()
    for f in os.listdir(hydrodir):
        if f.replace('.csv','.zip') in filelist and f.split('.')[0].split('_')[-1] == region:
            print f
            check = pd.read_csv(hydrodir + f)
            region_recordlens[f.split('.')[0].split('_Region')[0]] = len(check)
    expected_value = next(iter(region_recordlens.values()))
    for k,v in region_recordlens.iteritems():
        if v != expected_value:
            if not mismatches.has_key(region):
                mismatches[region] = [k]
            else:
                mismatches[region].append(k)
            
