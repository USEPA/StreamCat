# -*- coding: utf-8 -*-
"""
Created on Wed Dec 09 14:02:46 2015

This script writes out a schema.ini file for the tables format found in the 
csv's in StreamCat directories for reading into ArcMap.

@author: Rdebbout
"""
import os
import pandas as pd
import csv
#Location of tables to create schema.ini file for, either HydroRegions OR States
wrk_dir = 'L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat/FTP_Staging/StreamCat/States'
#################################################################################################################################
# create list of filenames that contain the string '.csv' in them from the directory above
files = os.listdir(wrk_dir)
files = [x for x in files if x.count('.csv')]
#remove old schema file
if os.path.exists('%s/schema.ini' % wrk_dir):
    os.remove('%s/schema.ini' % wrk_dir)
#open file to write to using csv module
b = open('%s/schema.ini' % wrk_dir, 'wb')
a = csv.writer(b,delimiter=',')
hold = 'bogus'
for f in files:
    print f
    if f.split('_')[0] != hold:
        tbl = pd.read_csv(wrk_dir + '/' + f)
        # use to compare table names so only 1 table has to be read in for each metric
        hold = f.split('_')[0]  
    if f.split('_')[0] == hold:  # same table name will copy the same schema from the 1st read-in
        a.writerow([str([f])])
        for i in range(len(tbl.columns)):
            if i == 0:
                a.writerow(['Col' + str(i + 1) + '=' + str(tbl.columns[i]) + ' Long'])
            else:
                a.writerow(['Col' + str(i + 1) + '=' + str(tbl.columns[i]) + ' Double'])
b.close()