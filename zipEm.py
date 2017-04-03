# -*- coding: utf-8 -*-
"""
Created on Thu Jan 12 11:30:50 2017

@author: Rdebbout
"""

import os
from os.path import basename
import zipfile
from Tkinter import Tk
from tkFileDialog import askdirectory
Tk().withdraw()

def main():
    home = askdirectory(title='Select directory where CSVs are stored...\n\
    A directory named \'zips\' will be created if not there\n\
    Only zips that\ don\'t exist yet will be made.',initialdir=os.getcwd())
    if not os.path.exists('%s/zips' % home):
        os.mkdir('%s/zips' % home)
    for f in os.listdir(home):
        if '.csv' in f:
            fn = f.split('.')[0]
            if not os.path.exists('%s/zips/%s.zip' % (home,fn)):
                print 'zipping......  %s' % fn
                zf = zipfile.ZipFile('%s/zips/%s.zip' % (home,fn), mode='w')
#                zf.write('%s/%s'%(home,f), 
#                         compress_type=zipfile.ZIP_DEFLATED)
                zf.write('%s/%s'%(home,f), basename('%s/%s'%(home,f)),
                         compress_type=zipfile.ZIP_DEFLATED)
                zf.close()

#############################################################################

if __name__ =='__main__':
    main()
   
    
#vpus = r'L:\Priv\CORFiles\Geospatial_Library\Data\Project\StreamCat\FTP_Staging\StreamCat\HydroRegions'
#states = r'L:\Priv\CORFiles\Geospatial_Library\Data\Project\StreamCat\FTP_Staging\StreamCat\States'
#v_uni = []
#for f in os.listdir(vpus):
#    if '.csv' in f:
#        v_uni.append(f.split('.')[0].split('Region')[0])
#
#
#s_uni = []       
#for f in os.listdir(states):
#    if '.csv' in f:
#        s_uni.append(f.split('.')[0][:-2])
#
#print set(v_uni)-set(s_uni)