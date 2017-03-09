# -*- coding: utf-8 -*-
"""
Created on Jan 22, 2016
Script to build final StreamCat tables.
Run script from command line passing directory and name of this script 
and then directory and name of the control table to use like this:
Python "F:\Watershed Integrity Spatial Prediction\Scripts\makeFinalTables.py" 
or Python "L:\Priv\CORFiles\Geospatial_Library\Data\Project\SSWR1.1B\ControlTables\ControlTable_StreamCat_RD.csv"
@author: rdebbout, mweber
"""

import sys, os
import pandas as pd
#ctl = pd.read_csv(sys.argv[1]).set_index('f_d_Title') 
ctl = pd.read_csv('J:/GitProjects/StreamCat/ControlTable_StreamCat.csv').set_index('f_d_Title')
dls = 'DirectoryLocations'
sys.path.append(ctl.ix['StreamCat_repo'][dls])  # sys.path.append('D:/Projects/Scipts')
from StreamCat_functions import NHD_Dict
inputs = NHD_Dict(ctl.ix['NHD_dir'][dls])
inDir = ctl.ix['out_dir'][dls]
outDir = ctl.ix['final_tables_dir'][dls]
LENGTHS ={'11': 204120, '02': 126185, '13': 56220, '01': 65968, '06': 57642,
          '07': 183667, '10L': 196552, '05': 170145, '18': 140835, '08': 151544,
          '16': 95143, '04': 105452, '12': 68127, '03S': 55586, '17': 231698,
          '14': 83084, '15': 100243, '09': 29776, '03N': 132903, '03W': 135522,
          '10U': 256645}
tables = dict()
for row in range(len(ctl.Final_Table_Name)):
    if ctl.run[row] == 1 and  len(ctl.Final_Table_Name[row]):
        tables[ctl.Final_Table_Name[row]] = ctl.FullTableName.ix[ctl.Final_Table_Name == ctl.Final_Table_Name[row]].tolist()
        tables[ctl.Final_Table_Name[row]].sort()
missing = []
for table in tables:
    for zone in inputs:
        for var in range(len(tables[table])):
            if not os.path.exists(inDir + '/%s_%s.csv'%(tables[table][var],zone)):
                missing.append(tables[table][var] + '_' + zone + '.csv')
if len(missing) > 0:
    for miss in missing:
        print 'Missing ' + miss
    print 'Check output from StreamCat.py'
    sys.exit()
for table in tables:
    print 'Running ' + table + ' .....into ' + outDir 
    # Looop through NHD Hydro-regions
    for zone in inputs:
        # Check if output tables exist before writing
        if not os.path.exists(outDir +'/' + table + '_Region' + zone + '.csv'):
            for var in range(len(tables[table])):
                # Get accumulation type, i.e. point, continuous, categorical
                accum = ctl.accum_type.ix[ctl.Final_Table_Name == table].any()
                metricName = ctl.MetricName.ix[ctl.FullTableName == tables[table][var]].item()
                # G3t metric type, i.e. mean, density, percent
                metricType = ctl.MetricType.ix[ctl.FullTableName == tables[table][var]].item()
                # appendMetric is simply whether to add Rp100 at end of file name
                appendMetric = ctl.AppendMetric.ix[ctl.FullTableName == tables[table][var]].item()
                if appendMetric == 'none':
                    appendMetric = ''
                # Typically conversion is 1, but if values need converting apply conversion factor
                conversion = float(ctl.Conversion.ix[ctl.FullTableName == tables[table][var]].values[0])
                # Read in the StreamCat allocation and accumulation table for the zone and the particular metric
                tbl = pd.read_csv(inDir + '/%s_%s.csv'%(tables[table][var],zone)) 
                # Grab initial set of columns from allocation and accumulation table to start building final table
                frontCols = [title for title in tbl.columns for x in ['COMID','AreaSqKm','PctFull'] if x in title and not 'Up' in title]            
                catArea = frontCols[1]
                catPct = frontCols[2]
                wsArea = frontCols[3]
                wsPct = frontCols[4] 
                frontCols = [frontCols[i] for i in [0,1,3,2,4]] #re-order for correct sequence
                summary = None
                if ctl.summaryfield.ix[ctl.Final_Table_Name == table].any():
                    summary = ctl.summaryfield.ix[ctl.FullTableName == tables[table][var]].item().split(';')         
                if metricType == 'Mean':   
                    colname1 = metricName + 'Cat' + appendMetric
                    colname2 = metricName + 'Ws' + appendMetric
                    tbl[colname1] = ((tbl['CatSum%s' % appendMetric] / tbl['CatCount%s' % appendMetric]) * conversion)
                    tbl[colname2] = ((tbl['WsSum%s' % appendMetric] / tbl['WsCount%s' % appendMetric]) * conversion)                        
                    if var == 0:
                        final = tbl[frontCols + [colname1] + [colname2]]
                    else: 
                        final = pd.merge(final,tbl[["COMID",colname1,colname2]],on='COMID')
                if metricType == 'Density':
                    colname1 = metricName + 'Cat' + appendMetric
                    colname2 = metricName + 'Ws' + appendMetric                   
                    if summary:
                        finalNameList = []
                        for sname in summary: 
                            if 'Dens' in  metricName:
                                metricName = metricName[:-4]
                            fnlname1 = metricName + sname + 'Cat' + appendMetric
                            fnlname2 = metricName + sname + 'Ws' + appendMetric
                            tbl[fnlname1] = tbl['Cat' + sname] / (tbl[catArea] * (tbl[catPct]/100))
                            tbl[fnlname2] = tbl['Ws' + sname] / (tbl[wsArea] * (tbl[wsPct]/100)) 
                            finalNameList.append(fnlname1)
                            finalNameList.append(fnlname2)
                    if table == 'RoadStreamCrossings' or table == 'CanalsDitches':
                        tbl[colname1] = (tbl.CatSum / (tbl.CatAreaSqKm * (tbl.CatPctFull/100)) * conversion) ## NOTE:  Will there ever be a situation where we will need to use 'conversion' here
                        tbl[colname2] = (tbl.WsSum / (tbl.WsAreaSqKm * (tbl.WsPctFull/100)) * conversion)                        
                    else:
                        tbl[colname1] = (tbl['CatCount%s' % appendMetric] / (tbl['CatAreaSqKm%s' % appendMetric] * (tbl['CatPctFull%s' % appendMetric]/100)) * conversion) ## NOTE:  Will there ever be a situation where we will need to use 'conversion' here
                        tbl[colname2] = (tbl['WsCount%s' % appendMetric] / (tbl['WsAreaSqKm%s' % appendMetric] * (tbl['WsPctFull%s' % appendMetric]/100)) * conversion)                      
                    if var == 0:
                        if summary:
                            final = tbl[frontCols + [colname1] + [x for x in finalNameList if 'Cat' in x] + [colname2] + [x for x in finalNameList if 'Ws' in x]]  
                        else: 
                            final = tbl[frontCols + [colname1] + [colname2]]
                    else: 
                        if summary:
                            final = pd.merge(final,tbl[["COMID"] + [colname1] + [x for x in finalNameList if 'Cat' in x] + [colname2] + [x for x in finalNameList if 'Ws' in x]],on='COMID')
                        else:
                            final = pd.merge(final,tbl[["COMID",colname1,colname2]],on='COMID')              
                if metricType == 'Percent':
                    lookup = pd.read_csv(metricName)                    
                    catcols,wscols = [],[]
                    for col in tbl.columns:
                        if 'CatVALUE' in col and not 'Up' in col:
                            tbl[col] = ((tbl[col] * 1e-6)/(tbl[catArea]*(tbl[catPct]/100))*100)
                            catcols.append(col)
                        if 'WsVALUE' in col:
                            tbl[col] = ((tbl[col] * 1e-6)/(tbl[wsArea]*(tbl[wsPct]/100))*100)
                            wscols.append(col)           
                    if var == 0:
                        final = tbl[frontCols+catcols + wscols]
                        final.columns = frontCols + ['Pct' + x + 'Cat' + appendMetric for x in lookup.final_val.values] + ['Pct' + y + 'Ws' + appendMetric for y in lookup.final_val.values]
                    else:
                        final2 = tbl[['COMID'] + catcols + wscols]
                        final2.columns = ['COMID'] + ['Pct' + x + 'Cat' + appendMetric for x in lookup.final_val.values] + ['Pct' + y + 'Ws' + appendMetric for y in lookup.final_val.values]
                        final = pd.merge(final,final2,on='COMID')
                        if table == 'AgMidHiSlopes':
                            final = final.drop(['PctUnknown1Cat','PctUnknown2Cat','PctUnknown1Ws', 'PctUnknown2Ws'], axis=1)
            final = final.set_index('COMID')
            if zone == '04':
                rmtbl = pd.read_csv('L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/FTP_Staging/StreamCat/Documentation/DataProcessingAndQualityAssurance/QA_Files/ProblemStreamsR04.csv')[['COMID']]
                final = final.drop(rmtbl.COMID.tolist(),axis=0)
            if zone == '06':
                stats = {}
                for c in final.columns.tolist():
                    stats[c] = {'min': final[c].min(), 'max':final[c].max()}
            if zone != '06':
                for c in final.columns.tolist():
                    if final[c].min() < stats[c]['min']:
                        stats[c]['min'] = final[c].min()
                    if final[c].max() > stats[c]['max']:
                        stats[c]['max'] = final[c].max()
            final = final.fillna('NA')
            final = final[final.columns.tolist()[:5] + [x for x in final.columns[5:] if 'Cat' in x] + [x for x in final.columns[5:] if 'Ws' in x]].fillna('NA')
            if 'ForestLossByYear0013' in table:
                final.drop([col for col in final.columns if 'NoData' in col], axis=1, inplace=True)
            if not LENGTHS[zone] == len(final):
                print "Table %s length zone %s incorrect!!!!...check Allocation\
                        and Accumulation results" % (table, zone)
            final.to_csv(outDir  + '/%s_Region%s.csv'%(table,zone))
    print table
    for stat in stats:
        print stat + ' ' + str(stats[stat])
    print 'All Done.....'

#                    if table == 'RoadStreamCrossings':
#                        finalNameList = []
#                        addName = 'SlpWtd'
#                        fnlname1 = metricName + addName + 'Cat' + appendMetric
#                        fnlname2 = metricName + addName + 'Ws' + appendMetric                         
#                        tbl[fnlname1] = tbl['Cat' + addName] / (tbl[catArea] * (tbl[catPct]/100))
#                        tbl[fnlname2] = tbl['Ws' + addName] / (tbl[wsArea] * (tbl[wsPct]/100)) 
#                        finalNameList.append(fnlname1)
#                        finalNameList.append(fnlname2) 