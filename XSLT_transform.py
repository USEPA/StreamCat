## Needs to be run in the 32-bit python environment installed with ArcGIS C:\Python27\ArcGIS10.3
##Make sure that the workspace has all of the xmls you want to transform as well as the FGDC Plus.xsl file in that location
import os, sys, arcpy
from arcpy import env

work_dir = "L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat/FTP_Staging/StreamCat/Documentation/Metadata/XMLs"
env.workspace = work_dir
html_dir = "L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat/FTP_Staging/StreamCat/Documentation/Metadata"
xslt = "L:/Priv/CORFiles/Geospatial_Library/Data/Project/StreamCat/Metadata/FGDC Plus.xsl"
for xml in arcpy.ListFiles("*.xml"):
    html = html_dir + '/' + xml.split(".")[0] + '.html'    
    if not os.path.exists(html):
        print html
        arcpy.XSLTransform_conversion(xml, xslt, html)