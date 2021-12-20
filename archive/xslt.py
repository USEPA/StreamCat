import arcpy
outs = "L:/Priv/CORFiles/Geospatial_Library_Projects/StreamCat/FTP_Staging/Documentation/Metadata"
xmls = "L:/Priv/CORFiles/Geospatial_Library_Projects/StreamCat/FTP_Staging/Documentation/Metadata/XMLs"
trans = "L:/Priv/CORFiles/Geospatial_Library_Projects/StreamCat/FTP_Staging/Documentation/Metadata/XMLs/XSLT_transform/FGDC Plus.xsl"
arcpy.XSLTransform_conversion(xmls + "/sw_flux.xml", trans, outs + "/sw_flux.html")
