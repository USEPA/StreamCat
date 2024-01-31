from datetime import datetime as dt
import json
import pandas as pd


title = 'The StreamCat Dataset: Accumulated Attributes for NHDPlusV2 (Version 2.1) Catchments for the Conterminous United States: '

KEYWORDS =['inlandwaters', 'ecosystem', 'environment', 'monitoring', 'natural resources',
   'surface water', 'modeling', 'united states of america', 'united states',
   'usa', 'alabama', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut',
   'delaware', 'district of columbia', 'florida', 'georgia', 'idaho', 'illinois',
   'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana', 'maine', 'maryland',
   'massachusetts', 'michigan', 'minnesota', 'mississippi', 'missouri', 'montana',
   'nebraska', 'nevada', 'new hampshire', 'new jersey', 'new mexico', 'new york',
   'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania',
   'rhode island', 'south carolina', 'south dakota', 'tennessee', 'texas', 'utah',
   'vermont', 'virginia', 'washington', 'west virginia', 'wisconsin', 'wyoming']

organization = 'U.S. Environmental Protection Agency, Office of Research and Development (ORD), Center for Public Health and Environmental Assessment (CPHEA), Pacific Ecological Systems Division (PESD), '
temporal = '2015/2030'

StreamCat = ('StreamCat currently contains over 600 metrics that '
    'include local catchment (Cat), watershed (Ws), and special '
    'metrics. The special metrics were derived through modeling or '
    'by combining other StreamCat metrics. These variables include '
    'predicted water temperature, predicted biological condition, '
    'and the indexes of catchment and watershed integrity. See '
    'Geospatial Framework and Terms below for definitions of '
    'catchment and watershed as used with the StreamCat Dataset.'
    '\n\nThese metrics are available for ~2.65 million stream '
    'segments and their associated catchments across the '
    'conterminous US. StreamCat metrics represent both natural '
    '(e.g., soils and geology) and anthropogenic (e.g, urban areas '
    'and agriculture) landscape information.')


describeBy = 'https://www.epa.gov/national-aquatic-resource-surveys/streamcat-metrics-and-definitions'
access = "https://www.epa.gov/national-aquatic-resource-surveys/streamcat-dataset"
gaft = 'https://gaftp.epa.gov/EPADataCommons/ORD/NHDPlusLandscapeAttributes/StreamCat/HydroRegions/'


tbl = pd.read_csv(
    "O:/PRIV/CPHEA/PESD/COR/CORFiles/Geospatial_Library_Projects"
    "/StreamCat/MetaData/submit_metadata_StreamCat.csv" 
)

records = []

for _, row in tbl.iterrows():
    # break
    records.append(
        {'@type': "dcat:Dataset",
         'title': title + row.title,                                           #made title variable
         'description': row.description,
         'keyword': KEYWORDS,
         'modified': dt.now().strftime('%Y-%m-%d'),
         'publisher':
             {'@type': 'org:Organization',
              'name': organization                                             # made organization variable
         },
         'contactPoint': {
         '@type': 'vcard:Contact',
         'fn': organization + row.contact,                                     # made 'contact' variable and fill "Marc Weber"
         'hasEmail': ('mailto:' + row.contact_Email)                           # made 'contact_Email' variable and fill weber.marc@epa.gov
         },
         'identifier': row.uuid,                                               # got rid of f"{{{row.uuid}}}" in favor of row.uuid
         'accessLevel': "public",
         'bureauCode': ['020:00'],
         'programCode': ['020:072'],
         'license': "https://edg.epa.gov/EPA_Data_License.html",
         'rights': "public (Data asset is or could be made publicly available to all without restrictions)",
         'spatial': '-125.0,24.5,-66.5,49.5',
         'temporal': temporal,
         'distribution': [
            {'@type': 'dcat:Distribution',
             'accessURL': access,                                              #access variable created
             'title': 'StreamCat Dataset',
             'description': StreamCat,                                         #made StreamCat a variable to make cleaner
			 'format': 'API',                                                  #changed to API
             'describedBy': describeBy,                                        # made URL variable
             'describedByType': 'text/html'
            },
            {'@type': 'dcat:Distribution',
             'downloadURL': gaft,                                              #gaft variable 
             'format': 'Comma-Separated Values (.csv)',
             'title': row.final_table_name,
             'description': row.description,
             "mediaType": "text/csv",
             "describedBy": describeBy,
          "describedByType": "text/html"
             }
          ],
         'accrualPeriodicity': "irregular",
         'dataQuality': True,
         'describedBy': describeBy,
         'describedByType': "text/html",
         'issued': "2015-04-23",
         'accrualPeriodicity': "R/P3Y",
         'language': ['en-US'],
         'landingPage': access,                                                #   
         'references': [
             gaft,                                                             #
             ('https://edg.epa.gov/metadata/catalog/search/resource/details.page?uuid=' + row.uuid)  ##Removed f'{{{row.uuid}}} 
         ],
         'theme': ['environment'],
        }
    )

blob = {
      "@context": 'https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld',
      "@id": "https://edg.epa.gov/data/Public/ORD/NHEERL/WED/NonGeo/StreamCat.json",
      "@type": "dcat:Catalog",
      "conformsTo": "https://project-open-data.cio.gov/v1.1/schema", #check validation in open
      "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
      "dataset": records,
}
with open(
        "O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/MetaData/completed_metadata"
        f"/StreamCat_metadata_{dt.now().strftime('%m_%d_%Y')}.json", 
        "w"
    ) as fifi:
    json.dump(blob, fifi, sort_keys=False, indent=4,)          

                                                                   
