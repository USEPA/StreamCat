from datetime import datetime as dt
import json
import pandas as pd

KEYWORDS = [
   'inlandwaters', 'ecosystem', 'environment', 'monitoring', 'natural resources',
   'surface water', 'modeling', 'united states of america', 'united states',
   'usa', 'alabama', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut',
   'delaware', 'district of columbia', 'florida', 'georgia', 'idaho', 'illinois',
   'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana', 'maine', 'maryland',
   'massachusetts', 'michigan', 'minnesota', 'mississippi', 'missouri', 'montana',
   'nebraska', 'nevada', 'new hampshire', 'new jersey', 'new mexico', 'new york',
   'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania',
   'rhode island', 'south carolina', 'south dakota', 'tennessee', 'texas', 'utah',
   'vermont', 'virginia', 'washington', 'west virginia', 'wisconsin', 'wyoming'
]

tbl = pd.read_csv(
    "O:/PRIV/CPHEA/PESD/COR/CORFiles/Geospatial_Library_Projects"
    "/StreamCat/MetaData/submit_metadata.csv"
)

records = []

for _, row in tbl.iterrows():
    # break
    records.append(
        {'@type': "dcat:Dataset",
         'title': ("The StreamCat Dataset: Accumulated Attributes for NHDPlusV2"
             " (Version 2.1) Catchments for the Conterminous United States: "
             f"{row.title}"),
         'description': row.description,
         'keyword': KEYWORDS,
         'modified': "2015-04-23",
         'publisher':
             {'@type': 'org:Organization',
              'name': ('U.S. EPA Office of Research and Development (ORD) - '
                       'National Health and Environmental Effects Research '
                       'Laboratory (NHEERL)')
         },
         'contactPoint': {
         '@type': 'vcard:Contact',
         'fn': ('U.S. Environmental Protection Agency, Office of Research and '
             'Development-National Health and Environmental Effects Research '
             'Laboratory (NHEERL), Marc Weber'),
         'hasEmail': 'mailto:weber.marc@epa.gov'
         },
         'identifier': f"{{{row.uuid}}}",
         'accessLevel': "public",
         'bureauCode': ['020:00'],
         'programCode': ['020:072'],
         'license': "https://edg.epa.gov/EPA_Data_License.html",
         'rights': "public (Data asset is or could be made publicly available to all without restrictions)",
         'spatial': '-125.0,24.5,-66.5,49.5',
         'temporal': "2015-04-23/2015-04-23",
         'distribution': [
            {'@type': 'dcat:Distribution',
             'accessURL': ('https://www.epa.gov/national-aquatic-resource-surveys'
                           '/streamcat-dataset-0'),
             'title': 'StreamCat Dataset',
             'description': ('StreamCat currently contains over 600 metrics that '
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
                 'and agriculture) landscape information.'),
			 'format': '.csv',
             'describedBy: ('https://gaftp.epa.gov/epadatacommons/ORD/NHDPlusLandscapeAttributes/StreamCat/Documentation/DataDictionary.html'),
             'describedByType': 'text/html'
             },
            {'@type': 'dcat:Distribution',
             'downloadURL': ('https://gaftp.epa.gov/EPADataCommons/ORD'
                             '/NHDPlusLandscapeAttributes/StreamCat/HydroRegions/'),
             'format': 'Comma-Separated Values (.csv)',
             'title': row.final_table_name,
             'description': row.description,
             "mediaType": "text/csv",
             "describedBy": "https://gaftp.epa.gov/epadatacommons/ORD/NHDPlusLandscapeAttributes/StreamCat/Documentation/DataDictionary.html",
          "describedByType": "text/html"
             }
          ],
         'accrualPeriodicity': "irregular",
         'dataQuality': True,
         'describedBy': ("https://gaftp.epa.gov/EPADataCommons/ORD"
                         "/NHDPlusLandscapeAttributes/StreamCat/Documentation"
                         "/DataDictionary.html"),
         'describedByType': "text/html",
         'issued': "2015-04-23",
         'language': ['en-US'],
         'landingPage': ("https://www.epa.gov/national-aquatic-resource-surveys"
                         "/streamcat-dataset-0"),
         'references': [
             ('https://gaftp.epa.gov/EPADataCommons/ORD/'
              'NHDPlusLandscapeAttributes/StreamCat/HydroRegions/'),
             ('https://edg.epa.gov/metadata/rest/document?id='
              f'%7B{row.uuid}%7D'),
             ('https://edg.epa.gov/metadata/catalog/search/resource'
              f'/details.page?uuid=%7B{row.uuid}%7D')
         ],
         'theme': ['environment'],
        }
    )

blob = {
      "@context": 'https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld',
      "@id": "https://edg.epa.gov/data/Public/ORD/NHEERL/WED/NonGeo/StreamCat.json",
      "@type": "dcat:Catalog",
      "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
      "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
      "dataset": records,
}
with open(
        "O:/PRIV/CPHEA/PESD/COR/CORFiles/Geospatial_Library_Projects"
        "/StreamCat/MetaData/completed_metadata"
        f"/StreamCat_metadata_{dt.now().strftime('%m_%d_%Y')}.json",
        "w"
    ) as fifi:
    json.dump(blob, fifi, sort_keys=False, indent=4)


