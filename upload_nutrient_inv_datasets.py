import pandas as pd
from database import DatabaseConnection

if __name__ == '__main__':

    db_conn = DatabaseConnection(execute=True)
    db_conn.connect()
    print("Connected to DB!")
    ctrl_table = pd.read_csv('ControlTable_StreamCat.csv')
    new_metrics = ctrl_table.loc[ctrl_table['run'] == 1]

    DATASET_PATH = "/path/to/folder/with/parquets"

    
    for metric_row in new_metrics.iterrows():
        # TODO
        # Need 
        ## dsname (final table), source_name, source_url, date_downloaded, 
        ## metric_category, AOIs, years, webtool name (short_display_name)
        ## description, units, uuid, metadata

        ds_name = metric_row['FullTableName']
        metric_name = metric_row['MetricName']
        year = metric_name.split('_')[-1]
        date_downloaded = metric_row['Date Added']

        path = DATASET_PATH + metric_name + '.parquet'
        dataset_results = db_conn.CreateDatasetFromFiles('streamcat', ds_name, path, 0)
        print(dataset_results)

    
    # For sc_metrics_tg / metric definitions web page
    # Need to do this twice
    # Once for 
    #   N_Fert_Farm[Year][AOI]
    # and 
    #   P_Fert_Farm[Year][AOI]
    for metric_name in ['N_Fert_Farm[Year][AOI]', 'P_Fert_Farm[Year][AOI]']:
        metric_data = {}
        metric_data['metric_name'] = metric_name
        metric_data['indicator_category'] = 'Nutrient Inventory'
        metric_data['aoi'] = ['ws', 'cat']
        metric_data['year'] = ','.join(range(1987, 2019))
        metric_data['webtool_name'] = 'FarmFertilizer' # final_table_name
        metric_data['description'] = ''
        metric_data['units'] = ''
        metric_data['uuid'] = ''
        metric_data['metadata'] = ''
        metric_data['source_name'] = ''
        metric_data['source_url'] = ''
        metric_data['date_downloaded'] = '6-SEP-2024'
        metric_data['dsid'] = 130
        tg_results = db_conn.InsertRow('sc_metrics_tg', metric_data)
