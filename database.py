from sqlalchemy.engine import create_engine
from sqlalchemy import inspect, Table, Column, MetaData, func, insert, update, delete, select, bindparam, event, text, and_, types
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects.oracle import NUMBER
from sqlalchemy.sql.compiler import SQLCompiler
import pandas as pd
import logging
import json
from datetime import datetime
import os
import re

if not os.path.exists('logs'):
    os.makedirs('logs')

def log_query(conn, clauseelement, multiparams, params, execution_options):
    # Log all non-SELECT SQL commands before execution
     if not clauseelement.is_select:
        if len(params) > 0 and len(multiparams) > 0:
            combined_params = {}
            if multiparams:
                combined_params.update(multiparams[0] if isinstance(multiparams, list) and multiparams else multiparams)
            combined_params.update(params or {})
            
            # Compile the SQL statement with the current dialect and substitute bind parameters
            compiled_sql = str(clauseelement.compile(dialect=conn.dialect, params=combined_params))
        else:
            compiled_sql = str(clauseelement.compile(dialect=conn.dialect, compile_kwargs={"literal_binds": True}))
        
        # Log the compiled SQL statement to a file
        with open(f'db_updates_{datetime.today().strftime("%m_%d_%Y")}.sql', 'a') as f:
            f.write(compiled_sql + ';\n')

class DatabaseConnection():
    def __init__(self, execute=False, database_dir="O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/DatabaseModification", database_config_file="streamcat_db_config.json") -> None:

        if database_dir and database_config_file:
            config_file_path = os.path.join(database_dir, database_config_file)
            fp = open(config_file_path)
            config_file = json.load(fp)
            self.dialect = config_file['dialect']
            self.driver = config_file['driver']
            self.username = config_file['username']
            self.password = config_file['password']
            self.host = config_file['host']
            self.port = config_file['port']
            self.service = config_file['service']
            fp.close()
        
        self.execute = execute
        
        self.engine = None
        self.metadata = None

    def __str__(self) -> str:
        """Create Database connection string"""
        return f"{self.dialect}+{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/?service_name={self.service}"
        
    def __del__(self):
        """Safely close engine on exit"""
        if self.engine:
            self.engine.dispose()
    
    def connect(self):
        """Connect to database"""
        if self.engine is None:
            self.engine = create_engine(self.__str__(), thick_mode={'lib_dir' : 'O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/DatabaseModification/instantclient-basic-windows/instantclient_23_4'}, logging_name="StreamCatDB")
            self.inspector = inspect(self.engine)
            self.metadata = MetaData()
            self.metadata.reflect(self.engine)
            os.makedirs('logs', exist_ok=True) # make logs dir if it doesn't exists
            logging.basicConfig(filename=f'logs/db_log_{datetime.today().strftime("%m_%d_%Y")}.log', filemode='a')
            logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)
            event.listen(self.engine, 'before_execute', log_query)
        return
    
    def disconnect(self):
        """Disconnect from database"""
        if self.engine:
            self.engine.dispose()
        return


    def RunQuery(self, query, params=None):
        """Execute query with given params. 
        If self.execute is true the query will be executed and autocommitted
        Otherwise it will print the compiled query to stdout and write it to a file.
        Filename db_updates_m_d_y.sql

        Args:
            query (SQL Alchemy executable): An executable sqlalchemy query. This is either a text() function or any core select, update, insert, delete function
            params (list[dict] | dict, optional): Parameters required for query. Dict if it is a single line query or list[dict] if multiline query. Defaults to None.

        Returns:
            result: compiled sql statement or execution results
            self.execute: whether or not the query was executed
        """
        #print(str(query))
        if self.execute == True:
            Session = sessionmaker(self.engine)
            with Session.begin() as conn:
                result = conn.execute(query) # removed params
                conn.commit()
        else:
            if isinstance(params, list):
                compiled_queries = []
                for param_set in params:
                    compiler = SQLCompiler(self.engine.dialect, query, compile_kwargs={"literal_binds": True})
                    compiler.process_parameters(param_set)
                    compiled_queries.append(str(compiler))
                result = '\n'.join(compiled_queries)
            elif isinstance(params, dict):
                compiler = SQLCompiler(self.engine.dialect, query, compile_kwargs={"literal_binds": True})
                compiler.process_parameters(params)
                result = str(compiler)
            else:
                result = str(query.compile(dialect=self.engine.dialect, compile_kwargs={"literal_binds": True}))

            #print(result)
            with open(f'db_updates_{datetime.today().strftime("%m_%d_%Y")}.sql', 'a') as db_file:
                db_file.write(result + ';\n')
        return result, self.execute # Return statement and whether or not it was executed
    
    def SelectColsFromTable(self, columns: list, table_name:str, function:None | dict = None):
        """Select columns from database table 

        Args:
            columns (list): columns to be selected
            table_name (str): name of db table
            function (None | dict, optional): Function to apply to query. Key will be the type of function to apply to the query, key is column name for function if applicable.
                                              Options are 'distinct', 'count', 'max', 'min', 'avg', 'groupby', 'orderby'. Defaults to None.

        Returns:
            result (sequence of rows): Result set of query
        """
        if len(columns) == 1:
            col_str = columns[0]
        else:
            col_str = ','.join(columns)
        if function is not None:
            if 'distinct' in function.keys():
                query = text(f"SELECT DISTINCT({col_str}) FROM {table_name}")
            elif 'count' in function.keys():
                col_str += f'COUNT({function['count']})' #TODO reorganize function to do something like this then add FROM then do any function that would happen after FROM like groupby and orderby
                query = text(f"SELECT {col_str} FROM {table_name}")
            elif 'orderby' in function.keys():
                query = text(f"SELECT {col_str} FROM {table_name} ORDER BY {function['orderby']}")
        else:
            query = text(f"SELECT {col_str} FROM {table_name}")
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchall()

        # parsed_res = []
        # for row in result:
        #     parsed_res.append(row._asdict())


        return result
    
    def TextSelect(self, text_stmt):
        """Select query using the SQLAlchemy text() function

        Args:
            text_stmt (str | Text): String query to pass to database

        Returns:
            Sequence[Row]: Return set from SQL select statement
        """
        if isinstance(text_stmt, str):
            text_stmt = text(text_stmt)

        with self.engine.connect() as conn:
            result = conn.execute(text_stmt).fetchall()
        
        return result


    
    def GetTableAsDf(self, table_name: str) -> pd.DataFrame | str:
        """Get database table by name as pandas Dataframe

        Args:
            table_name (str): Name of table to view

        Returns:
            pd.Dataframe: Database table
            str: If table not found by name return err string
        """
        if self.inspector.has_table(table_name):
            return pd.read_sql_table(table_name, self.engine)
        else:
            return f"No table found named {table_name} in database. Check log file for details."

    def GetTableSchema(self, table_name: str) -> str:
        """Get schema of given table. Equivilent to SQL statement: DESC table;

        Args:
            table_name (str): Name of db table

        Returns:
            str: Description of db table schema if found, or error string on failure
        """
        if self.inspector.has_table(table_name):
            metadata_obj = MetaData()
            table = Table(table_name, metadata_obj, autoload_with=self.engine)
            return str(CreateTable(table))
        else:
            return f"Table - {table_name} not found."
        
    def CreateNewTable(self, table_name: str, data: pd.DataFrame) -> bool:
        """Create new table in database

        Args:
            table_name (str): new table name
            data (pd.DataFrame): dataframe used to define columns and initialize data

        Returns:
            bool: True if table created and data is inserted; false otherwise.
        """
        if self.inspector.has_table(table_name):
            print(f"Table - {table_name} already exists")
            return False
        columns = []
        for col_name, col_type in zip(data.columns, data.dtypes):

            #TODO check csv files (as dataframes) to see their types so we can properly map them to the NUMBER type using types.Numeric()
            # add condition for COMID types that if col_name == 'comid' that column class will need primary_key=True
            # if no comid row use col[0] as primary key
            if (col_type.name == 'int64' or col_type.name == 'float64') and col_name.lower() == 'comid':
                col = Column(col_name, types.Numeric(), primary_key=True)
            elif col_type.name == 'int64' or col_type.name == 'float64':
                col = Column(col_name, types.Numeric())
            elif col_type.name == 'bool':
                col = Column(col_name, types.Boolean())
            elif col_type.name == 'datetime64[ns]':
                col = Column(col_name, types.DateTime())
            else:
                col = Column(col_name, types.VARCHAR())
            #TODO change the columns to nullable = true maybe. Also this should be for dataset tables only new function non dataset tables

            columns.append(col)

        new_table = Table(table_name, self.metadata, *columns)
        new_table.create(self.engine, checkfirst=True)
        result = self.BulkInsert(table_name, data.to_dict(orient='records'))
        if result:
            return True
        
    def CreateTableFromFile(self, table_name, file_path):
        df = pd.read_csv(file_path)
        result = self.CreateNewTable(table_name, df)
        if result:
            return True

        

    def getMaxDsid(self, partition: str):
        """Get highest dsid in db

        Args:
            partition (str): Either 'lakecat' or 'streamcat' so we know which datasets table to query for maximum

        Returns:
            max_dsid (int): largest dsid for given partition
        """
        table_name = 'lc_datasets' if partition == 'lakecat' else 'sc_datasets'
        ds_table = Table(table_name, self.metadata, autoload_with=self.engine)
        with self.engine.connect() as conn:
            max_dsid = conn.execute(func.max(ds_table.c.dsid)).scalar()
            conn.rollback()
        return max_dsid

    def InsertRow(self, table_name: str, values: dict):
        """Insert row into db table

        Args:
            table_name (str): Name of table to insert into
            values (dict): dictionary with items key = column_name : value = new_value
        """
        
        if self.inspector.has_table(table_name):
            table = self.metadata.tables[table_name]
            query = insert(table).values(values).returning(*table.c)
            result, executed = self.RunQuery(query)
            if executed:
                return result.fetchall()
            else:
                return result
    
    # TODO finish dynamic bindings
    # change old value and new_value to pd.Series or sqlalchemy Column
    # Update all items in these series
    def UpdateRow(self, table_name: str, column: str, id_column: str, id_val: str, new_value: str):
        """Update Row in database

        Args:
            table_name (str): Name of database table
            column (str): Name of column to update
            id (str): Identifier for row
            new_value (str): New value to set column equal to where the identifier is found

        Returns:
            result (Result): return result of the update query
        """
        # if has_table is false call create table
        if self.inspector.has_table(table_name):
            table = self.metadata.tables[table_name]
            #col = table.c.get(column)
            # if col == None:
            #     return f"No Column named {column} in Table {table_name}"
            
            id_col = table.c.get(id_column)
            if id_col is None:
                return f"No Column named {column} in Table {table_name}"
            #query = update(table).where(col == bindparam("id")).values(new_value=bindparam("new_value"))
            query = update(table).where(id_col == id_val).values({column: new_value})
            result, executed = self.RunQuery(query)
            if executed:
                return result.fetchall()
            else:
                return result
    

    # TODO add confirmation
    def DeleteRow(self, table_name, id):
        if self.inspector.has_table(table_name):
            table = self.metadata.tables[table_name]
            query = delete(table).where(id == bindparam("id"))# .returning(id)
            params = {"id" : id}
            result, executed = self.RunQuery(query, params)
            if executed:
                return result.fetchall()
            else:
                return result

    
    def BulkInsert(self, table_name, data):
        """Bulk insert multiple rows of data into database table

        Args:
            table_name (str): Name of table to insert data into
            data (list[dict]): list of dictionary items where each item is defined as key = column_name : value = new_value

        Returns:
            result : compiled queries 
        """
        if self.inspector.has_table(table_name):
            table = self.metadata.tables[table_name]
            results = []
            for row_data in data:
                insert_query = (
                    table.insert().values(row_data).returning(*table.c)
                )
                result, executed = self.RunQuery(insert_query)
                
                if executed:
                    results.extend(result.fetchall())
                else:
                    results.append(result)
        
            return results
        
        
    def BulkInsertFromFile(self, table_name, file_path):
        df = pd.read_csv(file_path)
        data = df.to_dict(orient='records')
        results = self.BulkInsert(table_name, data)
        if results:
            return results

        
    
    def BulkUpdateDataset(self, table_name, data):
        """Bulk update multiple rows of data in a database table.

        Args:
            table_name (str): Name of table to update data in.
            data (list[dict]): List of dictionary items where each item is defined as
                            key = column_name : value = new_value, including the primary key 'comid'.

        Returns:
            results: List of compiled queries or execution results.
        """
        if self.inspector.has_table(table_name):
            table = self.metadata.tables[table_name]
            
            results = []
            for row_data in data:
                # Ensure 'comid' exists in the row_data
                if 'comid' not in row_data:
                    raise ValueError("Each row must include the primary key 'comid'")
                
                # Remove 'comid' from the dictionary to use as update values
                comid = row_data.pop('comid')
                
                # Create an update statement
                update_query = (
                    table.update()
                    .where(table.c.comid == comid)
                    .values(row_data)
                    # .returning(*table.c)
                )
                
                # Compile the query and execute or return the compiled query based on self.execute
                result, executed = self.RunQuery(update_query)
                
                if executed:
                    results.extend(result.fetchall())
                else:
                    results.append(str(update_query.compile(dialect=self.engine.dialect)))
            
            return results
        
    def BulkUpdate(self, table_name, data):
        """Bulk update multiple rows of data in a database table with dynamic WHERE conditions.

        Args:
            table_name (str): Name of table to update data in.
            data (list[dict]): List of dictionaries where each dictionary has two keys:
                            'update_values': dict of column_name: new_value pairs for update.
                            'conditions': dict of column_name: condition_value pairs for WHERE clause.

        Returns:
            results: List of compiled queries or execution results.
        """

        """data example
        data = [
            {
                'update_values': {'column1': 'new_value1', 'column2': 'new_value2'},
                'conditions': {'metricname': oldname, 'status': 'active'}
            },
            {
                'update_values': {'column3': 'new_value3'},
                'conditions': {'id': 2}
            }
        ]
        """
        if self.inspector.has_table(table_name):
            table = self.metadata.tables[table_name]
            
            results = []
            for row_data in data:
                # Ensure 'update_values' and 'conditions' exist in the row_data
                if 'update_values' not in row_data or 'conditions' not in row_data:
                    raise ValueError("Each row must include 'update_values' and 'conditions'")
                
                update_values = row_data['update_values']
                conditions = row_data['conditions']

                # Create a WHERE clause from the conditions
                where_clause = and_(*[getattr(table.c, col) == val for col, val in conditions.items()])
                
                # Create an update statement
                update_query = (
                    table.update()
                    .where(where_clause)
                    .values(update_values)
                    # .returning(*table.c)
                )
                
                # Compile the query and execute or return the compiled query based on self.execute
                result, executed = self.RunQuery(update_query)
                
                if executed:
                    results.extend(result.fetchall())
                else:
                    results.append(result) # str(update_query.compile(dialect=self.engine.dialect))
            
            return results


    def CreateDataset(self, partition: str, df: pd.DataFrame, dsname: str, active: int = 0):
        """Create new dataset table from pandas dataframe. This will also insert the new metrics into our metric informatio tables, _metrics, _metrics_display_names and _metrics_tg.

        Args:
            partition (str): IMPORTANT: this needs to be either 'streamcat' or 'lakecat'. This is how we will decide what part of the database to create new data in.
            df (pd.DataFrame): Dataframe to upload to database as table
            dsname (str): New dataset name, defaults to csv name.
            active (int): binary int 1 if dataset will be published and displayed upon creation. 0 if not. Default is 0


        Returns:
            ds_result (tuple): new dataset table name and dataset name inserted into 
            metric_result: Rows inserted into _metrics table
            display_result: Rows inserted into _display_names table
        """
        
        if partition.lower() == 'streamcat':
            prefix = 'sc_'
        elif partition.lower() == 'lakecat':
            prefix = 'lc_'
        else:
            ValueError("Invalid partition! Needs to be either streamcat or lakecat")
        
        dsid = self.getMaxDsid(partition) + 1
        table_name = prefix + 'ds_' + str(dsid)
        if self.execute:
            # Change this to sqlalchemy CreateTable function called self.CreateNewTable
            #self.CreateNewTable(table_name, df)
            revert_columns = {}
            new_col_names = {}
            dtypes = {}
            for col_name in df.columns:
                dtypes[col_name.upper()] = NUMBER
                new_col_names[col_name] = col_name.upper()
                revert_columns[col_name.upper()] = col_name

            df.rename(columns=new_col_names, inplace=True)
            df.to_sql(table_name, self.engine, if_exists='replace', chunksize=10000, dtype=dtypes, index=False)
            df.rename(columns=revert_columns, inplace=True)
        else:
            # IF execute is false then we just write the raw sql queries to a file
            lines = []
            column_names = ', '.join(df.columns)
            base_query = f"INSERT INTO {table_name} ({column_names})"
            for idx, row in df.iterrows():
                values = ', '.join([f"{str(value)}" for value in row])
                line = base_query + f"({values});\n"
                lines.append(line)
            with open(f"create_{table_name}.sql", 'w') as f:
                f.writelines(lines)
        
        
        # Insert dataset info into sc / lc datasets
        ds_result = self.InsertRow(f'{prefix}datasets', {'dsid': dsid, 'dsname': dsname, 'tablename': table_name, 'active': active})

        display_names = set()
        metric_data = []
        display_params = []
        metrics_table_name = prefix + 'metrics'
        for metric in df.columns:
            if metric not in ['COMID', 'CatAreaSqKm', 'WsAreaSqKm', 'CatPctFull', 'WsPctFull', 'inStreamCat']:

                # get list of params to pass to one query
                params = {"dsname": dsname, "metricname": metric, "dsid": dsid}
                metric_data.append(params)

                # Add to metrics display names
                metric_name = metric.lower()
                if "rp100" in metric_name:
                    metric_name = metric_name.removesuffix("rp100")
                if "cat" in metric_name:
                    metric_name = metric_name.removesuffix("cat")
                if "ws" in metric_name:
                    metric_name = metric_name.removesuffix("ws")
                display_names.add(metric_name)

        # Insert into sc/lc _ metrics
        metric_result = self.BulkInsert(metrics_table_name, metric_data)
            
        
        display_table_name = prefix + 'metrics_display_names'
        for alias in display_names:
            # get list of params to pass to one query
            display_params.append({"metric_alias": alias, "dsid": dsid})
        
        # Insert into sc/lc _metrics_display_names
        display_result = self.BulkInsert(display_table_name, display_params)

        return ds_result, metric_result, display_result
    
    def CreateDatasetFromFiles(self, partition: str, dataset_name: str, files: list | str, active: int = 0):
        """Create new dataset in given partition from a list of files

        Args:
            partition (str): IMPORTANT: this needs to be either 'streamcat' or 'lakecat'. This is how we will decide what part of the database to create new data in.
            dataset_name (str): Name of the dataset. This will be used in the sc/lc_datasets table and is also the final_table name in the TG table (metric variable info page)
            files (list | str): list of paths to files
            active (int): binary int 1 if dataset will be published and displayed upon creation. 0 if not. Default is 0

        Returns:
            tuple(dataset_result, metric_result, display_result): see function CreateDataset
        """
        if ',' in files and isinstance(files, str):
            files = files.split(', ')
        
        if isinstance(files, list):
            dfs = [pd.read_csv(path) for path in files]
            df = pd.concat(dfs)
            # dsname = files[0].split('/')[-1].removesuffix('.csv')
        else:
            df = pd.read_csv(files)
            # dsname = files.split('/')[-1].removesuffix('.csv')

        # if '_' in dsname:
        #     dsname = dsname.split('_')[0]

        df.fillna(0, inplace=True)
        
        ds_result, metric_result, display_result = self.CreateDataset(partition, df, dataset_name, active)
        return ds_result, metric_result, display_result

    def FindAllMetrics(self, partition: str) -> list:
        """Get all metrics in given database partition

        Args:
            partition (str): IMPORTANT: this needs to be either 'streamcat' or 'lakecat'. This is how we will decide what part of the database to create new data in.

        Returns:
            list: all metrics found in dataset tables
        """
        full_available_metrics = []
        if partition.lower() == 'streamcat':
            prefix = 'sc_ds'
        elif partition.lower() == 'lakecat':
            prefix = 'lc_ds'
        else:
            ValueError("Invalid partition, needs to be either streamcat or lakecat")

        for table in self.metadata.sorted_tables:
            if 'sc_ds' in table.name:
                columns = table.columns
                # print(columns.keys())
                for col in columns.keys():
                    col_name = col.lower()
                    full_available_metrics.append(col_name)
        return full_available_metrics

    def FindMissingMetrics(self):
        full_available_metrics = self.FindAllMetrics()
        names = pd.read_sql('SELECT metricname FROM SC_METRICS', con=self.engine)
        current_metric_names = names['metricname'].apply(lambda x: str(x).lower())
        missing_from_sc_metrics = set(full_available_metrics) - set(current_metric_names)
        return ''.join(missing_from_sc_metrics) # could return just the set as well

        
    def GetMetricsInTG(self) -> pd.Series:
        def get_combinations(row):
            combinations = []
            # If metric name contains [Year] or [AOI] replace with row['year'] or row['aoi'l
            if row['year'] is not None:
                combinations = [(y, a) for y in row['year'] for a in row['aoi']]
            return combinations

        def get_full_metric_list(row):
            metrics = []
            if row['year'] is None:
                for aoi in row['aoi']:
                    name = str(row['metric_name'])
                    new_name = name.replace("[AOI]", aoi)
                    # find = name.find("[AOI]")
                    # print(name, find)
                    metrics.append(new_name)
            if row['aoi'] is None:
                for year in row['year']:
                    name = row['metric_name']
                    new_name = name.replace('[Year]', year)
                    metrics.append(new_name)
            if len(row['combinations']) > 0:
                for combo in row['combinations']:
                    name = row['metric_name']
                    new_name = name.replace('[Year]', combo[0]).replace('[AOI]', combo[1])
                    metrics.append(new_name)
            return metrics
        
        df = self.RunQuery("SELECT metric_name, aoi, year FROM SC_METRICS_TG WHERE indicator_category <> 'Base' ORDER BY metric_name ASC")
        
        # Extract individual years and AOIs
        df['year'] = df['year'].str.split(', ')
        df['aoi'] = df['aoi'].str.split(', ')

        # Generate all combinations of year and AOI
        # combinations = [(y, a) for y in df['year'].iloc[0] for a in df['aoi'].iloc[0]]
        df['combinations'] = df.apply(get_combinations, axis=1)

        # Create a new column with the desired format
        df['full_list'] = df.apply(get_full_metric_list, axis=1)

        # Turn column into a series
        full_tg_list = df['full_list'].explode()
        full_tg_list = full_tg_list.apply(lambda x: str(x).lower())

        return full_tg_list

    # def RemoveAoiFromRow(row):
    #     aois = ('cat', 'ws', 'catrp100', 'wsrp100')
    #     for aoi in aois:
    #         if row.endswith(aoi):
    #             return row[:-len(aoi)].strip()
    #     return row

    def GetAllDatasetNames(self):

        sc_dsnames = []
        lc_dsnames = []
        sc_select_stmt = text("SELECT dsname FROM sc_datasets")
        sc_datasets_res = self.TextSelect(sc_select_stmt)
        for row in sc_datasets_res:
            sc_dsnames.append(row._t[0])

        lc_select_stmt = text("SELECT dsname FROM lc_datasets")
        lc_datasets_res = self.TextSelect(lc_select_stmt)
        for row in lc_datasets_res:
            lc_dsnames.append(row._t[0])
        
        return sc_dsnames + lc_dsnames
    
    def UpdateMetricName(self, partition, old_name, new_name):
        # call this in the edit metric info fucntion if name is updated. 
        prefix = 'sc' if partition == 'streamcat' else 'lc'
        old_name_prefix = old_name.split('[')[0]
        new_name_prefix = new_name.split('[')[0]
        tg_query_result = self.TextSelect(text(f"SELECT metric_name, aoi, year, dsid FROM {prefix}_metrics_tg WHERE metric_name = '{old_name}'"))
        tg_info = {}
        for row in tg_query_result:
            tg_info = row._asdict()
            tg_info['name_prefix'] = tg_info['metric_name'].split('[')[0]
            tg_info['aoi'] = tg_info['aoi'].split(', ') if ',' in tg_info['aoi'] else [tg_info['aoi']]
            if tg_info['year'] is not None:
                tg_info['year'] = tg_info['year'].split(', ') if ',' in tg_info['year'] else [tg_info['year']]
            else: 
                tg_info['year'] = ['']

        # sc/lc_metrics_tg update
        tg_result = self.UpdateRow(f'{prefix}_metrics_tg', 'metric_name', 'metric_name', old_name, new_name)
        # sc/lc_metrics update
        # concat name_prefix and all the aois
        metric_result = []
        
        for year in tg_info['year']:
            for aoi in tg_info['aoi']:
                if '[Year]' in old_name:
                    old_metric_name = old_name_prefix + year + aoi
                else: 
                    old_metric_name = old_name_prefix + aoi

                if '[Year]' in new_name:
                    new_metric_name = new_name_prefix + year + aoi 
                else:
                    new_metric_name = new_name_prefix + aoi

                metric_res = self.UpdateRow(f'{prefix}_metrics', 'metricname', 'metricname', old_metric_name, new_metric_name)
                metric_result.append(metric_res)

        # sc/lc_metrics_display_names update
        display_query = self.TextSelect(text(f"SELECT metric_alias FROM {prefix}_metrics_display_names WHERE metric_alias LIKE '%{old_name_prefix.lower()}%'"))
        display_result = []
        for row, year in zip(display_query, tg_info['year']):
            old_display_name = row._t[0].lower()
            new_display_name = new_name_prefix.lower() + year
            display_update = self.UpdateRow(f'{prefix}_metrics_display_names', 'metric_alias', 'metric_alias', old_display_name, new_display_name)
            display_result.append(display_update)


        # Need to make full metric name array then use those to update the column names
        dataset_table_name = f'{prefix}_ds_{tg_info['dsid']}'
        dataset_table = self.metadata.tables[dataset_table_name]
        alter_table_results = []
        for year in tg_info['year']:
            for aoi in tg_info['aoi']:
                if '[Year]' in old_name:
                    old_col_name = old_name_prefix + year + aoi
                else:
                    old_col_name = old_name_prefix + aoi
                if '[Year]' in new_name:
                    new_col_name = new_name_prefix + year + aoi 
                else:
                    new_col_name = new_name_prefix + aoi
                if old_col_name.lower() in dataset_table.columns.keys():
                    print(f"Need to update dataset {dataset_table_name}")
                    alter_table_results.append(self.UpdateColumnName(dataset_table_name, old_col_name.lower(), new_col_name.lower()))
        
        return metric_result, display_result, tg_result, alter_table_results
    
    def UpdateColumnName(self, table_name, old_col, new_col):
        if self.inspector.has_table(table_name):
            alter_stmt = f'ALTER TABLE {table_name} RENAME COLUMN "{old_col}" TO "{new_col}"'
            alter_query = text(alter_stmt)
            result = self.RunQuery(alter_query)
            return result[0]
        else:
            return f"No table named {table_name} found."

    
    def UpdateDatasetColumn(self, table_name: str, col_name: str, values: list[dict]):
        """
        values should be dictionary mapping of comid to new updated value
        example:[ {"comid": comid_1, "new_value": new_value_1}, {"comid": comid_2, "new_value": new_value_2}]
        
        query: UPDATE table_name SET :column = :new_value WHERE :comid = comid
        """
        
        if self.inspector.has_table(table_name):
            query = text(f"UPDATE {table_name}, SET {col_name} = :new_value WHERE comid = :comid")
            
            exec = self.RunQuery(query, values)
            return exec


    def UpdateActiveDataset(self, dsname, partition):
        if partition == 'both':
            pass
            # TODO perform join + 2 updates
            sql = text(f"SELECT l.active, s.active FROM sc_datasets s JOIN lc_datasets l ON s.dsid = l.dsid WHERE s.dsname = '{dsname}'")
            res = self.TextSelect(sql)
            for row in res:
                new_val = 1 if 0 in row._t else 0
            
        else:
            prefix = 'sc' if partition == 'streamcat' else 'lc'
            ds_table_name = f'{prefix}_datasets'
            sql = text(f"SELECT active, tablename FROM {ds_table_name} WHERE dsname = '{dsname}'")
            res = self.TextSelect(sql)
            print(res[0]._t)
            active = res[0]._t[0]

            new_val = 0 if active == 1 else 1
        if new_val == 1:
            # check dataset for null values
            
            dataset_table = res[0]._t[1]
            table_df = self.GetTableAsDf(dataset_table)
            if table_df.isnull().any(bool_only=True):
                return f"Found null values found in dataset {dsname}. Fix this and try again."
        update_stmt = self.UpdateRow(ds_table_name, 'active', 'dsname', dsname, new_val)
        update_res = self.RunQuery(update_stmt)
        return update_res
    

    def getVersionNumber(self, partition): 
        table_name = 'lc_info' if partition == 'lakecat' else 'sc_info'
        info_table = Table(table_name, self.metadata, autoload_with=self.engine)
        with self.engine.connect() as conn:
            max_version = conn.execute(func.max(info_table.c.version)).scalar()
            conn.rollback()
        return max_version
    
    def newChangelogRow(self, partition, public_desc, change_desc):
        table_name = 'sc_info' if partition == 'streamcat' else 'lc_info'
        # stmt = f"INSERT INTO {table_name} (version, public_description) VALUES ((SELECT MAX(version)+1 FROM lc_info), '{public_desc}');"
        new_version_num = self.getVersionNumber(partition) + 1
        values = {
            "version": new_version_num,
            "public_description": public_desc,
            "change_description": change_desc
        }
        result = self.InsertRow(table_name, values)
        return result