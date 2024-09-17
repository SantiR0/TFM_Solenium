import os
import json
import pandas as pd
import datetime as dt
import logging


from pyspark.sql import functions as F
from pyspark.sql.functions import from_utc_timestamp, date_format
from delta.tables import DeltaTable
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType, FloatType, StringType, BooleanType, TimestampType, DataType,  StructType, StructField

class MedallionConfig:
    """
    A class to configure and manage environment, ingestion, and connector settings for the medallion architecture (Bronze, Silver, Gold layers).
    
    Attributes:
        ingest_name (str): The name of the ingestion process.
        spark (SparkSession): Spark session used for querying and data manipulation.
    """

    env_conf_path        = '/dbfs/FileStore/parameters/environment/environment_conf.json'
    ingestion_conf_path  = '/dbfs/FileStore/parameters/ingestion'
    connectors_conf_path = '/dbfs/FileStore/parameters/connectors'

    def __init__(self, ingest_name):
        """
        Initializes MedallionConfig with ingestion name and loads environment, storage, and connector configurations.

        Parameters:
        ingest_name: str
            The name of the ingestion process.
        """
        self.ingest_name = ingest_name
        self.spark = SparkSession.builder.getOrCreate()

        self._load_env_conf()
        

        self._ingestion_conf()
        self._connector_conf()
        self._set_medallion_path()
    
    def _load_env_conf(self):
        """
        Loads the environment configuration from a JSON file (located in dbfs) and sets environment variables.

        Raises:
        Exception
            If there is an issue loading the environment configuration file.
        """
        try:
            with open(self.env_conf_path, "r") as file:
                env_file = json.load(file)
        except Exception as e:
            raise Exception(f"Error loading environment medallion configuration file: {str(e)}")

        self._storage_account_name       = env_file.get('storage_account_name')
        self._storage_account_access_key = env_file.get('storage_account_access_key')
        self.silver_format              = env_file.get('silver_format')
        self.gold_format                = env_file.get('gold_format')
        

    def _set_medallion_path(self):
        """
        Sets the paths for the Bronze, Silver, and Gold layers based on the storage account name and table.
        """
        self.bronze_path  = f"abfss://bronze@{self._storage_account_name}.dfs.core.windows.net"
        self.silver_path  = f"abfss://silver@{self._storage_account_name}.dfs.core.windows.net"
        self.gold_path    = f"abfss://gold@{self._storage_account_name}.dfs.core.windows.net"

        self.bronze_path_file = os.path.join(self.bronze_path, self.table)
        self.silver_path_file = os.path.join(self.silver_path, self.table)
        self.checkpoint_silver_path  = os.path.join(self.silver_path, 'checkpoint')

    def _ingestion_conf(self):
        """
        Loads the ingestion configuration from a JSON file (located in dbfs) and extracts source and sink configurations.

        Raises:
        Exception
            If there is an issue loading the ingestion configuration file.
        """
        
        try:
            with open(self.ingestion_conf_path_file, "r") as file:
                ingest_config = json.load(file)
        except Exception as e:
            raise Exception(f"Error loading ingestion configuration file: {str(e)}")


        self.source   = ingest_config.get("source")
        self.sink     = ingest_config.get("sink")

        self.db       = self.source.get("db")
        self.table    = self.source.get("table")
        self.columns  = self.source.get("columns")
        self.delta    = self.source.get("delta_column")
        self.calendar = self.source.get("calendar", {})

        source_sink = self.sink.get("source")
        self.bronze_format = source_sink.get("format")
        self.bronze_opts   = source_sink.get("options")

    def _connector_conf(self):
        """
        Loads the connector configuration from a JSON file (located in dbfs) for setting up connections to external data sources.

        Raises:
        Exception
            If there is an issue loading the connector configuration file.
        """
        connector_path = self.source.get("connector")
        connector_conf_path_file = os.path.join(self.connectors_conf_path, connector_path)
        try: 
            with open(connector_conf_path_file, "r") as file:
                connector_file = json.load(file)
        except Exception as e:
            raise Exception(f"Error loading connector configuration file: {str(e)}")

        self.db_format    = connector_file.get("format")
        self.db_opts      = connector_file.get("options")
    
    def create_schema_table(self, path: str, table: str):
        """
        Creates a schema and a Delta table if they do not exist.

        Parameters:
        path: str
            The path where the table data is stored.
        table: str
            The name of the table to be created.
        """
        create_schema_sql = f""" CREATE SCHEMA IF NOT EXISTS {self.db};"""
        use_schema_sql = f"""USE {self.db}; """
        create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {self.db}.{table} 
                    USING DELTA 
                    LOCATION '{path}'
        """
        self.spark.sql(create_schema_sql)
        self.spark.sql(use_schema_sql)
        self.spark.sql(create_table_sql)

    def set_logger(self):
        """
        Sets up logging configuration for the ingest application.

        This function configures the root logger by removing any existing handlers and setting up a new logger configuration.
        It defines a logging format that includes the timestamp, logger name, log level, and log message. A specific logger
        for the application ('engine_ingest') is created and set to the INFO level. And the logging level for the 'py4j' logger is set to WARNING.
        """
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            level=logging.INFO,
            format= '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers= [logging.StreamHandler()]
        )

        app_logger = logging.getLogger('engine_ingest')
        app_logger.setLevel(logging.INFO)

        logging.getLogger('py4j').setLevel(logging.WARNING)

    
class DbIngestionToBronze:
    """
    A class to handle the ingestion of data from the database to the Bronze layer in a medallion architecture.
    
    Attributes:
        medallion_config (MedallionConfig): Configuration object containing paths and settings for the medallion architecture.
        start_date (str): Optional start date for filtering data.
        end_date (str): Optional end date for filtering data.
        flag_range (str): Whether to use a date range.
        spark (SparkSession): Spark session used for querying and data manipulation.
    """

    def __init__(self, medallion_config: MedallionConfig, start_date = None, end_date = None, flag_range = None):
        """
        Initializes the ingestion process with configuration and optional date range.

        Parameters:
        medallion_config: MedallionConfig
            Configuration object containing paths and settings for the medallion architecture.
        start_date: str, optional
            The start date for filtering data (default is None).
        end_date: str, optional
            The end date for filtering data (default is None).
        flag_range: str, optional
            A flag to indicate whether to use a date range (default is None).
        """
        self.medallion_config = medallion_config
        self.start_date = start_date
        self.end_date = end_date
        self.flag_range = flag_range
        self.spark = SparkSession.builder.getOrCreate()

    def _generate_sql_root(self):
        """
        Generates the SQL root query to extract data from the source database.

        Returns:
        str
            SQL query string for the specified table and columns.
        """
        columns = ', '.join(self.medallion_config.columns.keys())
        table  = self.medallion_config.table
        return f"SELECT {columns} FROM PUBLIC.{table}"

    def _calculate_date_range(self):
        """
        Calculates the start and end date for the SQL query, either from provided values or default values in the configuration.

        Returns:
        tuple
            A tuple containing start_date and end_date.
        """
        if self.flag_range and self.start_date and self.end_date:
            end_date  = pd.Timestamp(self.end_date, tz='UTC')
            start_date = pd.Timestamp(self.start_date, tz='UTC')
        else:
            end_date = pd.Timestamp(self.medallion_config.calendar.get("end_date", pd.Timestamp.now()), tz='UTC')
            start_date = pd.Timestamp(self.medallion_config.calendar.get("start_date", end_date - pd.Timedelta(days=3)), tz='UTC')
        return start_date, end_date
        
    def generate_sql_statement(self):
        """
        Generates the complete SQL query statement including filtering by date range if necessary.

        Returns:
        str
            SQL statement string with or without date filtering.
        """
        sql_root = self._generate_sql_root()

        if self.medallion_config.delta:
            start_date, end_date = self._calculate_date_range()
            return  f"{sql_root} WHERE {self.medallion_config.delta} BETWEEN '{start_date}' AND '{end_date}'"
        else:
            return sql_root
    
    def _convert_time_stamp(self, df: DataFrame, columns: dict) -> DataFrame:
        """
        Converts specified columns from timestamp format to string format.

        Parameters:
        df: DataFrame
            Spark DataFrame containing the data.
        columns: dict
            Dictionary of columns and their types to identify which columns to convert.

        Returns:
        DataFrame
            Spark DataFrame with converted timestamp columns.
        """
        
        columns_to_parse = [column for column, col_type in columns.items() if col_type == "timestamp"]

        for column in columns_to_parse: 
                df = df.withColumn(column, F.col(column).cast('string'))

        return df

    def load_data(self, sql_statement: str):
        """
        Loads data from the source database into a Spark DataFrame based on the SQL query.

        Parameters:
        sql_statement: str
            The SQL query to extract data from the source database.

        Returns:
        DataFrame
            Spark DataFrame containing the loaded data.

        Raises:
        RuntimeError
            If there is an error while loading the data from the source.
        """
        
        try:
            df = self.spark.read.format(self.medallion_config.db_format)\
                    .option("query", sql_statement)\
                    .options(**self.medallion_config.db_opts)\
                    .load()
            df = self._convert_time_stamp(df, self.medallion_config.columns)
            return df
        
        except Exception as e:
            raise RuntimeError(f"Error loading data with the SQL statement: {sql_statement}, error: {str(e)}") 


    def write_data(self, df: DataFrame, path: str):
        """
        Writes the DataFrame data to the Bronze layer as Parquet files.

        Parameters:
        df: DataFrame
            Spark DataFrame containing the data to be written.
        Raises:
        RuntimeError
            If there is an error while writing the data to the Bronze layer.
        """
        try:
            df.write.mode('append').parquet(path)
        except Exception as e:
            raise RuntimeError(f"Error writing data to the Bronze layer, error: {str(e)}")
    
    def update_calendar(self):
        """
        Updates the start_date in the calendar configuration to the current date for the next execution and saves the updated configuration to json file.
       
        Raises:
        Exception
            If there is an issue writing the updated configuration to json file.
        """
        self.medallion_config.calendar["start_date"] = dt.datetime.now().strftime("%Y%m%d")

        try:
            with open(self.medallion_config.ingestion_conf_path_file, 'w') as file:
                json.dump({"source":self.medallion_config.source, "sink": self.medallion_config.sink}, file, indent = 4)
        except Exception as e:
            raise Exception(f"Error writing updated calendar configuration to json file, error: {str(e)}")

class BronzeIngestionToSilver:
    """
    A class to handle the ingestion of data from the Bronze layer to the Silver layer in a medallion architecture.

    Attributes:
        medallion_config (MedallionConfig): Configuration object containing paths and settings for the medallion architecture.
        spark (SparkSession): Spark session used for querying and data manipulation.
    """
    def __init__(self, medallion_config: MedallionConfig):
        """
        Initializes the ingestion process with the configuration for the Silver layer.

        Parameters:
        medallion_config: MedallionConfig
            Configuration object containing paths and settings for the medallion architecture.
        """
        self.medallion_config = medallion_config
        self.spark = SparkSession.builder.getOrCreate()
    
    def process_batch(self, df_batch: DataFrame, batch_id: int, path: str):
        """
        Processes a batch of data from Bronze to Silver, merging new records with existing ones.

        Parameters:
        df_batch: DataFrame
            The Spark DataFrame containing the batch of data to process.
        batch_id: int
            The ID of the batch being processed.
        """

        dates_to_insert = [row.date for row in df_batch.select(F.col("date")).distinct().collect()]
        
        df_exists = self.spark.read.format("delta")\
                            .load(path)\
                            .filter(F.col("date")\
                            .isin(dates_to_insert))\
                            .select("id").cache()

        df_append = df_batch.join(df_exists, on = "id", how = 'left_anti')

        delta_table_exists = DeltaTable.forPath(self.spark, path)
        
        delta_table_exists.alias("exist_data")\
            .merge(df_append.alias("new_data"), "exist_data.id = new_data.id")\
            .whenNotMatchedInsertAll()\
            .execute()
    
    def load_transform_event_data(self) -> DataFrame:
        """
        Loads and transforms event data from the Bronze layer, applying necessary transformations for the Silver layer.

        Returns:
        DataFrame
            The transformed Spark DataFrame.
        Raises:
        RuntimeError
            If there is an error while loading or transforming the event data.
        """
        try:
            df = self.spark.readStream.format(self.medallion_config.bronze_format)\
                            .options(**self.medallion_config.bronze_opts)\
                            .option("cloudFiles.schemaLocation", self.medallion_config.bronze_path_file)\
                            .load(self.medallion_config.bronze_path_file)\
                            .withColumn("time", from_utc_timestamp(F.col("time"), "America/Bogota"))\
                            .withColumn("date", date_format('time', 'yyyy-MM-dd'))\
                            .withColumn("_ingested_at",F.current_timestamp())\
                            .withColumn("_filename",   F.input_file_name())\
                            .drop("artificial","recovered")
        except Exception as e:
            raise RuntimeError(f"Error loading and transforming event data from the Bronze layer, error: {str(e)}")
        return df
    
    def write_event_data(self, df: DataFrame):
        """
        Writes event data from the transformed DataFrame to the Silver layer.

        Parameters:
        df: DataFrame
            The transformed Spark DataFrame containing event data.
            
        Raises:
        RuntimeError
            If there is an error while writing the event data to the Silver layer.
        """
        try:
            df.writeStream\
                .foreachBatch(self.process_batch)\
                .option("checkpointLocation", self.medallion_config.checkpoint_silver_path)\
                .trigger(availableNow=True)\
                .start()
        except Exception as e:
            raise RuntimeError(f"Error writing event data to the Silver layer, error: {str(e)}")
    
    def load_dimension_data(self, path: str) -> DataFrame:
        """
        Loads dimension data from the Bronze layer.

        Returns:
        DataFrame
            The Spark DataFrame containing dimension data.
        Raises:
        RuntimeError
            If there is an error loading the dimension data from the Bronze layer.
        """
        try:
            return self.spark.read.parquet(path)
        except Exception as e:
            raise RuntimeError(f"Error loading dimension data from the Bronze layer, error: {str(e)}")
    
    def write_dimension_data(self, df: DataFrame, path: str):
        """
        Writes dimension data to the Silver layer, ensuring schema consistency.
        
        Parameters:
        df: DataFrame
            The transformed Spark DataFrame containing dimension data.
        Raises:
        RuntimeError
            If there is an error writing the dimension data to the Silver layer.
        """
        try:
            df.distinct().write\
                .mode('overwrite')\
                .format('parquet')\
                .save(path)
        except Exception as e:
            raise RuntimeError(f"Error writing dimension data to the Silver layer, error: {str(e)}")
    
   