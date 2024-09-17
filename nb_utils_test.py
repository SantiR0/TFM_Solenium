import pytest
import pandas as pd

from pyspark.sql import SparkSession
from nb_utils_modified import MedallionConfig, DbIngestionToBronze, BronzeIngestionToSilver


@pytest.fixture
def spark_session():
    """
    Fixture to initialize a Spark session for testing. The session is created at the start 
    and stopped at the end of the test.

    Yields:
        SparkSession: A Spark session for testing purposes.
    """
    spark = SparkSession.builder \
        .appName("unit-test") \
        .getOrCreate()

    yield spark
    spark.stop()


@pytest.fixture
def setup_environment_files():
    """
    Fixture to provide file paths for environment, ingestion, and connector configuration files.
    
    Returns:
        tuple: Paths to environment configuration, ingestion configuration, and connector configuration files.
    """
    env_conf_path        = '/home/s4nt1/Desktop/BDDE_master/TFM/ingestion_engine/ingestion_env/bin/tests/sources/environment_conf_test.json'
    ingestion_conf_path  = '/home/s4nt1/Desktop/BDDE_master/TFM/ingestion_engine/ingestion_env/bin/tests/sources/measurement_electrical_test.json'   
    connector_path       = '/home/s4nt1/Desktop/BDDE_master/TFM/ingestion_engine/ingestion_env/bin/tests/sources'   
    return env_conf_path, ingestion_conf_path, connector_path

def test_medallion_config(setup_environment_files, spark_session):
    """
    Test the initialization of the MedallionConfig class. This test checks the correct loading 
    of environment, ingestion, and connector configurations, along with the correct paths.

    Parameters:
        setup_environment_files (fixture): The file paths for the environment, ingestion, and connector configuration.
        spark_session (fixture): Spark session for the test.
    """
 
    spark  = spark_session
    env_conf_path, ingest_conf_dir, connector_conf_path = setup_environment_files

    MedallionConfig.env_conf_path = env_conf_path
    MedallionConfig.ingestion_conf_path = ingest_conf_dir
    MedallionConfig.ingestion_conf_path_file = ingest_conf_dir
    MedallionConfig.connectors_conf_path = connector_conf_path

    medallion_config = MedallionConfig("measurement_electrical_test")
    assert medallion_config.ingest_name == "measurement_electrical_test"
    assert medallion_config.spark == spark

    assert medallion_config._storage_account_name == "datasparkcourse"
    assert medallion_config._storage_account_access_key == "1010100xxxx"
    assert medallion_config.silver_format == "delta"
    assert medallion_config.gold_format == "delta"

    assert medallion_config.source["db"] == "quoia_test"
    assert medallion_config.source["table"] == "measurement_electrical_test"
    assert medallion_config.source["columns"]["id"] == "integer"
    assert medallion_config.source["delta_column"] == "time"
    assert medallion_config.source["calendar"]["start_date"] == "20240907"

    assert medallion_config.bronze_path == f"abfss://bronze@datasparkcourse.dfs.core.windows.net"
    assert medallion_config.silver_path == f"abfss://silver@datasparkcourse.dfs.core.windows.net"
    assert medallion_config.gold_path   == f"abfss://gold@datasparkcourse.dfs.core.windows.net"
    assert medallion_config.checkpoint_silver_path == f"abfss://silver@datasparkcourse.dfs.core.windows.net/checkpoint"
    assert medallion_config.bronze_path_file == f"abfss://bronze@datasparkcourse.dfs.core.windows.net/measurement_electrical_test"

    assert medallion_config.db_format == "postgresql"
    assert medallion_config.db_opts["password"] == "password"
    assert medallion_config.db_opts["port"] == "17924"

def test__load_env_conf_failure():
    """
    Test the failure scenario for loading the environment configuration. It ensures that the 
    exception is raised when the environment configuration file is missing or cannot be loaded.
    """

    with pytest.raises(Exception) as exc_info:
        test = MedallionConfig("measurement_electrical_test")

    assert "Error loading environment medallion configuration file:" in str(exc_info.value)

@pytest.fixture
def setup_medallion_config(setup_environment_files, spark_session):
    """
    Fixture to set up the MedallionConfig instance with the configuration paths provided.
    
    Parameters:
        setup_environment_files (fixture): The file paths for the environment, ingestion, and connector configuration.
        spark_session (fixture): Spark session for the test.

    Returns:
        MedallionConfig: The configured MedallionConfig object.
    """
    
    spark  = spark_session
    env_conf_path, ingest_conf_dir, connector_conf_path = setup_environment_files

    MedallionConfig.env_conf_path = env_conf_path
    MedallionConfig.ingestion_conf_path = ingest_conf_dir
    MedallionConfig.ingestion_conf_path_file = ingest_conf_dir
    MedallionConfig.connectors_conf_path = connector_conf_path
    MedallionConfig.bronze_path_file = env_conf_path

    ingestion_config = MedallionConfig("measurement_electrical_test")
    return ingestion_config

def test_generate_sql_statement(setup_medallion_config):
    """
    Test the generation of the SQL query based on the MedallionConfig. It checks that 
    the SQL root and final SQL query with date filtering are generated correctly.

    Parameters:
        setup_medallion_config (fixture): The configured MedallionConfig object.
    """
    medallion_config = setup_medallion_config
    ingestion = DbIngestionToBronze(medallion_config, "2023-01-01", "2023-01-02", "yes")

    assert ingestion.medallion_config == medallion_config
    assert ingestion.start_date == "2023-01-01"
    assert ingestion.end_date == "2023-01-02"
    assert ingestion.flag_range == "yes"
    assert ingestion.spark == medallion_config.spark

    assert ingestion._generate_sql_root() == "SELECT id, time, vp1, node_id FROM PUBLIC.measurement_electrical_test"
    
    assert ingestion.generate_sql_statement() == "SELECT id, time, vp1, node_id FROM PUBLIC.measurement_electrical_test WHERE time BETWEEN '2023-01-01 00:00:00+00:00' AND '2023-01-02 00:00:00+00:00'"

def test_load_data(spark_session, setup_medallion_config):
    """"
    Test the failure scenario when loading data with an invalid SQL statement. It ensures 
    that the correct exception is raised when the data cannot be loaded.
    
    Parameters:
        setup_environment_files (fixture): The file paths for the environment, ingestion, and connector configuration.
        spark_session (fixture): Spark session for the test.
    """

    medallion_config = setup_medallion_config
    ingestion = DbIngestionToBronze(medallion_config)

    sql_statement = "SELECT * FROM measurement_electrical_test"

    with pytest.raises(Exception) as exc_info:
        test = ingestion.load_data(sql_statement)

    assert f"Error loading data with the SQL statement: {sql_statement}, error:" in str(exc_info.value)

def test_write_data(spark_session, setup_medallion_config):
    """
    Test the writing of data to the Bronze layer. The test ensures that the data is 
    written correctly as Parquet files and validates the record count.

    Parameters:
        setup_environment_files (fixture): The file paths for the environment, ingestion, and connector configuration.
        spark_session (fixture): Spark session for the test.
    """
    medallion_config = setup_medallion_config
    ingestion = DbIngestionToBronze(medallion_config)

    df = spark_session.createDataFrame(pd.DataFrame({
        "id": [1, 2, 3],
        "time": ["2023-01-01", "2023-01-02", "2023-01-03"]
    }))

    sink_path = '/home/s4nt1/Desktop/BDDE_master/TFM/ingestion_engine/ingestion_env/bin/tests/sources/bronze_test_data/'

    ingestion.write_data(df, sink_path)

    assert spark_session.read.format("parquet").load(sink_path).count() == 3

def test_load_transform_event_data(spark_session, setup_medallion_config):
    """
    Test the failure scenario when loading and transforming event data from the Bronze layer. 
    It ensures that the correct exception is raised when an error occurs during the transformation.

    Parameters:
        setup_environment_files (fixture): The file paths for the environment, ingestion, and connector configuration.
        spark_session (fixture): Spark session for the test.
    """
    medallion_config = setup_medallion_config
    silver_ingestion = BronzeIngestionToSilver(medallion_config)

    with pytest.raises(Exception) as exc_info:
        silver_ingestion.load_transform_event_data()
    

    assert f"Error loading and transforming event data from the Bronze layer, error:" in str(exc_info.value)


def test_write_event_data(spark_session, setup_medallion_config):
    """
    Test the failure scenario when writing event data to the Silver layer. It ensures that
    the correct exception is raised when an error occurs during the writing process.

    Parameters:
        setup_environment_files (fixture): The file paths for the environment, ingestion, and connector configuration.
        spark_session (fixture): Spark session for the test.
    """

    medallion_config = setup_medallion_config
    silver_ingestion = BronzeIngestionToSilver(medallion_config)

    df = spark_session.createDataFrame(pd.DataFrame({
        "id": [1, 2, 3],
        "time": ["2023-01-01", "2023-01-02", "2023-01-03"]
    }))

    with pytest.raises(Exception) as exc_info:
        silver_ingestion.write_event_data(df)

    assert f"Error writing event data to the Silver layer, error:" in str(exc_info.value)

def test_load_dimension_data(spark_session, setup_medallion_config):
    """
    Test scenarios the loading of dimension data from the Bronze layer.
    Also test failure scenario wich ensures the correct exception is raised when an error occurs during the loading.

    Parameters:
        setup_environment_files (fixture): The file paths for the environment, ingestion, and connector configuration.
        spark_session (fixture): Spark session for the test.
    """
    medallion_config = setup_medallion_config
    silver_ingestion = BronzeIngestionToSilver(medallion_config)

    sink_path = '/home/s4nt1/Desktop/BDDE_master/TFM/ingestion_engine/ingestion_env/bin/tests/sources/bronze_test_data/'
    df = silver_ingestion.load_dimension_data(sink_path)

    assert df.count() == 3

    with pytest.raises(Exception) as exc_info:
        silver_ingestion.load_dimension_data("non_exist_path")

    assert f"Error loading dimension data from the Bronze layer, error:" in str(exc_info.value)


def test_write_dimension_data(spark_session, setup_medallion_config):
    """
    Test scenarios the writing of dimension data to the Silver layer.
    Also test failure scenario wich ensures the correct exception is raised when an error occurs during the loading.

    Parameters:
        setup_environment_files (fixture): The file paths for the environment, ingestion, and connector configuration.
        spark_session (fixture): Spark session for the test.
    """
    medallion_config = setup_medallion_config
    silver_ingestion = BronzeIngestionToSilver(medallion_config)

    df = spark_session.createDataFrame(pd.DataFrame({
        "id": [1, 2, 3],
        "time": ["2023-01-01", "2023-01-02", "2023-01-03"]
    }))
    sink_path = '/home/s4nt1/Desktop/BDDE_master/TFM/ingestion_engine/ingestion_env/bin/tests/sources/silver_test_data/'
    
    df = silver_ingestion.write_dimension_data(df, sink_path)

    assert spark_session.read.format("parquet").load(sink_path).count() == 3

    with pytest.raises(Exception) as exc_info:
        silver_ingestion.write_dimension_data(df, "non_exist_path")

    assert f"Error writing dimension data to the Silver layer, error:" in str(exc_info.value)