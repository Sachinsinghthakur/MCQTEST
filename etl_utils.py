import pandas as pd
from sqlalchemy import text
from datetime import datetime,timedelta
from utils import send_alert
from pyspark.sql import SparkSession
from pyspark import SparkConf

def get_last_successful_runs(engine, pipeline_name: str, limit: int = 2):
    """
    Fetches last N runs of the ETL pipeline from metadata table.

    Parameters:
        engine: SQLAlchemy engine
        pipeline_name (str): Unique name of the pipeline
        limit (int): How many recent runs to fetch

    Returns:
        pandas.DataFrame: Contains last_run_time and status
    """
    query = """
    SELECT last_run_time, status
    FROM etl_metadata
    WHERE pipeline_name = :pipeline_name
    ORDER BY last_run_time DESC
    LIMIT :limit
    """
    return pd.read_sql(text(query), engine, params={
        "pipeline_name": pipeline_name,
        "limit": limit
    })


def get_time_range(
                    engine,
                    pipeline_name,
                    start_override=None,
                    end_override=None,
                    datetime_format="%Y:%m:%d %H"):

    if start_override and end_override:
        return start_override, end_override

    last_runs_df = get_last_successful_runs(engine, pipeline_name, limit=2)

    if last_runs_df.empty:
        return datetime.utcnow() - timedelta(hours=1), datetime.utcnow()

    last_successful_run = last_runs_df.iloc[0]
    last_run_time = datetime.strptime(last_successful_run["last_run_time"], datetime_format)
    last_status = last_successful_run["status"]

    if last_status != "success":
        if len(last_runs_df) > 1 and last_runs_df.iloc[1]["status"] != "success":
            send_alert(f"[CRITICAL] Last 2 runs of pipeline `{pipeline_name}` failed.")
        return last_run_time, datetime.utcnow()

    return last_run_time, datetime.utcnow()


def get_max_executors(record_count):
    if record_count <= 1_000_000:
        return 5
    elif record_count <= 5_000_000:
        return 10
    elif record_count <= 10_000_000:
        return 20
    else:
        return 40
    

def initialize_spark_session_with_scaling(cluster_name: str,record_count: int):

    conf = SparkConf()
    max_executors = get_max_executors(record_count)

    '''Setting up Dynamic Allocation of executor based on input load to handle larger laods'''
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.dynamicAllocation.minExecutors", "2") 
    conf.set("spark.dynamicAllocation.initialExecutors", "4")
    conf.set("spark.dynamicAllocation.maxExecutors", str(max_executors))  

    '''Enabling AQE will help spark jobs to automatically balance the size
         and number of partitions for optimized execution of pipeline'''
    conf.set("spark.sql.adaptive.enabled", "true")

    '''enabling coalescePartitions to avoid small partitions'''
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")


    '''enabling adaptive.skewJoin for better join performance
         even in case of skewed keys like repeated question_id'''
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    conf.set("spark.executor.memory", "4g")
    conf.set("spark.executor.cores", "4")

    spark = SparkSession.builder \
        .appName(cluster_name) \
        .config(conf=conf) \
        .getOrCreate()

    return spark
