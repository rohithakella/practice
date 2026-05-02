from pyspark.sql import SparkSession, DataFrameReader
from practice.config import LOCAL_STORAGE, SPARK_CONFIGS, PARQUET_READ_OPTIONS


def get_spark() -> SparkSession:
    builder = SparkSession.builder
    for key, value in SPARK_CONFIGS.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def parquet_reader(spark: SparkSession) -> DataFrameReader:
    reader = spark.read
    for key, value in PARQUET_READ_OPTIONS.items():
        reader = reader.option(key, value)
    return reader


def dataset_path(bucket: str, key: str) -> str:
    return f"{LOCAL_STORAGE}/{bucket}/{key}"
