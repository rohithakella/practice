from practice.spark_session import get_spark, parquet_reader, dataset_path

spark = get_spark()

df = parquet_reader(spark).parquet(dataset_path("datasets", "nyc_taxi/2023/yellow_tripdata_2023-01.parquet"))

df.limit(1).show()
