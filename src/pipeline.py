from pyspark.sql import SparkSession, DataFrame


def create_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def read_data(source_path: str, spark: SparkSession) -> DataFrame:
    # code to read csv into spark dataframe
    spark.read.csv()
