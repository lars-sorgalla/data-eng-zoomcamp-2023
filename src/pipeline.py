from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set(
        "spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism
    )
    return spark


def read_data(source_path: str, spark: SparkSession) -> DataFrame:
    """get data from bronze directory and load into Spark dataframe"""
    return spark.read.csv(path=source_path, header=True, multiLine=True)


def convert_datatypes(df: DataFrame, spark: SparkSession) -> DataFrame:
    df_converted_dtypes = (
        df.withColumn("trending_date", f.to_date("trending_date", "yy.dd.MM"))
        .withColumn("category_id", f.col("category_id").cast("int"))
        .withColumn("publish_time", f.to_timestamp("publish_time"))
        .withColumn("views", f.col("views").cast("int"))
        .withColumn("likes", f.col("likes").cast("integer"))
        .withColumn("dislikes", f.col("dislikes").cast("integer"))
        .withColumn("comment_count", f.col("comment_count").cast("int"))
        .withColumn("comments_disabled", f.col("comments_disabled").cast("boolean"))
        .withColumn("ratings_disabled", f.col("ratings_disabled").cast("boolean"))
        .withColumn(
            "video_error_or_removed", f.col("video_error_or_removed").cast("boolean")
        )
    )
    return df_converted_dtypes
