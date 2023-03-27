from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
from google.cloud import bigquery, storage


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set(
        "spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism
    )
    # suppress _SUCCESS file generation
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    return spark


def read_from_bronze(source_path: str, spark: SparkSession) -> DataFrame:
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


def write_to_silver_layer(df: DataFrame, target_path: str) -> None:
    df.toPandas().to_parquet(path=target_path)


def write_to_gcs_bucket(
    file_in_silver_layer: str,
    bucket_name: str,
    json_credentials_path: str,
    blob_name: str,
) -> None:
    client: storage.Client = storage.Client.from_service_account_json(
        json_credentials_path
    )
    bucket: storage.Bucket = storage.Bucket(client, name=bucket_name)
    # name of uploaded file
    blob: storage.bucket.Blob = bucket.blob(blob_name=blob_name)
    blob.upload_from_filename(file_in_silver_layer)


def create_bq_dataset(json_credentials_path: str, dataset_name: str) -> None:
    client = bigquery.Client.from_service_account_json(json_credentials_path)

    # fully qualified name of tablespace/dataset
    dataset_id = f"{client.project}.{dataset_name}"

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "europe-west3"
    print(f"{dataset.location=}")
    # Send the dataset to the API for creation, with an explicit timeout.
    dataset_job = client.create_dataset(dataset, exists_ok=True, timeout=30)
    print("Created dataset {}.{}".format(client.project, dataset_job.dataset_id))


def create_bq_table(
    json_credentials_path: str,
    dataset_name: str,
    table_name: str,
    gcs_bucket_name: str,
    gcs_blob_name: str,
) -> None:
    """see:
    https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#loading_parquet_data_into_a_new_table
    """
    # get credentials
    client = bigquery.Client.from_service_account_json(json_credentials_path)

    # fully qualified name of table
    table_id = f"{client.project}.{dataset_name}.{table_name}"

    # delete table for idempotency
    client.delete_table(table=table_id, not_found_ok=True)

    # set config for load job to use Parquet source file format
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)

    # execute load job from Cloud Storage to BigQuery
    client.load_table_from_uri(
        source_uris=f"gs://{gcs_bucket_name}/{gcs_blob_name}",
        destination=table_id,
        location="europe-west3",
        job_config=job_config,
    )
