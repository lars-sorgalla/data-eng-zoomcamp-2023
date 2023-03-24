import src.pipeline as pipe

spark = pipe.create_spark_session()
raw_data_df = pipe.read_from_bronze(
    source_path="data/bronze/yt_popular_videos_de.csv", spark=spark
)
df_converted_dtypes = pipe.convert_datatypes(raw_data_df, spark)
pipe.write_to_silver_layer(
    df_converted_dtypes, target_path="data/silver/yt_popular_videos_de.snappy.parquet"
)
pipe.write_to_gcs_bucket(
    # TODO: .part file constantly renames itself after each run.
    # Use globbing to dynamically retrieve the file
    file_from_silver_layer="data/silver/yt_popular_videos_de.snappy.parquet",
    bucket_name="data-eng-capstone-project",
    json_credentials_path="./taxi-rides-ny-376611-f7f2f8eb424c.json",
)
