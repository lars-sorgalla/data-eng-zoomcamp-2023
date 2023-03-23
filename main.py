import src.pipeline as pipe

spark = pipe.create_spark_session()
raw_data_df = pipe.read_from_bronze(
    source_path="data/bronze/yt_popular_videos_de.csv", spark=spark
)
df_converted_dtypes = pipe.convert_datatypes(raw_data_df, spark)
pipe.to_silver_layer(
    df_converted_dtypes, target_path="data/silver/yt_popular_videos_de.snappy.parquet"
)
