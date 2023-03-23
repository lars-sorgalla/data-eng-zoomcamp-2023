import src.pipeline as pipe

spark = pipe.create_spark_session()

raw_data_df = pipe.read_data("data/bronze/yt_popular_videos_de.csv", spark)
# raw_data_df.printSchema()

df_converted_dtypes = pipe.convert_datatypes(raw_data_df, spark)
df_converted_dtypes.printSchema()
df_converted_dtypes.show(vertical=True, n=3)
