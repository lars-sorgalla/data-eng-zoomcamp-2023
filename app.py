import tomli
from src.pipeline import (
    create_spark_session,
    read_from_bronze,
    convert_datatypes,
    write_to_silver_layer,
    write_to_gcs_bucket,
    create_bq_dataset,
)

# parse job parameter configuration file
with open("./jobparams.toml", "rb") as f:
    job_conf: dict = tomli.load(f).get("jobparams")


spark = create_spark_session()
raw_data_df = read_from_bronze(
    source_path=job_conf.get("file_in_bronze_layer"), spark=spark
)

df_converted_dtypes = convert_datatypes(raw_data_df, spark)

print("\nstart: write_to_silver_layer\n")
write_to_silver_layer(
    df_converted_dtypes, target_path=job_conf.get("file_in_silver_layer")
)

print("\nstart: write_to_gcs_bucket\n")
write_to_gcs_bucket(
    file_in_silver_layer=job_conf.get("file_in_silver_layer"),
    bucket_name=job_conf.get("gcs_bucket_and_dataset_name"),
    json_credentials_path=job_conf.get("json_credentials_path"),
    blob_name=job_conf.get("gcs_blob_name"),
)

print("\nstart: create_bq_dataset\n")
create_bq_dataset(
    json_credentials_path=job_conf.get("json_credentials_path"),
    dataset_name=job_conf.get("gcs_bucket_and_dataset_name"),
)
