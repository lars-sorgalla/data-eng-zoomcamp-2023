import datetime
import unittest
import src.pipeline as p
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    TimestampType,
    DateType,
    StringType,
    StructType,
    StructField,
)
from prefect import task


class TestConvertDataTypes(unittest.TestCase):
    # executed before test case
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.getOrCreate()
        cls.spark.conf.set(
            "spark.sql.shuffle.partitions", cls.spark.sparkContext.defaultParallelism
        )

    # executed after test case
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_convert_datatypes(self):
        # 1. Prepare an input data frame that mimics our source data
        input_schema = StructType(
            [
                StructField("video_id", StringType(), nullable=True),
                StructField("trending_date", StringType(), nullable=True),
                StructField("title", StringType(), nullable=True),
                StructField("channel_title", StringType(), nullable=True),
                StructField("category_id", StringType(), nullable=True),
                StructField("publish_time", StringType(), nullable=True),
                StructField("tags", StringType(), nullable=True),
                StructField("views", StringType(), nullable=True),
                StructField("likes", StringType(), nullable=True),
                StructField("dislikes", StringType(), nullable=True),
                StructField("comment_count", StringType(), nullable=True),
                StructField("thumbnail_link", StringType(), nullable=True),
                StructField("comments_disabled", StringType(), nullable=True),
                StructField("ratings_disabled", StringType(), nullable=True),
                StructField("video_error_or_removed", StringType(), nullable=True),
                StructField("description", StringType(), nullable=True),
            ]
        )
        input_data = [
            (
                "LgVi6y5QIjM",
                "23.30.03",
                "Wie werde ich zu einem Senior-Software-Entwickler?",
                "Der Softwarekanal",
                "24",
                "2023-03-30T17:08:49.000Z",
                '"softwareentwicklung"|"karriere"',
                "252786",
                "35885",
                "230",
                "1539",
                "https://i.ytimg.com/vi/LgVi6y5QIjM/default.jpg",
                "False",
                "False",
                "False",
                "Ich zeige euch, wie man zu einem Senior-Software-Entwickler wird. Viel"
                + "Spaß!\n\n►Merch: findet ihr hier",
            )
        ]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        # 2. Prepare an expected output data frame
        expected_schema = StructType(
            [
                StructField("video_id", IntegerType(), nullable=True),
                StructField("trending_date", DateType(), nullable=True),
                StructField("title", StringType(), nullable=True),
                StructField("channel_title", StringType(), nullable=True),
                StructField("category_id", IntegerType(), nullable=True),
                StructField("publish_time", TimestampType(), nullable=True),
                StructField("tags", StringType(), nullable=True),
                StructField("views", IntegerType(), nullable=True),
                StructField("likes", IntegerType(), nullable=True),
                StructField("dislikes", IntegerType(), nullable=True),
                StructField("comment_count", IntegerType(), nullable=True),
                StructField("thumbnail_link", StringType(), nullable=True),
                StructField("comments_disabled", BooleanType(), nullable=True),
                StructField("ratings_disabled", BooleanType(), nullable=True),
                StructField("video_error_or_removed", BooleanType(), nullable=True),
                StructField("description", StringType(), nullable=True),
            ]
        )

        expected_data = [
            (
                # "LgVi6y5QIjM",
                25,
                datetime.date(2023, 3, 30),
                "Wie werde ich zu einem Senior-Software-Entwickler?",
                "Der Softwarekanal",
                24,
                datetime.datetime.strptime(
                    "2023-03-30T17:08:49.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                '"softwareentwicklung"|"karriere"',
                252786,
                35885,
                230,
                1539,
                "https://i.ytimg.com/vi/LgVi6y5QIjM/default.jpg",
                False,
                False,
                False,
                "Ich zeige euch, wie man zu einem Senior-Software-Entwickler wird. Viel"
                + "Spaß!\n\n►Merch: findet ihr hier",
            )
        ]

        expected_df = self.spark.createDataFrame(
            data=expected_data, schema=expected_schema
        )

        # 3. transform input data
        transformed_df = p.convert_datatypes.fn(input_df, self.spark)

        # 4. assert if transformed df and expected df match
        # 4.1. compare schemas
        print(f"{expected_df.schema.fields=}")
        print(f"{transformed_df.schema.fields=}")
        self.assertEquals(expected_df.schema, transformed_df.schema)


if __name__ == "__main__":
    unittest.main()
