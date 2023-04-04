import datetime
import unittest
from src import pipeline
from pyspark import sql
from pyspark.sql import types


class TestConvertDataTypes(unittest.TestCase):
    """Test class that is unit testing the main Spark transformation
    function: convert_datatypes().
    """

    # executed before test case
    @classmethod
    def setUpClass(cls):
        cls.spark = sql.SparkSession.builder.getOrCreate()
        cls.spark.conf.set(
            "spark.sql.shuffle.partitions", cls.spark.sparkContext.defaultParallelism
        )

    # executed after test case
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_convert_datatypes(self):
        # 1. Prepare an input data frame that mimics our source data
        input_schema = types.StructType(
            [
                types.StructField("video_id", types.StringType(), nullable=True),
                types.StructField("trending_date", types.StringType(), nullable=True),
                types.StructField("title", types.StringType(), nullable=True),
                types.StructField("channel_title", types.StringType(), nullable=True),
                types.StructField("category_id", types.StringType(), nullable=True),
                types.StructField("publish_time", types.StringType(), nullable=True),
                types.StructField("tags", types.StringType(), nullable=True),
                types.StructField("views", types.StringType(), nullable=True),
                types.StructField("likes", types.StringType(), nullable=True),
                types.StructField("dislikes", types.StringType(), nullable=True),
                types.StructField("comment_count", types.StringType(), nullable=True),
                types.StructField("thumbnail_link", types.StringType(), nullable=True),
                types.StructField(
                    "comments_disabled", types.StringType(), nullable=True
                ),
                types.StructField(
                    "ratings_disabled", types.StringType(), nullable=True
                ),
                types.StructField(
                    "video_error_or_removed", types.StringType(), nullable=True
                ),
                types.StructField("description", types.StringType(), nullable=True),
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
        expected_schema = types.StructType(
            [
                types.StructField("video_id", types.StringType(), nullable=True),
                types.StructField("trending_date", types.DateType(), nullable=True),
                types.StructField("title", types.StringType(), nullable=True),
                types.StructField("channel_title", types.StringType(), nullable=True),
                types.StructField("category_id", types.IntegerType(), nullable=True),
                types.StructField("publish_time", types.TimestampType(), nullable=True),
                types.StructField("tags", types.StringType(), nullable=True),
                types.StructField("views", types.IntegerType(), nullable=True),
                types.StructField("likes", types.IntegerType(), nullable=True),
                types.StructField("dislikes", types.IntegerType(), nullable=True),
                types.StructField("comment_count", types.IntegerType(), nullable=True),
                types.StructField("thumbnail_link", types.StringType(), nullable=True),
                types.StructField(
                    "comments_disabled", types.BooleanType(), nullable=True
                ),
                types.StructField(
                    "ratings_disabled", types.BooleanType(), nullable=True
                ),
                types.StructField(
                    "video_error_or_removed", types.BooleanType(), nullable=True
                ),
                types.StructField("description", types.StringType(), nullable=True),
            ]
        )

        expected_data = [
            (
                "LgVi6y5QIjM",
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
        transformed_df = pipeline.convert_datatypes.fn(input_df, self.spark)

        # 4. assert if transformed df and expected df match
        # 4.1. compare schemas
        self.assertEqual(
            expected_df.schema, transformed_df.schema, "Schemas not identical"
        )


if __name__ == "__main__":
    unittest.main()
