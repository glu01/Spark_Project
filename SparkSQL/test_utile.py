from unittest import TestCase
from pyspark.sql import *
from lib.utils import count_by_country
class UtilsTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
                .master("local[3]")\
                .appName("HappySQL")\
                .getOrCreate()

    def test_country_count(self):
        survey_df = self.spark.read \
                .option("header", "true")\
                .option("inferSchema", "true") \
                .csv("/Users/glu01/PycharmProjects/Spark_Projects/SparkSQL/data/sample.csv")
        count_list = count_by_country(self.spark, survey_df).collect()
        count_dict = dict()
        for row in count_list:
            count_dict[row['Country']] = row['count']
        self.assertEqual(count_dict["United States"], 4, "Count for United States should be 4")
        self.assertEqual(count_dict["Canada"], 2, "Count for Canada should be 2")
        self.assertEqual(count_dict["United Kingdom"], 1, "Count for Unites Kingdom should be 1")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
