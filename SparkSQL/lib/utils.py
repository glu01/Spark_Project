from pyspark import SparkConf

def count_by_country(spark, df):
    df.createOrReplaceTempView("temp_survey_table")
    return spark.sql("SELECT Country, COUNT(*) AS count FROM temp_survey_table WHERE Age < 40 GROUP BY Country")
