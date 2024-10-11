# from pyspark.shell import spark
from pyspark.sql import *
from pyspark import SparkConf
from lib.logger import  Log4j
from lib.utils import count_by_country
import sys

if __name__ == "__main__":
    conf = SparkConf()\
        .setMaster("local[3]")\
        .setAppName("HelloSQL") \
        .set("spark.driver.extraJavaOptions",
             "-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=./app-logs -Dlogfile.name=helloSQL")

    spark = SparkSession.builder\
        .config(conf=conf)\
        .getOrCreate()
    logger = Log4j(spark)
    # spark = SparkSession.builder\
    #     .master("local[3]")\
    #     .appName("HelloSQL")\
    #     .getOrCreate()
    logger.info("HelloSQL starting:")
    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    survey_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .format("csv")\
        .load(sys.argv[1])

    count_df = count_by_country(spark, survey_df)
    count_df.show()

    logger.info("HelloSQL finishing")