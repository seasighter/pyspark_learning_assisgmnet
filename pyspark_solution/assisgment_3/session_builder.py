# creating spark session

from pyspark.sql import SparkSession

spark_session = SparkSession.builder\
    .appName("Myapp")\
    .getOrCreate()
