import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Practice').getOrCreate()
