from numpy import False_
import pyspark
from pyspark.sql import Row
from pyspark.sql import SparkSession
import sys
import os

os.environ['HADOOP_HOME'] = "C:/hadoop-3.0.0"
sys.path.append("C:/hadoop-3.0.0/bin")

spark = SparkSession \
    .builder \
    .appName("mongodbtest1") \
    .master('local') \
    .config("spark.mongodb.input.uri", "mongodb://10.18.11.143:27017/vishwakarma_testing.pride") \
    .config("spark.mongodb.output.uri", "mongodb://10.18.11.143:27017/vishwakarma_testing.pride") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()

books_tbl = spark.read \
    .format('com.mongodb.spark.sql.DefaultSource') \
    .load()
books_table = books_tbl.createOrReplaceTempView("books_tbl")
query1 = spark.sql("SELECT * FROM books_tbl")
query1.show(20,truncate=False_)

# print(books_tbl.first())
