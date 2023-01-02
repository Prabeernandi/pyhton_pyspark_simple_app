import os
import sys
import time

from pyspark.sql import SparkSession

os.environ['HADOOP_HOME'] = "C:/hadoop-3.0.0"
sys.path.append("C:/hadoop-3.0.0/bin")

startTime = time.time()

spark = SparkSession \
    .builder \
    .appName("mongodbtest1") \
    .master('local') \
    .config("spark.mongodb.input.uri", "mongodb://10.18.11.143:27017/vishwakarma_testing.pride") \
    .config("spark.mongodb.output.uri", "mongodb://10.18.11.143:27017/vishwakarma_testing.pyspark_testing") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()

books_tbl = spark.read \
    .format('com.mongodb.spark.sql.DefaultSource') \
    .load()
books_table = books_tbl.createOrReplaceTempView("books_tbl")
query1 = spark.sql("SELECT * FROM books_tbl")
# userData = spark.createDataFrame(books_tbl)

books_tbl.write \
    .format('com.mongodb.spark.sql.DefaultSource') \
    .mode("overwrite") \
    .save()

endTime = time.time()
print("total time ", (endTime - startTime), " seconds")
print(books_tbl.count())
