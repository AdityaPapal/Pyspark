import os
os.environ['PYSPARK_PYTHON'] = "python"
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Learning Pyspark").getOrCreate()
print('hello.....start learning pyspark with advanced')


data = spark.read.csv("datasets/flight.csv",header=True)
data.show(5)

data.printSchema()


# inferschema allows to convert datatypes while reading in csv
df = spark.read.csv("datasets/flight.csv",header=True,inferSchema=True)
df.show(5)
df.printSchema()