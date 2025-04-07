import os
from pyspark.sql.functions import col, current_date, date_format, to_date, datediff, add_months, date_add, date_sub

os.environ['PYSPARK_PYTHON'] = "python"
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import random
spark = SparkSession.builder.appName("Learning Pyspark").getOrCreate()
print('---------Working with dates in Pyspark---------')

data=[["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
df=spark.createDataFrame(data,["id","input"])
df.show()


# current_date() returns today date
df.select(current_date().alias("current_date")).show()

# date_format: change format of date
df.select(
    col("input"),date_format(col("input"),"dd-MM-yyyy").alias("to_date")
).show()


# to_date(): used to cast date_col in date format
df.select(col("input"),
    to_date(col("input"),"yyyy-MM-dd").alias("date_type")
).show()

df.select(
    col("input"),
    datediff(current_date(),col("input")).alias("date_diff")
).show()




#add_months() , date_add(), date_sub()
df.select(col("input"),
    add_months(col("input"),3).alias("add_months"),
    add_months(col("input"),-3).alias("sub_months"),
    date_add(col("input"),4).alias("date_add"),
    date_sub(col("input"),4).alias("date_sub")
  ).show()

data2 = [
    ["2025-04-07"]
]
df1 = spark.createDataFrame(data2,["date"])
df1.show()
df1.printSchema()


collect_date = df1.select(to_date(col("date"))).collect()[0][0]
print(collect_date)
print(type(collect_date))