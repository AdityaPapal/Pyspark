from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, greatest
from datetime import datetime, timedelta
import os
from pyspark.sql.functions import max

from pyspark.sql.functions import col, current_date, date_format, to_date, datediff, add_months, date_add, date_sub

os.environ['PYSPARK_PYTHON'] = "python"
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import random
spark = SparkSession.builder.appName("Learning Pyspark").getOrCreate()
print('---------Working with dates in Pyspark---------')

# Helper to generate date list
def generate_dates(start_date, num_days, interval_days=1):
    return [(i + 1, start_date + timedelta(days=i * interval_days), (i + 1) * 10) for i in range(num_days)]

# Data for DataFrame 1
start_date_1 = datetime(2023, 1, 1)
data1 = generate_dates(start_date_1, 10)

# Data for DataFrame 2
start_date_2 = datetime(2023, 6, 1)
data2 = generate_dates(start_date_2, 10, interval_days=2)

# Create DataFrames
df1 = spark.createDataFrame(data1, ["ID", "Date", "Value"])
df2 = spark.createDataFrame(data2, ["ID", "Date", "Value"])

# Show DataFrames
print("DataFrame 1:")
df1.show()
df1.printSchema()
print("DataFrame 2:")
df2.show()
df2.printSchema()


df1 = df1.select(col("ID"),date_format(col("Date"),"yyyy-MM-dd").alias("date1"))
df1.show()
df2 = df2.select(col("ID"),date_format(col('Date'),"yyyy-MM-dd").alias("date2"))
df2.show()


left_join_data = df1.join(df2,on='ID',how="left")
left_join_data.show()

max_date_1 = left_join_data.agg(max("date1").alias("max_date"))
max_date_1.show()
max_date_2 = left_join_data.agg(max("date2").alias("max_date2"))
max_date_2.show()



left_join_data = left_join_data.withColumn("Greatest_date",greatest(col('date1'),col('date2')))
left_join_data.show()