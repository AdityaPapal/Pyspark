import os
from pyspark.sql.functions import col, lit

os.environ['PYSPARK_PYTHON'] = "python"
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Learning Pyspark").getOrCreate()
print('---------Manipulate columns fields in dataframe---------')

data = [
    ('James','','Smith','1991-04-01','M',3000),
    ('Michael','Rose','','2000-05-19','M',4000),
    ('Robert','','Williams','1978-09-05','M',4000),
    ('Maria','Anne','Jones','1967-12-01','F',4000),
    ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]

df = spark.createDataFrame(data,columns)

df.show()
df.printSchema()

df2 = df.withColumn("salary", col("salary").cast("Integer"))
df2.printSchema()
df2.show(truncate=False)

df3 = df.withColumn("salary", col("salary") * 100)
df3.printSchema()
df3.show(truncate=False)

df4 = df.withColumn("CopiedColumn", col("salary") * -1)
df4.printSchema()

df5 = df.withColumn("Country", lit("USA"))
df5.printSchema()

df6 = df.withColumn("Country", lit("USA")).withColumn("anotherColumn",
                                                      lit("anotherValue"))
df6.printSchema()

df.withColumnRenamed("gender", "sex") \
    .show(truncate=False)

df4.drop("CopiedColumn").show(truncate=False)
 