import os
os.environ['PYSPARK_PYTHON'] = "python"
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Learning Pyspark").getOrCreate()
print('---------show() function in pyspark---------')


columns = ["Seqno","Quote"]
data = [("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool.")]

df = spark.createDataFrame(data,columns)


# Display 2 rows and full column contents
df.show(2)

# Display full column contents with 3 columns
df.show(2,truncate=False)


# display data in vertically
df.show(3,truncate=False,vertical=True)

