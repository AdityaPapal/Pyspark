import os
from pyspark.sql.functions import col
os.environ['PYSPARK_PYTHON'] = "python"
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import random
spark = SparkSession.builder.appName("Learning Pyspark").getOrCreate()
print('---------Manipulate columns fields in dataframe---------')

# Sample data generators
first_names = ["John", "Jane", "Alice", "Bob", "Mike", "Emma", "Tom", "Lily", "Sam", "Eva"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Davis", "Miller", "Wilson", "Clark", "Lewis"]
domains = ["gmail.com", "yahoo.com", "outlook.com"]

def generate_data(i):
    first = random.choice(first_names)
    last = random.choice(last_names)
    name = f"{first} {last}"
    age = random.randint(18, 60)
    gender = random.choice(["Male", "Female"])
    mobile = "9" + str(random.randint(100000000, 999999999))
    email = f"{first.lower()}.{last.lower()}{i}@{random.choice(domains)}"
    return (i, name, age, gender, mobile, email)

# Generate 50 entries
data = [generate_data(i) for i in range(1, 51)]

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("mobile_number", StringType(), True),
    StructField("email_id", StringType(), True),
])

# Create DataFrame
df = spark.createDataFrame(data, schema)
df.show()

# multiple ways to select columns
df.select(df['id']).show()
df.select(df.id).show()

# we used selectExpr() for ETL purpose
new_df = df.selectExpr(
    "cast(id as bigint) as person_id",
    "cast(name as string) as person_name",
    "cast(age as bigint) as person_age"
).filter(
    col("person_id") <= 10
)
new_df.show()


print("collect() function in pyspark")

first_person_age = df.select(col('age')).collect()[0][0]
