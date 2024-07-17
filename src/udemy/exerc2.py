import numpy as np
import pyspark.sql.functions as sqlfun
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# If we want to determenistically set for each column a specific data type, create a struct type like this

schema = StructType(
    [
        StructField("Index", IntegerType()),
        StructField("Customer Id", IntegerType()),
        StructField("First Name", StringType()),
        StructField("Last Name", StringType()),
        StructField("Company", StringType()),
        StructField("City", StringType()),
        StructField("Phone 1", StringType()),
        StructField("Phone 2", StringType()),
        StructField("Subscription Date", StringType()),
        StructField("Website", StringType()),
        StructField("Salary", FloatType()),
        StructField("Language", StringType()),
    ]
)

# Load data into DataFrame
df = spark.read.csv("./blob/customers-10000.csv", header=True, schema=schema)

# Get all types
print(df.dtypes)

# Get first rows from the DataFrame
print(df.head(5))

# Get first row
print(df.first())

# Get a summary
print(df.describe().show())

# Get all columns
print(df.columns)

# Get the number of rows
print(df.count())

# Get unique number of rows
print(df.distinct().count())
