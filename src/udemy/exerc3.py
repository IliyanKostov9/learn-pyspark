import numpy as np
import pyspark.sql.functions as sqlfun
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./blob/customers-10000.csv", header=True)

# Remove rows with null values
df_drop = df.na.drop()
df_drop.show()

# Or remove rows for specific column
df_null_salary = df.filter(df.Salary.isNotNull())
df_null_salary.show()

# Remove duplicates
df_no_duplicate = df.dropDuplicates().show()

# Select particular columns
df.select("Salary", "First Name", "Last Name").show()

df = df.withColumnRenamed("First Name", "FirstName")

# Get rows when first name is Heather
df.filter(df.FirstName == "Heather").show()

df = df.withColumnRenamed("Phone 1", "Phone1")

# Get all phone numbers ending with 5229
df.filter(df.Phone1.like("%5229")).show()
# Or you can use
df.filter(df.Phone1.endswith("5229")).show()  # startswith

# Get between id 1 to 5
df.filter(df.Index.between(1, 5)).show()

# Get name if it's in a list of names
df.filter(df.FirstName.isin("Heather", "Kristina")).show()

# Or sub-string a name into a new column
df.select(df.FirstName, df.FirstName.substr(1, 5).alias("name")).show()
