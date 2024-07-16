import numpy as np
import pyspark.sql.functions as sqlfun
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load data into DataFrame
df = (
    spark.read.format("csv").option("header", "true").load("./blob/customers-10000.csv")
)

# Print schema
print(df.printSchema())

# Rename columns
df = df.withColumnRenamed("Last Name", "LastName")


# Create new column clean_last_name whenever LastName is null
# df = df.withColumn(
#     "clean_last_name", when(df.LastName.isNull(), "Unknown").otherwise(df.LastName)
# )

# Or Discard those rows being added to the DataFrame, whose column of LastName is null
# df = df.filter(df.LastName.isNotNull())

# Remove the $ sign for the salary by creating a new column
df = df.withColumn("clean_salary", df.Salary.substr(2, 100).cast("float"))

# Create new column clean_salary by calculating the average of the salary
mean = df.groupBy().avg("clean_salary").take(1)[0][0]

print(mean)

# Create new salary column, that populates every Null value from the clean_salary column with the mean value
df = df.withColumn(
    "new_mean_salary",
    when(df.clean_salary.isNull(), lit(mean)).otherwise(df.clean_salary),
)

df.show()

# Select all of the Latitude column values and return a DataFrame
latitude = df.select("Latitude")


latitude = latitude.filter(latitude.Latitude.isNotNull())

latitude = latitude.withColumn(
    "float_latitude", latitude.Latitude.cast("float")
).select(
    "float_latitude"
)  # Do Select to ignore the first column Latitude, since we don't need it

latitude.show()

median = np.median(latitude.collect())

print(f"Mean value of Latitude is: {median}")

df = df.withColumn(
    "lat",
    when(df.Latitude.isNull(), lit(median)).otherwise(df.Latitude),
)

df.show()

#
# df = df.groupBy("Country").agg(
#     sqlfun.avg("").alias("Contryy"), sqlfun.avg("City").alias("Cityy")
# )
df.show(vertical=True)

print(df.columns)

df.select().describe().show()
