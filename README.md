# Introduction

This project is intended to be used only for learning purposes on PySpark.

## Notes

- split-apply-combine strategy = groups data based on a certain condition, applies a function to each of them and then combines them back as a DataFrame
`createDataFrame`

### Driver

`spark = SparkSession.builder.getOrCreate()` is the driver (process), that runs the main function
The driver schedules the jobs, that are executed by the worker nodes
The driver determines how data is partitioned and distributes it to the nodes.

`DataFrame.collect()` = retrieves all of the distributed data from the distributed DataFrame into the driver.
It returns all of the rows from all of the partitions into the driver program via a Python list.
Used when wanting to work with the entire dataset locally, like visualization, exporting and analysis.
If the dataframe is large, then it's recommended to instead use take(n)

