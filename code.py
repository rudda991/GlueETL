from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Create DataFrame Example") \
    .getOrCreate()

# Sample data: a list of tuples
data = [(1, "Alice", 29),
        (2, "Bob", 31),
        (3, "Charlie", 25)]

# Define the schema (column names)
columns = ["id", "name", "age"]

# Create a DataFrame from the data
df = spark.createDataFrame(data, schema=columns)

# Show the DataFrame
df.show()

