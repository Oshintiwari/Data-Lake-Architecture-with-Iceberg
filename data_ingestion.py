from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("IcebergDataLake") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3://your-bucket-name/iceberg/") \
    .getOrCreate()

# Sample Data
data = [(1, "Alice", 3000), (2, "Bob", 4000), (3, "Charlie", 5000)]
columns = ["ID", "Name", "Salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Write to Iceberg Table
df.write.format("iceberg").mode("overwrite").save("spark_catalog.default.employees")

# Read from Iceberg Table
read_df = spark.read.format("iceberg").load("spark_catalog.default.employees")
read_df.show()
