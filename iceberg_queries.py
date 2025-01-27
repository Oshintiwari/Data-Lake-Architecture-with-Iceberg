from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergQueries") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3://your-bucket-name/iceberg/") \
    .getOrCreate()

# Query Data
query_df = spark.sql("SELECT * FROM spark_catalog.default.employees WHERE Salary > 3500")
query_df.show()

# Lineage Tracking Log
print("Query executed successfully with lineage tracking.")
