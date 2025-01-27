#!/bin/bash
echo "Setting up Apache Iceberg Data Lake Pipeline..."

# AWS S3 Bucket Setup
aws s3 mb s3://your-bucket-name

# Spark Configuration Setup
mkdir -p config/
echo "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog" > config/spark_iceberg_config.yaml
echo "spark.sql.catalog.spark_catalog.type=hadoop" >> config/spark_iceberg_config.yaml
echo "spark.sql.catalog.spark_catalog.warehouse=s3://your-bucket-name/iceberg/" >> config/spark_iceberg_config.yaml

echo "Setup Complete!"
