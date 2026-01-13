from pyspark.sql import SparkSession
from delta.tables import *

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

table_path = "/opt/spark/warehouse/enriched_trades"
deltaTable = DeltaTable.forPath(spark, table_path)

deltaTable.history().select("version", "operation", "timestamp").show(5, truncate=False)
