from pyspark.sql import SparkSession
from delta.tables import *

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

table_path = "/opt/spark/warehouse/enriched_trades"
deltaTable = DeltaTable.forPath(spark, table_path)

print("Starting Compaction (OPTIMIZE)...")
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", "134217728")  # 128 MB
# This coalesces small files into larger ones. 
# ZORDER is optional but makes filtering by 'symbol' much faster.
deltaTable.optimize().executeCompaction()

print("Starting Cleanup (VACUUM)...")
# This permanently deletes files that are no longer in the latest state 
# AND are older than 168 hours (7 days).
# Delta has a built-in safety check that prevents you from 
# vacuuming anything less than 168 hours (7 days).
# bypass: spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
deltaTable.vacuum(168)