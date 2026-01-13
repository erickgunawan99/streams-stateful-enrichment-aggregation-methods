from pyspark.sql import SparkSession
from delta.tables import *

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sql("""
    ALTER TABLE delta.`/opt/spark/warehouse/enriched_trades` 
    SET TBLPROPERTIES ('delta.targetFileSize' = '134217728', 
          'delta.logRetentionDuration' = '30 days')
""")
print("Set target file size to 128 MB for Delta table at /opt/spark/warehouse/enriched_trades")
spark.stop()