from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, window

warehouse_location = "/opt/spark/warehouse"
spark = SparkSession.builder \
    .appName("metric") \
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.stateStore.providerClass", 
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("spark.scheduler.mode", "FAIR") \
    .config("spark.cores.max", "2") \
    .config("spark.shuffle.partitions", "8") \
    .getOrCreate()

# Read the table back as a stream
delta_stream = spark.readStream.format("delta") \
    .load("/opt/spark/warehouse/enriched_trades")


POSTGRES_URL = "jdbc:postgresql://postgres:5432/streaming"
POSTGRES_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

def write_to_postgres_generic(df, epoch_id, table_name, mode="append"):
    print(f"[{epoch_id}] Writing batch to {table_name}...")
    
    try:
        df.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", table_name) \
            .options(**POSTGRES_PROPS) \
            .mode(mode) \
            .save()
            
        print(f"[{epoch_id}] Success: Written to {table_name}")
    except Exception as e:
        print(f"[{epoch_id}] ERROR writing to {table_name}: {str(e)}")
        raise e

def save_enriched_trades(df, epoch_id):
    clean_df = df.filter("symbol IS NOT NULL")
    write_to_postgres_generic(clean_df, epoch_id, "enriched_trades", "append")

def write_metrics_to_postgres(df, epoch_id):
    write_to_postgres_generic(df, epoch_id, "sector_metrics", "append")

q2 = delta_stream.writeStream \
    .foreachBatch(save_enriched_trades) \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", "/opt/spark/checkpoints/postgres_enriched_trades") \
    .start()

q3 = delta_stream \
    .withWatermark("event_timestamp", "5 minutes") \
    .groupBy(window(col("event_timestamp"), "1 minute"),
        col("sector")) \
    .agg(expr("count(*) as trade_count"),
         expr("sum(trade_value) as total_value"),
         expr("avg(price) as avg_price"),
         expr("sum(volume) as total_volume")) \
    .select(col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("sector"),
            col("trade_count"),
            col("total_value"),
            col("avg_price"),
            col("total_volume")) \
    .writeStream \
    .foreachBatch(write_metrics_to_postgres) \
    .trigger(processingTime="1 minutes") \
    .option("checkpointLocation", "/opt/spark/checkpoints/postgres_sector_metrics") \
    .outputMode("update") \
    .start()

spark.streams.awaitAnyTermination()