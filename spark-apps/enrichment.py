from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, F
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, DoubleType, IntegerType, LongType
import pandas as pd
from pyspark.sql.streaming.state import GroupState
from typing import Iterator


def enrich_with_state_func(
    key: tuple, 
    pdf_iter: Iterator[pd.DataFrame], 
    state: GroupState
) -> Iterator[pd.DataFrame]:
    
    # 1. Collect all rows
    pdf = pd.concat(list(pdf_iter), ignore_index=True)
    pdf = pdf.sort_values('event_timestamp')
    
    # 2. Fetch State (Simulate the 'Seed' row)
    if state.exists:
        # State in Spark 3.5 is returned as a python tuple if you used StructType
        current_state_tuple = state.get
        
        state_dict = {
            'company': current_state_tuple[0],
            'sector': current_state_tuple[1],
            'market_cap': current_state_tuple[2],
            'info_timestamp': pd.Timestamp(current_state_tuple[3]),
            'record_type': 'info_state',
            'event_timestamp': pd.Timestamp(current_state_tuple[3])
        }
        # Add missing columns as None/NaN
        state_df = pd.DataFrame([state_dict])
        pdf = pd.concat([state_df, pdf], ignore_index=True)

    # 3. Forward Fill (The Magic)
    cols_to_fill = ['company', 'sector', 'market_cap', 'info_timestamp']
    pdf[cols_to_fill] = pdf[cols_to_fill].ffill()

    # 4. Update State
    last_info_mask = pdf['company'].notna()
    if last_info_mask.any():
        last_info_row = pdf[last_info_mask].iloc[-1]
        
        should_update = False
        if not state.exists:
            should_update = True
        else:
            # Compare timestamps
            current_state_tuple = state.get
            stored_ts = pd.Timestamp(current_state_tuple[3])
            new_ts = pd.Timestamp(last_info_row['info_timestamp'])
            if new_ts > stored_ts:
                should_update = True
        
        if should_update:
            # Update expects a tuple matching the schema
            new_state = (
                str(last_info_row['company']),
                str(last_info_row['sector']),
                int(last_info_row['market_cap']),
                last_info_row['info_timestamp']
            )
            state.update(new_state)

    # 5. Filter & Yield
    result_df = pdf[pdf['record_type'] == 'trade'].copy()
    
    if not result_df.empty:
        result_df['trade_value'] = result_df['price'] * result_df['volume']
        # Return strictly the columns matching output_schema
        yield result_df[[
            'symbol', 'price', 'volume', 'event_timestamp', 
            'company', 'sector', 'market_cap', 'info_timestamp', 'trade_value'
        ]]

warehouse_location = "/opt/spark/warehouse"
# Spark session
spark = SparkSession.builder \
    .appName("StreamEnrichment") \
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.stateStore.providerClass", 
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("spark.cores.max", "1") \
    .config("spark.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schemas
trades_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("trade_timestamp", LongType(), True)
])

info_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("company", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("market_cap", LongType(), True),
    StructField("info_timestamp", LongType(), True)
])


# Read Kafka streams
trades_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-trades") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load()

print("=== Trades stream created ===")
trades_df.printSchema()

trades_parsed = trades_df.select(from_json(col("value").cast("string"), trades_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_timestamp", (F.col("trade_timestamp") / 1000).cast("timestamp"))

trades_aligned = trades_parsed \
    .withColumn("record_type", expr("'trade'")) \
    .withColumn("company", expr("null").cast("string")) \
    .withColumn("sector", expr("null").cast("string")) \
    .withColumn("market_cap", expr("null").cast("long")) \
    .withColumn("info_timestamp", expr("null").cast("timestamp")) \
    .select("symbol", "event_timestamp", "record_type", "price", "volume", 
            "company", "sector", "market_cap", "info_timestamp")
# add record_type to identify which trade which info so that we can filter later
print("=== Trades stream parsed ===")
trades_aligned.printSchema()

info_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-info") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load()

print("=== Info stream created ===")
info_df.printSchema()

info_parsed = info_df.select(from_json(col("value").cast("string"), info_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_timestamp", (F.col("info_timestamp") / 1000).cast("timestamp")) 

info_aligned = info_parsed \
    .withColumn("info_timestamp", col("event_timestamp")) \
    .withColumn("record_type", expr("'info'")) \
    .withColumn("price", expr("null").cast("double")) \
    .withColumn("volume", expr("null").cast("integer")) \
    .select("symbol", "event_timestamp", "record_type", "price", "volume", 
            "company", "sector", "market_cap", "info_timestamp")
print("=== Info stream parsed ===")
info_aligned.printSchema()

unified_stream = trades_aligned.unionByName(info_aligned) \
    .withWatermark("event_timestamp", "5 minutes") 

output_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("event_timestamp", TimestampType(), True), # Was trade_timestamp
    StructField("company", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("market_cap", LongType(), True),
    StructField("info_timestamp", TimestampType(), True),
    StructField("trade_value", DoubleType(), True)
])
state_schema = StructType([
    StructField("company", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("market_cap", LongType(), True),
    StructField("info_timestamp", TimestampType(), True)
])

enriched_trades = unified_stream.groupBy("symbol") \
    .applyInPandasWithState(
        func=enrich_with_state_func,
        stateStructType=state_schema,
        outputStructType=output_schema,
        outputMode="append", # We append enriched trades
        timeoutConf="NoTimeout"
    )

print("=== Enriched trades stream created (No Duplicates) ===")
enriched_trades.printSchema()


print("=== Starting queries ===")
# --- Start streaming queries ---
query1 = enriched_trades \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .option("checkpointLocation", "/opt/spark/checkpoints/enriched_delta") \
    .toTable("enriched_trades")

query1.awaitTermination()