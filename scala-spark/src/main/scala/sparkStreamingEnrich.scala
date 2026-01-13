import org.apache.spark.sql.{SparkSession, Dataset, Encoders}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.math.BigDecimal
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.streaming.Trigger
import org.antlr.v4.runtime.atn.SemanticContext.OR
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.Or
object sparkStreamingEnrich {
  // 1. Output Schema
    case class EnrichedTrade(
        symbol: String, price: BigDecimal, volume: Int, 
        trade_timestamp: Long, company: String, sector: String, 
        market_cap: Long, info_timestamp: Long, trade_value: BigDecimal
    )

    // 2. The Common Parent for the Union
    sealed trait MarketEvent { def symbol: String 
    def getTimestamp: Long }

    // 3. Wrapper for Trades
    case class TradeEvent(
        symbol: String, price: BigDecimal, volume: Int, trade_timestamp: Long
    ) extends MarketEvent {
      override def getTimestamp: Long = trade_timestamp
    }

    // 4. Wrapper for Info
    case class InfoEvent(
        symbol: String, company: String, sector: String, market_cap: Long, info_timestamp: Long
    ) extends MarketEvent {
          override def getTimestamp: Long = info_timestamp
      }

    // 5. The State you keep in memory
    case class InfoState(
        company: String, sector: String, cap: Long, info_timestamp: Long
    )

    def updateState(
          symbol: String, 
          events: Iterator[MarketEvent], 
          state: GroupState[InfoState]
        ): Iterator[EnrichedTrade] = {
        
          // 1. Sort ALL events (Trades and Info) by timestamp chronologically
          // This ensures we only use info that actually existed when the trade happened
          if (state.hasTimedOut) {
               state.remove()
               return Iterator.empty
             }

          val sortedEvents = events.toList.sortBy(_.getTimestamp)

          if (sortedEvents.nonEmpty) {
              val latestEventTs = sortedEvents.map(_.getTimestamp).max
              // Set the 24-hour headroom based on the newest data seen
              state.setTimeoutTimestamp(latestEventTs + (24 * 60 * 60 * 1000L))
          }

          // 2. Load the initial state
          var currentState: Option[InfoState] = if (state.exists) Some(state.get) else None

          // 3. Single Pass: Process in order
          sortedEvents.map {
            case i: InfoEvent =>
              // Only update state if this info is newer than what we currently have
              if (i.info_timestamp > currentState.map(_.info_timestamp).getOrElse(0L)) {
                val newState = InfoState(i.company, i.sector, i.market_cap, i.info_timestamp)
                state.update(newState)
                currentState = Some(newState)
              }
              None
          
            case t: TradeEvent =>
              // Use the currentState as it was RIGHT NOW (not at the end of the batch)
              val enriched = currentState match {
                case Some(info) =>
                  EnrichedTrade(
                    symbol = t.symbol,
                    price = t.price,
                    volume = t.volume,
                    trade_timestamp = t.trade_timestamp,
                    company = info.company,
                    sector = info.sector,
                    market_cap = info.cap,
                    info_timestamp = info.info_timestamp,
                    trade_value = t.price * BigDecimal(t.volume)
                  )
                case None =>
                  EnrichedTrade(
                    symbol = t.symbol,
                    price = t.price,
                    volume = t.volume,
                    trade_timestamp = t.trade_timestamp,
                    company = "UNKNOWN",
                    sector = "UNKNOWN",
                    market_cap = 0L,
                    info_timestamp = 0L,
                    trade_value = t.price * BigDecimal(t.volume)
                  )
              }
              Some(enriched)
          }.flatten.iterator
        }
        
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
            .appName("Spark Streaming Enrichment")
            .config("spark.executor.memory", "1g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.executor.memoryOverhead", "1g")
            .config("spark.sql.streaming.stateStore.minDeltasForSnapshot", "10")
            .getOrCreate()

        import spark.implicits._

        implicit val marketEventEncoder = Encoders.kryo[MarketEvent]
        val tradeSchema = new StructType()
            .add("symbol", StringType)
            .add("price", DecimalType(18, 2))
            .add("volume", IntegerType)
            .add("trade_timestamp", LongType)

        val infoSchema = new StructType()
            .add("symbol", StringType)
            .add("company", StringType)
            .add("sector", StringType)
            .add("market_cap", LongType)
            .add("info_timestamp", LongType)

        val tradeStream = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "stock-trades")
            .option("startingOffsets", "earliest")
            .load()
        
        val infoStream = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "stock-info")
            .option("startingOffsets", "earliest")
            .load()

        val tradeDS: Dataset[TradeEvent] = tradeStream
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), tradeSchema).as("data"))
            .select("data.*") // Flatten the columns
            .as[TradeEvent]   // Convert to Case Class
        
        val infoDS: Dataset[InfoEvent] = infoStream
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), infoSchema).as("data"))
            .select("data.*") // Flatten the columns
            .as[InfoEvent]

        val allEventsDS: Dataset[MarketEvent] = tradeDS.map(t => t: MarketEvent)
            .union(infoDS.map(i => i: MarketEvent))

        val enrichedStream = allEventsDS
            .groupByKey(_.symbol) // Key by Symbol
            .flatMapGroupsWithState(
              OutputMode.Append(), 
              GroupStateTimeout.EventTimeTimeout() 
            )(updateState)

        val metricsDF = enrichedStream.withColumn("ts", (col("trade_timestamp") / 1000).cast("timestamp")).
                  withWatermark("ts", "5 minutes").
                  groupBy(window($"ts", "1 minute"), $"sector").agg(
                    count("*").as("trade_count"),
                    sum("trade_value").as("total_value"),
                    avg("price").as("avg_price"),
                    sum("volume").as("total_volume")
                  ).
                  select($"window.start".as("window_start"), $"window.end".as("window_end"), $"sector", $"trade_count", $"total_value", $"avg_price", $"total_volume")

        val rawWrite = enrichedStream.writeStream
            .foreachBatch { (batchDF: Dataset[EnrichedTrade], batchId: Long) => 
              batchDF.sparkSession.sparkContext.setLogLevel("WARN")

              batchDF.foreachPartition { (partition: Iterator[EnrichedTrade]) =>

                  val conn = java.sql.DriverManager.getConnection(
                    "jdbc:postgresql://postgres:5432/streaming", "postgres", "postgres"
                  )
                  conn.setAutoCommit(true)

                  val upsertSql = 
                    """
                    INSERT INTO enriched_trades (
                      symbol, price, volume, trade_timestamp, 
                      company, sector, market_cap, info_timestamp, trade_value
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (symbol, trade_timestamp, volume) DO NOTHING;
                    """.stripMargin

                  val stmt = conn.prepareStatement(upsertSql)

                  try {
                    partition.foreach { record =>
                         stmt.setString(1, record.symbol)
                         stmt.setBigDecimal(2, record.price.bigDecimal)
                         stmt.setInt(3, record.volume)
                         stmt.setTimestamp(4, new java.sql.Timestamp(record.trade_timestamp))
                         stmt.setString(5, record.company)
                         stmt.setString(6, record.sector)
                         stmt.setLong(7, record.market_cap) 
                         stmt.setTimestamp(8, new java.sql.Timestamp(record.info_timestamp))
                         stmt.setBigDecimal(9, record.trade_value.bigDecimal)

                         stmt.addBatch()
                    }

                    val results = stmt.executeBatch()

                  } catch {
                    case e: Exception => 
                      System.err.println("WORKER ERROR: JDBC UPSERT FAILED!")
                      e.printStackTrace() // Log errors to Spark Worker logs
                  } finally {
                    if (stmt != null) stmt.close()
                    if (conn != null) conn.close()
                  }
                  ()
                }
          }.option("checkpointLocation", "/opt/spark/checkpoints/scala-enriched").outputMode("append")
                .start()
                  

        val metricsWrite = metricsDF.writeStream.
            foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>

              batchDF.sparkSession.sparkContext.setLogLevel("WARN")
              
              val metricCount = batchDF.count()
              System.out.println(s"!!!!!!!!!! BATCH $batchId: Metrics Count = $metricCount !!!!!!!!!!")
              
              batchDF.foreachPartition { (partition: Iterator[Row]) =>
                  // 1. Database Connection Configuration
                  // Note: These are defined inside the partition to ensure they are available on the executor
                  val dbUrl = "jdbc:postgresql://postgres:5432/streaming"
                  val dbUser = "postgres"
                  val dbPass = "postgres"

                  // 2. Open Connection per Partition
                  val conn = java.sql.DriverManager.getConnection(dbUrl, dbUser, dbPass)
                  conn.setAutoCommit(true)

                  // 3. The PostgreSQL Upsert SQL (ON CONFLICT)
                  val upsertSql = 
                    """
                    INSERT INTO sector_metrics (
                      window_start, window_end, sector, trade_count, total_value, avg_price, total_volume
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (window_start, sector) 
                    DO UPDATE SET 
                      trade_count   = EXCLUDED.trade_count,
                      total_value   = EXCLUDED.total_value,
                      avg_price     = EXCLUDED.avg_price,
                      total_volume  = EXCLUDED.total_volume;
                    """.stripMargin

                  val stmt = conn.prepareStatement(upsertSql)

                  try {
                    partition.foreach { row =>
                         // Use .getAs[Type]("name") for name-based access
                         stmt.setTimestamp(1, row.getAs[java.sql.Timestamp]("window_start"))
                         stmt.setTimestamp(2, row.getAs[java.sql.Timestamp]("window_end"))
                         stmt.setString(3, row.getAs[String]("sector"))
                         stmt.setLong(4, row.getAs[Long]("trade_count"))

                         // Spark Row returns java.math.BigDecimal for decimal types
                         stmt.setBigDecimal(5, row.getAs[java.math.BigDecimal]("total_value"))
                         stmt.setBigDecimal(6, row.getAs[java.math.BigDecimal]("avg_price"))

                         stmt.setLong(7, row.getAs[Long]("total_volume"))

                         stmt.addBatch()
                    }

                    // 4. Execute all rows in this partition as a single network request
                    val results = stmt.executeBatch()
                    

                  } catch {
                    case e: Exception => 
                      System.err.println("WORKER ERROR: JDBC UPSERT FAILED!")
                      e.printStackTrace() // Log errors to Spark Worker logs
                  } finally {
                    // 5. Clean up resources
                    if (stmt != null) stmt.close()
                    if (conn != null) conn.close()
                  }
                  ()
                }
          }.option("checkpointLocation", "/opt/spark/checkpoints/scala-metrics").outputMode("update")
                .start()
      
      spark.streams.awaitAnyTermination()
    }
  }