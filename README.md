# streams-stateful-enrichment-aggregation-methods

# 🚀 Financial Streaming Pipeline: Multi-Architecture Benchmarking

This repository explores the evolution of real-time data engineering patterns, specifically focusing on the challenges of **Enrichment** and **Stateful Aggregation**. We benchmark three distinct ways to handle high-velocity stock market data using PySpark, Apache Flink, and Scala Spark.

## 🛠 The Challenge
Ingest two high-frequency Kafka streams:
1. **Stock Trades**: Raw execution data (Symbol, Price, Volume, Timestamp).
2. **Market Info**: Metadata (Symbol, Company Name, Sector, Market Cap).

**The Goal:** Produce an enriched stream and a windowed metrics table (1-minute tumbling windows) with strict **idempotency** and **fault tolerance**.

---

## 🏗 Architectural Evolution

### 1. PySpark + Delta Lake (The Staged Pattern)
Initially, we attempted a direct PySpark stream with two sinks. However, we found that the **Py4J gateway overhead** and the management of two stateful streams (enrichment + aggregation) caused the job to lag significantly behind its `TriggerTime`.

**The Solution:**
* **Job A:** PySpark reads from Kafka and persists raw enriched data into a **Delta Lake** table.
* **Job B:** A separate consumer reads from Delta and aggregates metrics into Postgres.

* **Pros:** Highly resilient; Delta Lake provides a reliable buffer and "Time Travel" capabilities.
* **Cons:** Highest resource footprint (two jobs + storage I/O). The "two-hop" approach adds significant end-to-end latency.

### 2. Apache Flink (The Unified Pattern)
A transition to a "True Streaming" engine using Flink's `connect` and `KeyedProcessFunction`.

* **Pros:** Lowest possible latency (event-at-a-time). Native "fan-out" to multiple sinks is seamless.
* **Cons:** High architectural complexity. Requires a dedicated JobManager/TaskManager infrastructure that can be heavy for small-scale deployments.

### 3. Scala Spark (The Optimized JVM Pattern) - *Current Implementation*
We moved the logic to Scala to eliminate Python serialization bottlenecks and used a "Hard Way" approach: two independent `writeStream` sinks within a single JVM application.

* **Pros:** Performance is nearly on par with Flink for micro-batching. Directly leverages JVM memory for state, reducing overhead.
* **Cons:** Requires manual management of two checkpoints and careful tuning of stateful union logic.

---

## 📊 Architecture Diagram

### 1. Pyspark
```mermaid
graph LR
    subgraph Sources
        K1[Kafka: Stock Trades]
        K2[Kafka: Market Info]
    end

    subgraph JobA[Job A: Ingest & Enrich]
        UJ[PySpark Enrichment]
    end

    subgraph Storage[Intermediate Storage]
        DL[(Delta Lake Table\n'Silver Layer')]
    end

    subgraph JobB[Job B: Raw Sink]
        WR[Enriched to DB]
    end

    subgraph JobC[Job C: Metrics Sink]
        WA[Windowed Aggregation]
    end

    subgraph Sinks
        PG_E[(Postgres: enriched)]
        PG_M[(Postgres: metrics)]
    end

    %% Ingestion Flow
    K1 & K2 --> UJ
    UJ -->|writeStream| DL

    %% Parallel Reading Flow
    DL -->|readStream B| WR
    DL -->|readStream C| WA

    %% Output Flow
    WR -->|upsert| PG_E
    WA -->|upsert| PG_M

    style Storage fill:#4A6B8A,stroke:#fff,color:white
```

### 2. Flink (Unified Pattern)
```mermaid
    graph LR
    subgraph Sources
        K1[Kafka: Stock Trades]
        K2[Kafka: Market Info]
    end

    subgraph FlinkCluster[Flink Cluster]
        KPF[KeyedProcessFunction]
    end

    subgraph Sinks
        PG_R[(Postgres: enriched_trades)]
        PG_M[(Postgres: sector_metrics)]
    end

    K1 & K2 -->|Connect| KPF
    KPF -->|Output 1| PG_R
    KPF -->|Output 2| PG_M
```

### 3. Scala Spark 
```mermaid
    graph LR
    subgraph Sources
        K1[Kafka: Stock Trades]
        K2[Kafka: Market Info]
    end

    subgraph SparkJVM[Single Scala Spark JVM]
        subgraph Plan1[Execution Plan 1: Raw Lane]
            ES1[Enrichment State]
        end
        
        subgraph Plan2[Execution Plan 2: Metrics Lane]
            ES2[Enrichment State] --> WA[Windowed Aggregation]
        end
    end

    subgraph Sinks
        PG_R[(Postgres: enriched_trades)]
        PG_M[(Postgres: sector_metrics)]
    end

    K1 & K2 -->|Plan 1 Re-compute| ES1
    K1 & K2 -->|Plan 2 Re-compute| ES2
    ES1 -->|writeStream 1| PG_R
    WA -->|writeStream 2| PG_M
```


## Instructions
### Docker
    docker compose up -d
### Pyspark submit
    make sure its root dir
    docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark/jobs/enrichment
    docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark/jobs/metric
### Scala spark
    cd scala-spark/
    sbt clean package
    docker cp target/scala-2.12/sparkscalademo_2.12-0.1.jar spark-master:/opt/spark/jobs/
    cd .. 
    docker exec -it spark-master spark-submit --master spark://spark-master:7077 --class sparkStreamingEnrich /opt/spark/jobs/sparkscalademo_2.12-0.1.jar
### Flink submit
    cd flink-job/
    mvn clean package
    docker cp target/flink-enrichment-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/
    cd ..
    docker exec -it flink-jobmanager flink run -c com.streaming.project.FlinkEnrichmentJob /opt/flink/flink-enrichment-1.0-SNAPSHOT.jar
## Produce data after job runs
    ""root dir""
    python3 mock_data/stock_info.py
    ""wait a couple seconds to have all enrichment info rows for all symbols sent up to kafka topic before running the stock_trade mock data. but keep both running for regular info updates""
    python3 mock_data/stock_trades.py
