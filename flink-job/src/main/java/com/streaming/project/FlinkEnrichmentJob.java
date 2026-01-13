package com.streaming.project;

import com.streaming.project.model.Trade;
import com.streaming.project.model.Info;
import com.streaming.project.model.EnrichedTrade;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Duration;

public class FlinkEnrichmentJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 1. Setup JSON Deserializers
        JsonDeserializationSchema<Trade> tradeSchema = new JsonDeserializationSchema<>(Trade.class);
        JsonDeserializationSchema<Info> infoSchema = new JsonDeserializationSchema<>(Info.class);

        // 2. Define Kafka Sources
        KafkaSource<Trade> tradeSource = KafkaSource.<Trade>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("stock-trades")
                .setGroupId("flink-enrichment-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("commit.offsets.on.checkpoint", "false")
                .setProperty("auto.offset.reset", "earliest")
                .setValueOnlyDeserializer(tradeSchema)
                .build();

        KafkaSource<Info> infoSource = KafkaSource.<Info>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("stock-info")
                .setGroupId("flink-info-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("commit.offsets.on.checkpoint", "false")
                .setProperty("auto.offset.reset", "earliest")
                .setValueOnlyDeserializer(infoSchema)
                .build();

        DataStream<Trade> tradeStream = env.fromSource(tradeSource, WatermarkStrategy.noWatermarks(), "TradeSource")
                .keyBy(Trade::getSymbol);

        DataStream<Info> infoStream = env.fromSource(infoSource, WatermarkStrategy.noWatermarks(), "InfoSource")
                .keyBy(Info::getSymbol);

        // 3. Enrichment Step
        DataStream<EnrichedTrade> enrichedStream = tradeStream
                .connect(infoStream)
                .process(new EnrichmentFunction());

        // 4. Assign Watermarks AFTER Join
        DataStream<EnrichedTrade> timedStream = enrichedStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<EnrichedTrade>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.getTrade_timestamp())
                                .withIdleness(Duration.ofMinutes(1))
                );

        // 5. SINK 1: Raw Enriched Data to Postgres
        enrichedStream.addSink(JdbcSink.sink(
                "INSERT INTO flink_enriched_trades (symbol, price, volume, trade_timestamp, company, sector, market_cap, info_timestamp, trade_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, t) -> {
                    ps.setString(1, t.getSymbol());
                    ps.setBigDecimal(2, t.getPrice());
                    ps.setInt(3, t.getVolume());
                    Long tradeTimestampEpoch = t.getTrade_timestamp();
                    if (tradeTimestampEpoch != null && tradeTimestampEpoch > 0) {
                        ps.setTimestamp(4, new Timestamp(tradeTimestampEpoch));
                    } else {
                        ps.setNull(4, java.sql.Types.TIMESTAMP);
                    }
                    ps.setString(5, t.getCompany());
                    ps.setString(6, t.getSector());
                    ps.setLong(7, t.getMarket_cap());
                    Long infoTimestampEpoch = t.getInfo_timestamp();
                    if (infoTimestampEpoch != null && infoTimestampEpoch > 0) {
                        ps.setTimestamp(8, new Timestamp(infoTimestampEpoch));
                    } else {
                        ps.setNull(8, java.sql.Types.TIMESTAMP);
                    }
                    ps.setBigDecimal(9, t.getTrade_value());
                },
                getJdbcOptions()
        ));

        // 6. SINK 2: Sector Metrics (1-Minute Aggregation)
        timedStream
                .keyBy(EnrichedTrade::getSector)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.minutes(5))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(30)))
                .aggregate(new SectorAggregator(), new SectorWindowProcessor())
                .addSink(JdbcSink.sink(
                        "INSERT INTO flink_sector_metrics (window_start, window_end, sector, trade_count, total_value, avg_price, total_volume) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (window_start, sector) DO UPDATE SET " +
                        "trade_count = EXCLUDED.trade_count, total_value = EXCLUDED.total_value, " +
                        "avg_price = EXCLUDED.avg_price, total_volume = EXCLUDED.total_volume",
                        (ps, m) -> {
                            ps.setTimestamp(1, new Timestamp(m.windowStart));
                            ps.setTimestamp(2, new Timestamp(m.windowEnd));
                            ps.setString(3, m.sector);
                            ps.setInt(4, m.tradeCount);
                            ps.setBigDecimal(5, m.totalValue);
                            ps.setBigDecimal(6, m.avgPrice);
                            ps.setLong(7, m.totalVolume);
                        },
                        getJdbcOptions()
                ));

        env.execute("Flink Real-Time Enrichment & Metrics");
    }

    // --- Sector Metric Logic ---

    public static class SectorMetric {
        public long windowStart;
        public long windowEnd;
        public String sector;
        public int tradeCount;
        public BigDecimal totalValue;
        public BigDecimal avgPrice;
        public long totalVolume;

        public SectorMetric(long start, long end, String s, int count, BigDecimal val, BigDecimal avg, long vol) {
            this.windowStart = start; this.windowEnd = end; this.sector = s;
            this.tradeCount = count; this.totalValue = val; this.avgPrice = avg; this.totalVolume = vol;
        }
    }

    public static class SectorAccumulator {
        public int count = 0;
        public BigDecimal totalValue = BigDecimal.ZERO;
        public long totalVolume = 0L;
        public BigDecimal sumPrice = BigDecimal.ZERO;
    }

    public static class SectorAggregator implements AggregateFunction<EnrichedTrade, SectorAccumulator, SectorAccumulator> {
        @Override public SectorAccumulator createAccumulator() { return new SectorAccumulator(); }
        @Override public SectorAccumulator add(EnrichedTrade val, SectorAccumulator acc) {
            acc.count++;
            acc.totalValue = acc.totalValue.add(val.getTrade_value());
            acc.totalVolume += val.getVolume();
            acc.sumPrice = acc.sumPrice.add(val.getPrice());
            return acc;
        }
        @Override public SectorAccumulator getResult(SectorAccumulator acc) { return acc; }
        @Override public SectorAccumulator merge(SectorAccumulator a, SectorAccumulator b) {
            a.count += b.count;
            a.totalValue = a.totalValue.add(b.totalValue);
            a.totalVolume += b.totalVolume;
            a.sumPrice = a.sumPrice.add(b.sumPrice);
            return a;
        }
    }

    public static class SectorWindowProcessor extends ProcessWindowFunction<SectorAccumulator, SectorMetric, String, TimeWindow> {
        @Override
        public void process(String sector, Context ctx, Iterable<SectorAccumulator> elements, Collector<SectorMetric> out) {
            SectorAccumulator acc = elements.iterator().next();
            BigDecimal avg = acc.sumPrice.divide(new BigDecimal(acc.count), 8, RoundingMode.HALF_UP);
            out.collect(new SectorMetric(ctx.window().getStart(), ctx.window().getEnd(), sector, acc.count, acc.totalValue, avg, acc.totalVolume));
        }
    }

    // --- Existing Enrichment Logic (Unchanged) ---

    public static class EnrichmentFunction extends KeyedCoProcessFunction<String, Trade, Info, EnrichedTrade> {
        private transient ValueState<Info> lastKnownInfo;
        private transient Counter counter;

        @Override
        public void open(Configuration config) {
            lastKnownInfo = getRuntimeContext().getState(new ValueStateDescriptor<>("info", Info.class));
            this.counter = getRuntimeContext().getMetricGroup().counter("total_enriched_records");
        }

        @Override
        public void processElement1(Trade trade, Context ctx, Collector<EnrichedTrade> out) throws Exception {
            Info info = lastKnownInfo.value();
            String sector = (info != null) ? info.getSector() : "Unknown";
            counter.inc();

            String company = "Unknown";
            long marketCap = 0L;
            long infoTs = 0L;
            BigDecimal tradeValue = trade.getPrice().multiply(BigDecimal.valueOf(trade.getVolume()));
            if (info != null) {
                company = info.getCompany();
                sector = info.getSector();
                marketCap = info.getMarket_cap();
                infoTs = info.getInfo_timestamp();
            }

            out.collect(new EnrichedTrade(
                trade.getSymbol(), trade.getPrice(), trade.getVolume(), trade.getTrade_timestamp(),
                company, sector, marketCap, infoTs, tradeValue
            ));
        }

        @Override
        public void processElement2(Info info, Context ctx, Collector<EnrichedTrade> out) throws Exception {
            Info current = lastKnownInfo.value();
            if (current == null || info.getInfo_timestamp() > current.getInfo_timestamp()) {
                lastKnownInfo.update(info);
            }
        }
    }

    private static JdbcConnectionOptions getJdbcOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://postgres:5432/streaming")
                .withDriverName("org.postgresql.Driver")
                .withUsername("postgres")
                .withPassword("postgres")
                .build();
    }
}