"""
Real-Time Customer Transaction Analytics Pipeline — Stream Processor

PyFlink job that:
  1. Consumes raw transactions from Kafka topic ``transactions``.
  2. Applies tumbling-window aggregations (daily spend per user).
  3. Computes z-score anomaly flags (amount > 3 SD from user mean).
  4. Writes enriched events to Kafka topic ``transactions_enriched``.

Requires: Apache Flink 1.18+, PyFlink, kafka-connector JAR.
"""

import os
import json
import logging

from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import (
    MapFunction,
    ProcessWindowFunction,
    KeyedProcessFunction,
    RuntimeContext,
    ValueStateDescriptor,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SOURCE_TOPIC = os.getenv("KAFKA_SOURCE_TOPIC", "transactions")
SINK_TOPIC = os.getenv("KAFKA_SINK_TOPIC", "transactions_enriched")
ANOMALY_TOPIC = os.getenv("KAFKA_ANOMALY_TOPIC", "anomalies")
CONSUMER_GROUP = os.getenv("FLINK_CONSUMER_GROUP", "flink-txn-processor")
PARALLELISM = int(os.getenv("FLINK_PARALLELISM", "2"))
ANOMALY_SD_THRESHOLD = float(os.getenv("ANOMALY_SD_THRESHOLD", "3.0"))
WINDOW_SIZE_SECONDS = int(os.getenv("WINDOW_SIZE_SECONDS", "86400"))  # 1 day
DAILY_SPEND_TOPIC = os.getenv("KAFKA_DAILY_SPEND_TOPIC","daily_spend")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("txn-processor")


# ---------------------------------------------------------------------------
# UDFs
# ---------------------------------------------------------------------------
class ParseTransaction(MapFunction):
    """Deserialise JSON string into a Python dict."""

    def map(self, value: str):
        try:
            txn = json.loads(value)
            return txn
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning("Skipping malformed record: %s", exc)
            return None


class AnomalyDetector(KeyedProcessFunction):
    """
    Stateful per-user z-score anomaly detector.

    Maintains running mean and variance via Welford's online algorithm.
    Flags transactions where amount > mean + ANOMALY_SD_THRESHOLD * stddev.
    """

    def open(self, runtime_context: RuntimeContext):
        # state: (count, mean, M2)  — Welford accumulators
        self.stats = runtime_context.get_state(
            ValueStateDescriptor("user_stats", Types.PICKLED_BYTE_ARRAY())
        )

    def process_element(self, txn: dict, ctx):
        if txn is None:
            return

        amount = txn.get("amount", 0.0)
        state = self.stats.value()

        if state is None:
            count, mean, m2 = 0, 0.0, 0.0
        else:
            count, mean, m2 = state

        # Welford update
        count += 1
        delta = amount - mean
        mean += delta / count
        delta2 = amount - mean
        m2 += delta * delta2

        self.stats.update((count, mean, m2))

        # Compute z-score (need at least 2 observations)
        is_anomaly = False
        z_score = 0.0
        if count >= 2:
            variance = m2 / (count - 1)
            stddev = variance ** 0.5
            if stddev > 0:
                z_score = (amount - mean) / stddev
                is_anomaly = abs(z_score) > ANOMALY_SD_THRESHOLD

        enriched = {
            **txn,
            "user_txn_count": count,
            "user_running_mean": round(mean, 2),
            "z_score": round(z_score, 4),
            "is_anomaly": is_anomaly,
        }

        yield enriched


class WindowAggregate(ProcessWindowFunction):
    """Tumbling-window aggregation: daily spend per user."""

    def process(self, key, context, elements):
        total = sum(e.get("amount", 0) for e in elements if e)
        count = len([e for e in elements if e])
        window_start = context.window().start
        window_end = context.window().end

        yield {
            "user_id": key,
            "window_start": window_start,
            "window_end": window_end,
            "txn_count": count,
            "total_spend": round(total, 2),
            "avg_spend": round(total / count, 2) if count else 0,
        }


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(30_000)  # 30-second checkpoint interval

    # --- Kafka source ---
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(SOURCE_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )



    raw_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "KafkaSource")

    # --- Parse JSON ---
    parsed = raw_stream.map(ParseTransaction()).filter(lambda x: x is not None)
    
    watermarked = parsed.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            lambda txn, _: int(txn.get("timestamp",0)*1000)
        )
    )

    # --- Anomaly detection (keyed by user_id) ---
    enriched = (
        watermarked
        .key_by(lambda txn: txn["user_id"])
        .process(AnomalyDetector())
    )
    
    # --- Daily spend windows per user
    daily_spend = (
        watermarked
        .key_by(lambda txn: txn["user_id"])
        .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
        .process(WindowAggregate())
    )
    
    # --- Daily spend -> Kafka topic --- 
    daily_spend_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(DAILY_SPEND_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    daily_spend.map(lambda agg: json.dumps(agg)).sink_to(daily_spend_sink)    

    # --- Kafka sink for enriched events ---
    enriched_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(SINK_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    enriched.map(lambda txn: json.dumps(txn)).sink_to(enriched_sink)

    # --- Anomaly branch → separate topic ---
    anomalies = enriched.filter(lambda txn: txn.get("is_anomaly", False))

    anomaly_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(ANOMALY_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    anomalies.map(lambda txn: json.dumps(txn)).sink_to(anomaly_sink)

    logger.info(
        "Pipeline built: %s → parse → anomaly detect → %s / %s",
        SOURCE_TOPIC,
        SINK_TOPIC,
        ANOMALY_TOPIC,
    )

    return env


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Starting Flink transaction processor …")
    pipeline_env = build_pipeline()
    pipeline_env.execute("TxnAnalyticsPipeline")
