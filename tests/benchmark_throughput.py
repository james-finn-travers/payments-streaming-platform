"""
Pipeline Throughput Benchmark
=============================
Measures the maximum transactions/second at each stage:

  1. Transaction generation  (CPU-only, no I/O)
  2. JSON serialization      (CPU-only)
  3. Kafka producer           (with live broker)
  4. Kafka consumer           (read-back speed)
  5. End-to-end produce→consume round-trip

Usage:
    python -m tests.benchmark_throughput
"""

import json
import os
import sys
import time
import statistics
import uuid
import logging
from contextlib import contextmanager
from typing import Callable

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.producer import generate_transaction, create_producer, KAFKA_BOOTSTRAP

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("benchmark")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
DIVIDER = "=" * 70
HEADER_FMT = "\n{div}\n  {title}\n{div}"


@contextmanager
def timer():
    """Context manager that tracks elapsed wall-clock time."""
    class _T:
        elapsed = 0.0
    t = _T()
    start = time.perf_counter()
    yield t
    t.elapsed = time.perf_counter() - start


def run_trial(fn: Callable, n: int) -> float:
    """Run *fn* n times and return txns/sec."""
    with timer() as t:
        for _ in range(n):
            fn()
    return n / t.elapsed if t.elapsed > 0 else float("inf")


def print_results(label: str, tps_values: list[float]):
    avg = statistics.mean(tps_values)
    med = statistics.median(tps_values)
    mn = min(tps_values)
    mx = max(tps_values)
    std = statistics.stdev(tps_values) if len(tps_values) > 1 else 0
    print(f"  {label}")
    print(f"    avg  = {avg:>12,.0f} txn/s")
    print(f"    med  = {med:>12,.0f} txn/s")
    print(f"    min  = {mn:>12,.0f} txn/s")
    print(f"    max  = {mx:>12,.0f} txn/s")
    print(f"    std  = {std:>12,.0f}")
    return avg


# ---------------------------------------------------------------------------
# Stage 1: Transaction Generation
# ---------------------------------------------------------------------------
def bench_generation(n: int = 10_000, trials: int = 5) -> float:
    print(HEADER_FMT.format(div=DIVIDER, title="Stage 1: Transaction Generation (CPU only)"))
    print(f"  Generating {n:,} transactions x {trials} trials …\n")

    results = []
    for i in range(trials):
        tps = run_trial(generate_transaction, n)
        results.append(tps)
        print(f"    trial {i+1}: {tps:>10,.0f} txn/s")

    print()
    return print_results("Generation throughput", results)


# ---------------------------------------------------------------------------
# Stage 2: JSON Serialization
# ---------------------------------------------------------------------------
def bench_serialization(n: int = 20_000, trials: int = 5) -> float:
    print(HEADER_FMT.format(div=DIVIDER, title="Stage 2: JSON Serialization"))

    # Pre-generate transactions so we only measure serialization
    txns = [generate_transaction() for _ in range(n)]
    print(f"  Serializing {n:,} pre-generated transactions x {trials} trials …\n")

    results = []
    for i in range(trials):
        with timer() as t:
            for txn in txns:
                json.dumps(txn).encode("utf-8")
        tps = n / t.elapsed
        results.append(tps)
        print(f"    trial {i+1}: {tps:>10,.0f} txn/s")

    print()
    return print_results("Serialization throughput", results)


# ---------------------------------------------------------------------------
# Stage 3: Kafka Producer
# ---------------------------------------------------------------------------
def bench_kafka_producer(n: int = 50_000, trials: int = 3) -> float:
    print(HEADER_FMT.format(div=DIVIDER, title="Stage 3: Kafka Producer (async send + flush)"))

    bench_topic = f"benchmark_{uuid.uuid4().hex[:8]}"
    print(f"  Topic: {bench_topic}")
    print(f"  Broker: {KAFKA_BOOTSTRAP}")
    print(f"  Sending {n:,} transactions x {trials} trials …\n")

    # Pre-generate transactions so generation time is excluded
    txns = [generate_transaction() for _ in range(n)]

    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=1,                              # acks=1 for throughput test
            batch_size=64 * 1024,                # 64 KB batches
            linger_ms=5,
            compression_type="lz4",
            buffer_memory=128 * 1024 * 1024,     # 128 MB
            max_in_flight_requests_per_connection=5,
        )
    except NoBrokersAvailable:
        print("  ⚠  Kafka broker not reachable — skipping producer benchmark.\n")
        return 0.0

    results = []
    for i in range(trials):
        with timer() as t:
            for txn in txns:
                producer.send(bench_topic, key=txn["user_id"], value=txn)
            producer.flush(timeout=60)
        tps = n / t.elapsed
        results.append(tps)
        print(f"    trial {i+1}: {tps:>10,.0f} txn/s  ({t.elapsed:.2f}s)")

    producer.close()
    print()
    avg = print_results("Kafka producer throughput", results)

    # Clean up benchmark topic
    _delete_topic(bench_topic)
    return avg


# ---------------------------------------------------------------------------
# Stage 4: Kafka Consumer
# ---------------------------------------------------------------------------
def bench_kafka_consumer(n: int = 50_000) -> float:
    print(HEADER_FMT.format(div=DIVIDER, title="Stage 4: Kafka Consumer (read-back)"))

    bench_topic = f"benchmark_cons_{uuid.uuid4().hex[:8]}"
    print(f"  Topic: {bench_topic}")
    print(f"  Producing {n:,} messages first …")

    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import NoBrokersAvailable

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=1,
            batch_size=64 * 1024,
            linger_ms=5,
            compression_type="lz4",
        )
    except NoBrokersAvailable:
        print("  ⚠  Kafka broker not reachable — skipping consumer benchmark.\n")
        return 0.0

    txns = [generate_transaction() for _ in range(n)]
    for txn in txns:
        producer.send(bench_topic, key=txn["user_id"], value=txn)
    producer.flush(timeout=60)
    producer.close()
    print(f"  Produced {n:,} messages. Now consuming …\n")

    consumer = KafkaConsumer(
        bench_topic,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        auto_offset_reset="earliest",
        group_id=f"bench-{uuid.uuid4().hex[:8]}",
        consumer_timeout_ms=5000,
        max_poll_records=1000,
        fetch_max_bytes=10 * 1024 * 1024,  # 10 MB
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    consumed = 0
    with timer() as t:
        for msg in consumer:
            consumed += 1
            if consumed >= n:
                break

    consumer.close()
    tps = consumed / t.elapsed if t.elapsed > 0 else 0
    print(f"    Consumed {consumed:,} messages in {t.elapsed:.2f}s")
    print(f"    Consumer throughput: {tps:>,.0f} txn/s\n")

    _delete_topic(bench_topic)
    return tps


# ---------------------------------------------------------------------------
# Stage 5: End-to-End (produce + consume)
# ---------------------------------------------------------------------------
def bench_end_to_end(n: int = 20_000) -> float:
    print(HEADER_FMT.format(div=DIVIDER, title="Stage 5: End-to-End (produce → consume)"))

    bench_topic = f"benchmark_e2e_{uuid.uuid4().hex[:8]}"
    print(f"  Topic: {bench_topic}")
    print(f"  {n:,} transactions end-to-end …\n")

    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    import threading

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=1,
            batch_size=64 * 1024,
            linger_ms=5,
            compression_type="lz4",
        )
    except NoBrokersAvailable:
        print("  ⚠  Kafka broker not reachable — skipping E2E benchmark.\n")
        return 0.0

    txns = [generate_transaction() for _ in range(n)]
    consumed_count = 0
    consumer_done = threading.Event()

    def consume_worker():
        nonlocal consumed_count
        consumer = KafkaConsumer(
            bench_topic,
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            auto_offset_reset="earliest",
            group_id=f"bench-e2e-{uuid.uuid4().hex[:8]}",
            consumer_timeout_ms=10000,
            max_poll_records=1000,
            fetch_max_bytes=10 * 1024 * 1024,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        for msg in consumer:
            consumed_count += 1
            if consumed_count >= n:
                break
        consumer.close()
        consumer_done.set()

    with timer() as t:
        # Start consumer in background thread
        consumer_thread = threading.Thread(target=consume_worker, daemon=True)
        consumer_thread.start()

        # Produce all messages
        for txn in txns:
            producer.send(bench_topic, key=txn["user_id"], value=txn)
        producer.flush(timeout=60)
        producer.close()

        # Wait for consumer to finish
        consumer_done.wait(timeout=120)

    tps = consumed_count / t.elapsed if t.elapsed > 0 else 0
    print(f"    Produced + consumed {consumed_count:,} msgs in {t.elapsed:.2f}s")
    print(f"    End-to-end throughput: {tps:>,.0f} txn/s\n")

    _delete_topic(bench_topic)
    return tps


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _delete_topic(topic: str):
    """Best-effort topic cleanup."""
    try:
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP.split(","))
        admin.delete_topics([topic])
        admin.close()
    except Exception:
        pass  # non-critical


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
def print_summary(results: dict):
    print(HEADER_FMT.format(div=DIVIDER, title="SUMMARY — Max Throughput Per Stage"))
    print()
    for stage, tps in results.items():
        bar = "█" * int(tps / max(results.values()) * 40) if tps > 0 else ""
        print(f"  {stage:<30s}  {tps:>12,.0f} txn/s  {bar}")
    print()

    bottleneck = min((v, k) for k, v in results.items() if v > 0)
    print(f"  ➜  Pipeline bottleneck: {bottleneck[1]}")
    print(f"  ➜  Max sustainable throughput: ~{bottleneck[0]:,.0f} txn/s")
    print(DIVIDER)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print(HEADER_FMT.format(div=DIVIDER, title="Payment Pipeline Throughput Benchmark"))
    print(f"  Kafka broker: {KAFKA_BOOTSTRAP}\n")

    results = {}
    results["1. Generation (CPU)"] = bench_generation()
    results["2. JSON serialization"] = bench_serialization()
    results["3. Kafka producer"] = bench_kafka_producer()
    results["4. Kafka consumer"] = bench_kafka_consumer()
    results["5. End-to-end (P→C)"] = bench_end_to_end()

    print_summary(results)


if __name__ == "__main__":
    main()
