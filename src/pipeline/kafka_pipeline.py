"""
kafka_pipeline.py
-----------------
Kafka Producer + Consumer for the price comparison pipeline.

What it does:
  1. Producer  → serialises each product as JSON → sends to 'raw-prices' topic
  2. Consumer  → reads back from 'raw-prices' topic
  3. If Kafka is not running → falls back to in-memory pass-through
     (terminal shows exactly what happened)

Topic used: raw-prices
"""

import json
import os
import time
from datetime import datetime
from typing import List, Dict, Callable


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW       = os.getenv("KAFKA_TOPIC_RAW", "raw-prices")


class KafkaPipeline:
    def __init__(self, log_callback: Callable):
        self.log      = log_callback
        self._producer = None
        self._available = False
        self._try_connect()

    # ── Connection ─────────────────────────────────────────────────────────────
    def _try_connect(self):
        try:
            from kafka import KafkaProducer, KafkaConsumer
            from kafka.errors import NoBrokersAvailable

            test_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                request_timeout_ms=3000,
                max_block_ms=3000,
            )
            test_producer.close()
            self._available = True
            self.log(f"  [Kafka] ✅ Connected to broker at {KAFKA_BOOTSTRAP}")
        except Exception as e:
            self._available = False
            self.log(f"  [Kafka] ⚠️  Broker not reachable ({KAFKA_BOOTSTRAP})")
            self.log(f"  [Kafka]    Reason: {str(e)[:60]}")
            self.log(f"  [Kafka]    Fallback: in-memory pass-through (pipeline continues)")

    # ── Main API ───────────────────────────────────────────────────────────────
    def send_and_receive(self, products: List[Dict]) -> List[Dict]:
        """
        Send all products to Kafka and read them back.
        Falls back gracefully if Kafka is unavailable.
        """
        self.log("\n" + "=" * 52)
        self.log("PHASE 2 — KAFKA PIPELINE")
        self.log(f"Broker  : {KAFKA_BOOTSTRAP}")
        self.log(f"Topic   : {TOPIC_RAW}")
        self.log(f"Messages: {len(products)}")
        self.log("=" * 52)

        if not products:
            self.log("  [Kafka] No products to send")
            return []

        if self._available:
            return self._kafka_roundtrip(products)
        else:
            return self._memory_passthrough(products)

    # ── Real Kafka path ────────────────────────────────────────────────────────
    def _kafka_roundtrip(self, products: List[Dict]) -> List[Dict]:
        from kafka import KafkaProducer, KafkaConsumer

        # ── Produce ──────────────────────────────────────────────────────────
        self.log(f"  [Kafka] Producing {len(products)} messages → topic '{TOPIC_RAW}'")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
        )
        sent = 0
        for p in products:
            key = f"{p.get('platform','')}:{p.get('product_name','')}:{p.get('size_label','')}"
            future = producer.send(TOPIC_RAW, key=key, value=p)
            sent += 1
        producer.flush()
        producer.close()
        self.log(f"  [Kafka] ✅ {sent} messages published to '{TOPIC_RAW}'")

        # ── Consume ──────────────────────────────────────────────────────────
        self.log(f"  [Kafka] Consuming messages from '{TOPIC_RAW}'...")
        time.sleep(0.5)  # brief wait for broker to commit

        consumer = KafkaConsumer(
            TOPIC_RAW,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=f"price-compare-{int(time.time())}",  # unique group = read all
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=3000,
        )
        consumed = []
        for msg in consumer:
            consumed.append(msg.value)
            if len(consumed) >= len(products):
                break
        consumer.close()

        self.log(f"  [Kafka] ✅ {len(consumed)} messages consumed from '{TOPIC_RAW}'")
        self.log(f"  [Kafka] Offset committed — messages acknowledged")
        return consumed if consumed else products  # fallback if consume was empty

    # ── Fallback path ──────────────────────────────────────────────────────────
    def _memory_passthrough(self, products: List[Dict]) -> List[Dict]:
        self.log(f"  [Kafka] FALLBACK: Simulating Kafka message flow in-memory")
        self.log(f"  [Kafka] Serialising {len(products)} records to JSON...")
        # Simulate serialise → deserialise (same as real Kafka does)
        serialised   = [json.dumps(p, default=str).encode("utf-8") for p in products]
        deserialised = [json.loads(s.decode("utf-8")) for s in serialised]
        self.log(f"  [Kafka] {len(serialised)} messages serialised (JSON, UTF-8)")
        self.log(f"  [Kafka] {len(deserialised)} messages deserialised")
        self.log(f"  [Kafka] ✅ Pass-through complete — {len(deserialised)} records ready")
        self.log(f"  [Kafka] TIP: Run 'docker-compose up kafka' for real Kafka broker")
        return deserialised
