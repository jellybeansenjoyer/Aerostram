"""Tail-read Kafka topics with a bounded poll deadline (preview API)."""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

log = logging.getLogger(__name__)


def _tail_raw(
    bootstrap: str,
    topic: str,
    limit: int,
    deadline_sec: float,
    decode: Callable[[bytes], dict[str, Any]],
) -> list[dict[str, Any]]:
    """Seek near log end per partition, then poll until limit rows or deadline."""
    deadline = time.monotonic() + max(0.1, deadline_sec)
    group_id = f"aerostream-bff-{uuid.uuid4().hex[:12]}"
    conf = {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 10000,
        # Fail faster when brokers are wrong/unreachable (avoid browser “hangs”).
        "socket.timeout.ms": 15000,
        "connections.max.idle.ms": 30000,
        "metadata.max.age.ms": 900000,
    }
    consumer = Consumer(conf)
    out: list[dict[str, Any]] = []
    try:
        list_timeout = max(1.0, min(10.0, float(deadline_sec)))
        meta = consumer.list_topics(topic, timeout=list_timeout)
        if topic not in meta.topics:
            log.warning("topic %s not found in metadata", topic)
            return []
        partitions = sorted(meta.topics[topic].partitions.keys())
        if not partitions:
            return []
        tps = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(tps)
        # librdkafka requires at least one poll() after assign before seek is valid.
        consumer.poll(0.0)

        per_partition = max(1, (limit + len(partitions) - 1) // len(partitions) + 5)
        for tp in tps:
            wm_timeout = max(1.0, min(10.0, deadline - time.monotonic()))
            try:
                low, high = consumer.get_watermark_offsets(tp, timeout=wm_timeout)
            except KafkaException as e:
                log.warning("watermark %s: %s", tp, e)
                continue
            if high <= low:
                target = low
            else:
                target = max(low, high - per_partition)
            try:
                consumer.seek(TopicPartition(topic, tp.partition, target))
            except KafkaException as e:
                log.warning("seek %s partition=%s to %s failed: %s — retrying at low=%s", topic, tp.partition, target, e, low)
                try:
                    consumer.seek(TopicPartition(topic, tp.partition, low))
                except KafkaException as e2:
                    log.warning("seek retry failed partition=%s: %s", tp.partition, e2)

        while time.monotonic() < deadline and len(out) < limit:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            msg = consumer.poll(timeout=min(1.0, remaining))
            if msg is None:
                continue
            if msg.error():
                log.debug("poll error: %s", msg.error())
                continue
            raw = msg.value()
            if raw is None:
                continue
            try:
                row = decode(raw)
                row["_kafka_partition"] = msg.partition()
                row["_kafka_offset"] = msg.offset()
                out.append(row)
            except Exception:
                log.exception("decode failed for topic=%s partition=%s", topic, msg.partition())
        return out[:limit]
    finally:
        consumer.close()


def tail_json_values(
    bootstrap: str,
    topic: str,
    limit: int,
    deadline_sec: float,
) -> list[dict[str, Any]]:
    def decode(raw: bytes | None) -> dict[str, Any]:
        if not raw:
            return {}
        text = raw.decode("utf-8", errors="replace")
        data = json.loads(text)
        if not isinstance(data, dict):
            return {"_raw": data}
        return data

    return _tail_raw(bootstrap, topic, limit, deadline_sec, decode)


def tail_avro_values(
    bootstrap: str,
    schema_registry_url: str,
    topic: str,
    limit: int,
    deadline_sec: float,
) -> list[dict[str, Any]]:
    sr = SchemaRegistryClient({"url": schema_registry_url})
    deser = AvroDeserializer(sr)

    def decode(raw: bytes | None) -> dict[str, Any]:
        if not raw:
            return {}
        ctx = SerializationContext(topic, MessageField.VALUE)
        obj = deser(raw, ctx)
        if isinstance(obj, dict):
            return obj
        return {"value": obj}

    return _tail_raw(bootstrap, topic, limit, deadline_sec, decode)
