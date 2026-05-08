from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from prometheus_client import Counter

from app.config import Settings
from app.predictor import Predictor

log = logging.getLogger(__name__)


def _load_schema_text() -> str:
    base = Path(__file__).resolve().parent.parent
    p = base / "schemas" / "PitPrediction.avsc"
    return p.read_text(encoding="utf-8")


def run_consumer_loop(
    settings: Settings,
    predictor: Predictor,
    on_ready: Callable[[], None],
    consumed_counter: Counter,
    emitted_counter: Counter,
    error_counter: Counter,
    stop_event: threading.Event,
) -> None:
    sr_conf = {"url": settings.schema_registry_url}
    schema_registry_client = SchemaRegistryClient(sr_conf)
    avro_deserializer = AvroDeserializer(schema_registry_client)
    schema_str = _load_schema_text()
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    consumer_conf = {
        "bootstrap.servers": settings.kafka_bootstrap,
        "group.id": settings.consumer_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "client.id": settings.client_id,
    }
    producer_conf = {
        "bootstrap.servers": settings.kafka_bootstrap,
        "client.id": settings.client_id + "-producer",
    }

    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    consumer.subscribe([settings.input_topic])
    log.info(
        "Subscribed to %s -> %s",
        settings.input_topic,
        settings.output_topic,
    )

    ready_notified = False

    def delivery_report(err, msg) -> None:  # noqa: ANN001
        if err is not None:
            log.warning("Delivery failed: %s", err)
            error_counter.inc()

    try:
        while not stop_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if not ready_notified and consumer.assignment():
                ready_notified = True
                on_ready()
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Consumer error: %s", msg.error())
                error_counter.inc()
                continue

            try:
                ctx = SerializationContext(msg.topic(), MessageField.VALUE)
                event = avro_deserializer(msg.value(), ctx)
                if not isinstance(event, dict):
                    error_counter.inc()
                    continue
            except Exception:
                log.exception("Deserialize failed")
                error_counter.inc()
                continue

            consumed_counter.inc()
            try:
                prob = predictor.pit_probability(event)
                recommend = prob >= settings.pit_threshold
                out = {
                    "car_id": str(event["car_id"]),
                    "timestamp_ms": int(event["timestamp_ms"]),
                    "pit_probability": prob,
                    "recommend_pit": recommend,
                    "model_version": settings.model_version,
                }
                payload = avro_serializer(
                    out,
                    SerializationContext(
                        settings.output_topic, MessageField.VALUE
                    ),
                )
                key = str(event["car_id"]).encode("utf-8")
                producer.produce(
                    settings.output_topic,
                    key=key,
                    value=payload,
                    on_delivery=delivery_report,
                )
                producer.poll(0)
                emitted_counter.inc()
            except Exception:
                log.exception("Inference or produce failed")
                error_counter.inc()
                continue

            if settings.slowdown_ms > 0:
                time.sleep(settings.slowdown_ms / 1000.0)

    finally:
        producer.flush(timeout=10)
        consumer.close()
