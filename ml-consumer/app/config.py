from __future__ import annotations

import os
from dataclasses import dataclass


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return float(raw)


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka-1:9092,kafka-2:9092,kafka-3:9092",
    )
    schema_registry_url: str = os.getenv(
        "SCHEMA_REGISTRY_URL", "http://schema-registry:8081"
    )
    input_topic: str = os.getenv("INPUT_TOPIC", "enriched-telemetry")
    output_topic: str = os.getenv("OUTPUT_TOPIC", "pit-predictions")
    consumer_group: str = os.getenv("CONSUMER_GROUP", "aerostream-ml-consumer")
    pit_threshold: float = _get_float("PIT_THRESHOLD", 0.65)
    slowdown_ms: int = _get_int("CONSUMER_SLOWDOWN_MS", 0)
    model_path: str = os.getenv("MODEL_PATH", "/app/models/pit_rf.pkl")
    model_version: str = os.getenv("MODEL_VERSION", "rf-1.0.0")
    client_id: str = os.getenv("KAFKA_CLIENT_ID", "aerostream-ml-consumer")
