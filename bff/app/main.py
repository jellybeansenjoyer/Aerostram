import logging

from confluent_kafka import KafkaException
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.json_sanitize import sanitize_for_json
from app.kafka_recent import tail_avro_values, tail_json_values

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(title="AeroStream Dashboard BFF", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root() -> dict[str, object]:
    """Always returns JSON so hitting http://host:8089/ in a browser shows a useful payload."""
    return {
        "service": "aerostream-dashboard-bff",
        "docs": "/docs",
        "health": "/health",
        "preview": {
            "pit_predictions": "/api/v1/pit-predictions/recent?limit=5",
            "stream_aggregates": "/api/v1/stream-aggregates/recent?limit=5",
        },
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v1/pit-predictions/recent")
def pit_predictions_recent(
    limit: int = Query(20, ge=1),
) -> dict:
    lim = min(limit, settings.max_limit)
    try:
        items = tail_avro_values(
            settings.kafka_bootstrap_servers,
            settings.schema_registry_url,
            settings.pit_predictions_topic,
            lim,
            settings.poll_deadline_sec,
        )
    except KafkaException as e:
        log.exception("pit-predictions tail failed (Kafka)")
        raise HTTPException(
            status_code=503,
            detail=(
                "Kafka unreachable or topic metadata failed. "
                "If the BFF runs on your machine (not in Docker), set KAFKA_BOOTSTRAP_SERVERS to your broker "
                "advertised addresses (e.g. localhost:9092,localhost:9094,localhost:9096). "
                f"Configured: {settings.kafka_bootstrap_servers!r}. Underlying error: {e!s}"
            ),
        ) from e
    except Exception as e:
        log.exception("pit-predictions tail failed")
        raise HTTPException(
            status_code=503,
            detail=(
                "Could not read pit predictions (Schema Registry or deserialization). "
                f"SCHEMA_REGISTRY_URL={settings.schema_registry_url!r}. Error: {e!s}"
            ),
        ) from e
    return {
        "topic": settings.pit_predictions_topic,
        "limit": lim,
        "items": sanitize_for_json(items),
    }


@app.get("/api/v1/stream-aggregates/recent")
def stream_aggregates_recent(
    limit: int = Query(20, ge=1),
) -> dict:
    lim = min(limit, settings.max_limit)
    try:
        items = tail_json_values(
            settings.kafka_bootstrap_servers,
            settings.stream_aggregates_topic,
            lim,
            settings.poll_deadline_sec,
        )
    except KafkaException as e:
        log.exception("stream-aggregates tail failed (Kafka)")
        raise HTTPException(
            status_code=503,
            detail=(
                "Kafka unreachable or topic metadata failed. "
                "If the BFF runs on your machine, set KAFKA_BOOTSTRAP_SERVERS to localhost broker ports. "
                f"Configured: {settings.kafka_bootstrap_servers!r}. Error: {e!s}"
            ),
        ) from e
    except Exception as e:
        log.exception("stream-aggregates tail failed")
        raise HTTPException(status_code=503, detail=str(e)) from e
    return {
        "topic": settings.stream_aggregates_topic,
        "limit": lim,
        "items": sanitize_for_json(items),
    }
