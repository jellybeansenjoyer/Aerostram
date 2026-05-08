from __future__ import annotations

import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from starlette.responses import Response

from app.config import Settings
from app.kafka_worker import run_consumer_loop
from app.predictor import Predictor

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

MESSAGES_CONSUMED = Counter(
    "ml_consumer_messages_consumed_total",
    "Enriched telemetry records deserialized",
)
PREDICTIONS_EMITTED = Counter(
    "ml_consumer_predictions_emitted_total",
    "PitPrediction records produced",
)
ERRORS = Counter(
    "ml_consumer_errors_total",
    "Deserialize, inference, or produce failures",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    predictor = Predictor(settings.model_path)
    stop_event = threading.Event()
    kafka_ready = threading.Event()

    def mark_ready() -> None:
        kafka_ready.set()

    worker = threading.Thread(
        target=run_consumer_loop,
        args=(
            settings,
            predictor,
            mark_ready,
            MESSAGES_CONSUMED,
            PREDICTIONS_EMITTED,
            ERRORS,
            stop_event,
        ),
        daemon=True,
        name="kafka-ml-worker",
    )
    worker.start()
    app.state.kafka_ready = kafka_ready
    yield
    stop_event.set()
    worker.join(timeout=20)
    if worker.is_alive():
        log.warning("Kafka worker thread did not exit cleanly")


app = FastAPI(
    title="AeroStream ML Consumer",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/ready")
def ready():
    ready_evt = getattr(app.state, "kafka_ready", None)
    if ready_evt is not None and ready_evt.is_set():
        return {"status": "ready"}
    return JSONResponse(
        status_code=503,
        content={"status": "not_ready"},
    )


@app.get("/metrics")
def metrics():
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)
