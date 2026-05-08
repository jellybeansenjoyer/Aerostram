# Dashboard BFF (FE-9)

Read-only FastAPI service that tails Kafka topics for the React dashboard (pit predictions as Avro, ksql aggregates as JSON).

## Local

```bash
cd bff
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096
export SCHEMA_REGISTRY_URL=http://localhost:8081
uvicorn app.main:app --host 0.0.0.0 --port 8089
```

## Docker

`docker compose up -d dashboard-bff` (port **8089**, override with `DASHBOARD_BFF_PORT` in `.env`).

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

## API

- `GET /health`
- `GET /api/v1/pit-predictions/recent?limit=20`
- `GET /api/v1/stream-aggregates/recent?limit=20`

The Vite dev server proxies `/svc/bff` → `http://127.0.0.1:8089` (see `dashboard/vite.config.ts`).
