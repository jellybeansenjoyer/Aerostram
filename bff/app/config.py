from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Defaults match host-side dev (docker compose overrides with kafka-1:9092,... etc.).
    kafka_bootstrap_servers: str = "localhost:9092,localhost:9094,localhost:9096"
    schema_registry_url: str = "http://localhost:8081"

    pit_predictions_topic: str = "pit-predictions"
    stream_aggregates_topic: str = "stream-aggregates"

    poll_deadline_sec: float = 5.0
    max_limit: int = 100


settings = Settings()
