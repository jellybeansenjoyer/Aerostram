"""Unit tests for Kafka tail helpers — Consumer is fully mocked."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.kafka_recent import tail_json_values


@patch("app.kafka_recent.Consumer")
def test_tail_json_collects_decoded_rows(mock_consumer_cls) -> None:
    mock_c = MagicMock()
    mock_consumer_cls.return_value = mock_c

    topic_meta = MagicMock()
    topic_meta.partitions = {0: MagicMock()}
    meta = MagicMock()
    meta.topics = {"stream-aggregates": topic_meta}
    mock_c.list_topics.return_value = meta

    mock_c.get_watermark_offsets.return_value = (0, 100)

    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = b'{"car_id":"car-01","avg_speed_kph":220.5}'
    msg.partition.return_value = 0
    msg.offset.return_value = 42

    calls = {"n": 0}

    def poll_side_effect(*_args, **_kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return None  # post-assign handshake poll
        if calls["n"] == 2:
            return msg
        return None

    mock_c.poll.side_effect = poll_side_effect

    rows = tail_json_values("kafka:9092", "stream-aggregates", 10, 5.0)

    assert len(rows) == 1
    assert rows[0]["car_id"] == "car-01"
    assert rows[0]["avg_speed_kph"] == 220.5
    assert rows[0]["_kafka_partition"] == 0
    assert rows[0]["_kafka_offset"] == 42
    mock_c.assign.assert_called_once()
    mock_c.close.assert_called_once()


@patch("app.kafka_recent.Consumer")
def test_tail_json_missing_topic_returns_empty(mock_consumer_cls) -> None:
    mock_c = MagicMock()
    mock_consumer_cls.return_value = mock_c

    meta = MagicMock()
    meta.topics = {}
    mock_c.list_topics.return_value = meta

    rows = tail_json_values("kafka:9092", "unknown-topic", 5, 1.0)

    assert rows == []
    mock_c.assign.assert_not_called()
