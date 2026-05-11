from app.json_sanitize import sanitize_for_json


def test_sanitize_nan_inf_to_none() -> None:
    import math

    out = sanitize_for_json({"pit_probability": float("nan"), "x": float("inf"), "ok": 1.0})
    assert out["pit_probability"] is None
    assert out["x"] is None
    assert out["ok"] == 1.0


def test_sanitize_nested() -> None:
    import math

    out = sanitize_for_json({"items": [{"v": math.nan}]})
    assert out["items"][0]["v"] is None
