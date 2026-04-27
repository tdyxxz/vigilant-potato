from __future__ import annotations

from statistics import mean, pstdev
from typing import Any


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = position - lower_index
    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]
    return lower_value + ((upper_value - lower_value) * fraction)


def derive_detection_threshold(scored_topics: list[dict[str, Any]], minimum_threshold: float = 0.45) -> float:
    scores = [float(topic.get("trend_score", 0.0)) for topic in scored_topics]
    if not scores:
        return minimum_threshold
    if len(scores) == 1:
        return max(minimum_threshold, round(scores[0], 4))

    percentile_75 = _quantile(scores, 0.75)
    average_score = mean(scores)
    spread = pstdev(scores)

    adaptive_threshold = max(
        minimum_threshold,
        average_score + (spread * 0.35),
        percentile_75 - 0.05,
    )
    return round(adaptive_threshold, 4)


def detect_emerging_topics(
    scored_topics: list[dict[str, Any]],
    threshold: float = 0.6,
    saturation_limit: float = 0.7,
    top_n: int = 10,
    adaptive: bool = True,
) -> list[dict[str, Any]]:
    effective_threshold = derive_detection_threshold(scored_topics, minimum_threshold=threshold) if adaptive else threshold
    emerging = [
        {**topic, "detection_threshold": effective_threshold}
        for topic in scored_topics
        if float(topic.get("trend_score", 0.0)) >= effective_threshold and float(topic.get("saturation", 1.0)) < saturation_limit
    ]
    return emerging[:top_n]
