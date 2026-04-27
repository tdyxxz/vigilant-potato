from __future__ import annotations

from copy import deepcopy
from typing import Any

SOURCE_WEIGHTS = {
    "google_trends": 1.0,
    "wikipedia": 0.9,
    "reddit": 0.75,
}


def _source_weight(source: str) -> float:
    return SOURCE_WEIGHTS.get(source, 0.7)


def _calculate_weighted_velocity(cluster: dict[str, Any]) -> float:
    weighted_total = 0.0
    for signal in cluster.get("signals", []):
        source = str(signal.get("source", "")).strip().lower()
        velocity = float(signal.get("velocity", 0.0))
        weighted_total += velocity * _source_weight(source)
    return round(weighted_total, 4)


def _calculate_corroboration_bonus(cluster: dict[str, Any]) -> float:
    sources = {str(source).strip().lower() for source in cluster.get("sources", []) if str(source).strip()}
    if len(sources) <= 1:
        return 0.0
    return round(min((len(sources) - 1) * 0.12, 0.36), 4)


def score_topics(clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    for cluster in clusters:
        combined_velocity = float(cluster.get("combined_velocity", 0.0))
        weighted_velocity = _calculate_weighted_velocity(cluster) if cluster.get("signals") else combined_velocity
        source_count = float(cluster.get("source_count", 0))
        saturation_score = float(cluster.get("saturation_score", 0.0))
        corroboration_bonus = _calculate_corroboration_bonus(cluster)
        trend_score = round(
            (weighted_velocity * 0.55)
            + (combined_velocity * 0.15)
            + (source_count * 0.12)
            + corroboration_bonus
            - (saturation_score * 0.5),
            4,
        )

        scored.append(
            {
                "topic": cluster["cluster_topic"],
                "aliases": cluster.get("aliases", [cluster["cluster_topic"]]),
                "trend_score": trend_score,
                "velocity": round(combined_velocity, 4),
                "weighted_velocity": round(weighted_velocity, 4),
                "corroboration_bonus": corroboration_bonus,
                "saturation": round(saturation_score, 4),
                "source_count": int(source_count),
                "sources": cluster.get("sources", []),
                "signals": cluster.get("signals", []),
            }
        )

    return sorted(scored, key=lambda item: item["trend_score"], reverse=True)


def apply_historical_deltas(
    scored_topics: list[dict[str, Any]],
    previous_index: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    if not previous_index:
        enriched = []
        for topic in scored_topics:
            updated = deepcopy(topic)
            updated["previous_trend_score"] = None
            updated["trend_delta"] = round(updated["trend_score"], 4)
            updated["velocity_delta"] = round(updated["velocity"], 4)
            updated["delta_bonus"] = 0.0
            enriched.append(updated)
        return enriched

    previous_lookup = {str(item.get("topic", "")).strip().lower(): item for item in previous_index}
    enriched: list[dict[str, Any]] = []

    for topic in scored_topics:
        updated = deepcopy(topic)
        previous = previous_lookup.get(str(topic.get("topic", "")).strip().lower())
        previous_trend_score = float(previous.get("trend_score", 0.0)) if previous else 0.0
        previous_velocity = float(previous.get("velocity", 0.0)) if previous else 0.0

        trend_delta = round(updated["trend_score"] - previous_trend_score, 4)
        velocity_delta = round(updated["velocity"] - previous_velocity, 4)
        delta_bonus = round(max(trend_delta, 0.0) * 0.25, 4)

        updated["previous_trend_score"] = round(previous_trend_score, 4) if previous else None
        updated["trend_delta"] = trend_delta
        updated["velocity_delta"] = velocity_delta
        updated["delta_bonus"] = delta_bonus
        updated["trend_score"] = round(updated["trend_score"] + delta_bonus, 4)
        enriched.append(updated)

    return sorted(enriched, key=lambda item: item["trend_score"], reverse=True)


def apply_topic_ledger(
    scored_topics: list[dict[str, Any]],
    topic_ledger: dict[str, list[dict[str, Any]]] | None,
    history_window: int = 5,
) -> list[dict[str, Any]]:
    if not topic_ledger:
        enriched = []
        for topic in scored_topics:
            updated = deepcopy(topic)
            updated["rolling_trend_average"] = None
            updated["rolling_velocity_average"] = None
            updated["momentum_delta"] = round(updated["trend_score"], 4)
            updated["acceleration_delta"] = round(updated["velocity"], 4)
            updated["ledger_bonus"] = 0.0
            enriched.append(updated)
        return enriched

    enriched: list[dict[str, Any]] = []
    for topic in scored_topics:
        updated = deepcopy(topic)
        history = topic_ledger.get(str(topic.get("topic", "")).strip().lower(), [])
        recent_history = history[-history_window:]
        if not recent_history:
            updated["rolling_trend_average"] = None
            updated["rolling_velocity_average"] = None
            updated["momentum_delta"] = round(updated["trend_score"], 4)
            updated["acceleration_delta"] = round(updated["velocity"], 4)
            updated["ledger_bonus"] = 0.0
            enriched.append(updated)
            continue

        trend_average = sum(float(item.get("trend_score", 0.0)) for item in recent_history) / len(recent_history)
        velocity_average = sum(float(item.get("velocity", 0.0)) for item in recent_history) / len(recent_history)
        momentum_delta = round(updated["trend_score"] - trend_average, 4)
        acceleration_delta = round(updated["velocity"] - velocity_average, 4)
        ledger_bonus = round((max(momentum_delta, 0.0) * 0.18) + (max(acceleration_delta, 0.0) * 0.08), 4)

        updated["rolling_trend_average"] = round(trend_average, 4)
        updated["rolling_velocity_average"] = round(velocity_average, 4)
        updated["momentum_delta"] = momentum_delta
        updated["acceleration_delta"] = acceleration_delta
        updated["ledger_bonus"] = ledger_bonus
        updated["trend_score"] = round(updated["trend_score"] + ledger_bonus, 4)
        enriched.append(updated)

    return sorted(enriched, key=lambda item: item["trend_score"], reverse=True)
