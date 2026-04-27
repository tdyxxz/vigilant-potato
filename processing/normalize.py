from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any

BLOCKED_EXACT_TOPICS = {
    "removed by reddit",
    "specialsearch",
    "xxx",
}

BLOCKED_SUBSTRINGS = (
    "announcement temporary",
    "daily discussion thread",
    "discussion thread",
    "feedback friday",
    "live thread",
    "mods applications",
    "quarterly post",
    "removed by reddit",
    "share your startup",
    "state of the subreddit",
    "thread ",
)


def _normalize_topic(topic: str) -> str:
    cleaned = re.sub(r"\s+", " ", topic.strip().lower())
    return re.sub(r"[^\w\s-]", "", cleaned)


def _tokenize(topic: str) -> set[str]:
    return {token for token in re.split(r"[\s_-]+", topic) if len(token) > 2}


def _similarity(left: str, right: str) -> float:
    left_tokens = _tokenize(left)
    right_tokens = _tokenize(right)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens) / len(left_tokens | right_tokens)
    fuzzy = SequenceMatcher(None, left, right).ratio()
    return max(overlap, fuzzy)


def _is_low_quality_topic(topic: str) -> bool:
    if not topic or len(topic) < 4:
        return True
    if topic in BLOCKED_EXACT_TOPICS:
        return True
    if topic.isdigit():
        return True
    if len(set(topic.replace(" ", ""))) <= 2 and len(topic.replace(" ", "")) >= 3:
        return True

    token_count = len(_tokenize(topic))
    if token_count == 0:
        return True

    for blocked in BLOCKED_SUBSTRINGS:
        if blocked in topic:
            return True

    return False


def is_high_quality_topic(topic: str) -> bool:
    return not _is_low_quality_topic(_normalize_topic(topic))


def normalize_signals(raw_signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for signal in raw_signals:
        topic = _normalize_topic(str(signal.get("topic", "")))
        source = str(signal.get("source", "")).strip().lower()
        velocity = float(signal.get("velocity", 0.0))
        timestamp = signal.get("timestamp")
        if not topic or not source or timestamp is None or _is_low_quality_topic(topic):
            continue
        prepared.append(
            {
                "topic": topic,
                "source": source,
                "velocity": max(velocity, 0.0),
                "timestamp": timestamp,
            }
        )

    scaled = _scale_velocity(prepared)
    deduplicated: list[dict[str, Any]] = []
    for signal in scaled:
        duplicate = next(
            (existing for existing in deduplicated if _similarity(signal["topic"], existing["topic"]) >= 0.9),
            None,
        )
        if duplicate is None:
            deduplicated.append(signal)
            continue
        if signal["velocity"] > duplicate["velocity"]:
            duplicate.update(signal)

    return deduplicated


def _scale_velocity(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not signals:
        return []

    grouped: dict[str, list[dict[str, Any]]] = {}
    for signal in signals:
        grouped.setdefault(signal["source"], []).append(signal)

    for items in grouped.values():
        velocities = [item["velocity"] for item in items]
        minimum = min(velocities)
        maximum = max(velocities)
        span = maximum - minimum
        for item in items:
            if span == 0:
                item["velocity"] = 1.0 if maximum > 0 else 0.0
            else:
                item["velocity"] = round((item["velocity"] - minimum) / span, 4)

    return signals
