from __future__ import annotations

from collections import defaultdict
import re
from difflib import SequenceMatcher
from typing import Any

GENERIC_TOKENS = {
    "2024",
    "2025",
    "2026",
    "2027",
    "breaking",
    "film",
    "live",
    "movie",
    "news",
    "official",
    "update",
    "video",
}


def _tokenize(topic: str) -> set[str]:
    tokens = set()
    for token in re.split(r"[\s_-]+", topic.lower()):
        if len(token) <= 2 or token in GENERIC_TOKENS or token.isdigit():
            continue
        tokens.add(token)
    return tokens


def _topics_match(left: str, right: str) -> bool:
    if left == right:
        return True

    left_tokens = _tokenize(left)
    right_tokens = _tokenize(right)
    if not left_tokens or not right_tokens:
        return False
    shared_tokens = left_tokens & right_tokens
    overlap_count = len(shared_tokens)
    jaccard = overlap_count / len(left_tokens | right_tokens)
    fuzzy = SequenceMatcher(None, left, right).ratio()

    if len(left_tokens) == 1 and len(right_tokens) == 1:
        return overlap_count == 1

    if overlap_count >= 2 and jaccard >= 0.4:
        return True

    return fuzzy >= 0.9


def _canonical_topic(signals: list[dict[str, Any]]) -> str:
    by_topic: dict[str, dict[str, Any]] = defaultdict(lambda: {"velocity": 0.0, "count": 0, "sources": set()})
    for signal in signals:
        topic = signal["topic"]
        by_topic[topic]["velocity"] += float(signal.get("velocity", 0.0))
        by_topic[topic]["count"] += 1
        by_topic[topic]["sources"].add(signal.get("source"))

    def topic_score(item: tuple[str, dict[str, Any]]) -> tuple[float, float, int, int, str]:
        topic, stats = item
        token_count = len(_tokenize(topic))
        length_penalty = max(token_count - 6, 0) * 0.08
        source_bonus = len(stats["sources"]) * 0.12
        score = stats["velocity"] + (stats["count"] * 0.08) + source_bonus - length_penalty
        return (round(score, 6), round(stats["velocity"], 6), -token_count, -len(topic), topic)

    return max(by_topic.items(), key=topic_score)[0]


def _topic_aliases(signals: list[dict[str, Any]]) -> list[str]:
    by_topic: dict[str, float] = defaultdict(float)
    for signal in signals:
        by_topic[signal["topic"]] += float(signal.get("velocity", 0.0))
    return [topic for topic, _ in sorted(by_topic.items(), key=lambda item: (-item[1], item[0]))]


def merge_signals(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    clusters: list[dict[str, Any]] = []

    for signal in signals:
        match = next(
            (cluster for cluster in clusters if _topics_match(signal["topic"], cluster["cluster_topic"])),
            None,
        )
        if match is None:
            clusters.append(
                {
                    "cluster_topic": signal["topic"],
                    "aliases": [signal["topic"]],
                    "sources": [signal["source"]],
                    "combined_velocity": signal["velocity"],
                    "source_count": 1,
                    "signals": [signal],
                }
            )
            continue

        match["signals"].append(signal)
        if signal["source"] not in match["sources"]:
            match["sources"].append(signal["source"])
        match["combined_velocity"] += signal["velocity"]
        match["source_count"] = len(match["sources"])

    for cluster in clusters:
        cluster["cluster_topic"] = _canonical_topic(cluster["signals"])
        cluster["aliases"] = _topic_aliases(cluster["signals"])
        cluster["combined_velocity"] = round(cluster["combined_velocity"], 4)

    return sorted(clusters, key=lambda item: item["combined_velocity"], reverse=True)
