from __future__ import annotations

import math
from typing import Any


def estimate_saturation(clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    max_signal_mentions = max((len(cluster.get("signals", [])) for cluster in clusters), default=1)

    for cluster in clusters:
        signal_mentions = len(cluster.get("signals", []))
        reddit_mentions = sum(1 for signal in cluster.get("signals", []) if signal.get("source") == "reddit")
        source_count = max(int(cluster.get("source_count", 1)), 1)
        mock_search_results = (signal_mentions * 1200) + (source_count * 700) + (reddit_mentions * 500)

        search_pressure = min(math.log10(mock_search_results + 1) / 6, 1.0)
        mention_pressure = min(signal_mentions / max_signal_mentions, 1.0)
        reddit_pressure = min(reddit_mentions / 5, 1.0)
        saturation_score = round((search_pressure * 0.35) + (mention_pressure * 0.45) + (reddit_pressure * 0.2), 4)

        enriched_cluster = dict(cluster)
        enriched_cluster["saturation_score"] = min(saturation_score, 1.0)
        enriched.append(enriched_cluster)

    return enriched
