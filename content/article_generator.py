from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
import re
from typing import Any


@dataclass(frozen=True)
class ArticleRecord:
    title: str
    topic: str
    aliases: list[str]
    summary: str
    timeline: list[str]
    current_state: str
    related_context: str
    evidence: list[dict[str, Any]]


TITLE_STOPWORDS = {"a", "an", "and", "as", "at", "by", "for", "from", "in", "of", "on", "or", "the", "to", "with"}
TITLE_DROP_PHRASES = (
    "are talking about",
    "caused more extensive",
    "than publicly known",
    "publicly known",
    "instead argues researcher",
    "slack messages interviews with current and former works paint picture of company in turmoil",
)


def _format_timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.strftime("%Y-%m-%d %H:%M UTC")
    except ValueError:
        return value


def synthesize_headline(topic: str, aliases: list[str] | None = None) -> str:
    candidates = [topic, *(aliases or [])]
    cleaned_candidates: list[str] = []
    for candidate in candidates:
        text = re.sub(r"\s+", " ", candidate.strip())
        text = re.sub(r"\bi will not promote\b", "", text, flags=re.IGNORECASE).strip(" -:,;")
        if text:
            cleaned_candidates.append(text)

    base = min(cleaned_candidates or [topic], key=lambda value: (len(value.split()), len(value)))
    compressed = base
    for phrase in TITLE_DROP_PHRASES:
        compressed = re.sub(rf"\b{re.escape(phrase)}\b", " ", compressed, flags=re.IGNORECASE)
    compressed = re.sub(r"\s+", " ", compressed).strip(" -:,;")
    if compressed:
        base = compressed

    compact_words = [word for word in base.split() if word.lower() not in TITLE_STOPWORDS]
    if len(compact_words) >= 3 and len(compact_words) < len(base.split()):
        base = " ".join(compact_words)

    if len(base.split()) > 8:
        informative_words = [word for word in base.split() if word.lower() not in TITLE_STOPWORDS]
        if informative_words:
            base = " ".join(informative_words[:6])

    words = []
    for index, word in enumerate(base.split()):
        lower = word.lower()
        if lower in {"us", "uk", "eu", "ai"}:
            words.append(lower.upper())
            continue
        if index != 0 and lower in TITLE_STOPWORDS:
            words.append(lower)
            continue
        words.append(lower.capitalize())

    headline = " ".join(words).strip()
    return headline[:90].rstrip()


def _build_summary(topic: str, source_counts: Counter[str], strongest_signal: dict[str, Any] | None) -> str:
    leading_source = strongest_signal["source"] if strongest_signal else "unknown"
    leading_velocity = strongest_signal["velocity"] if strongest_signal else 0.0
    return (
        f"Observed activity suggests `{topic}` is accelerating across {len(source_counts)} source(s). "
        f"The strongest measured signal currently comes from `{leading_source}` with normalized velocity `{leading_velocity:.4f}`."
    )


def _variant_text(canonical_topic: str, observed_topic: str) -> str:
    if observed_topic == canonical_topic:
        return "using the canonical topic label"
    return f'using the observed variant `{observed_topic}`'


def _source_action(source: str) -> str:
    actions = {
        "google_trends": "showed rising search interest",
        "reddit": "showed discussion momentum",
        "wikipedia": "showed elevated pageview attention",
    }
    return actions.get(source, "showed measurable attention")


def _timeline_line(topic: str, signal: dict[str, Any]) -> str:
    return (
        f"- {_format_timestamp(signal['timestamp'])}: `{signal['source']}` {_source_action(signal['source'])} "
        f"with normalized velocity `{signal['velocity']:.4f}` for `{topic}` ({_variant_text(topic, signal['topic'])})"
    )


def _evidence_line(topic: str, evidence_item: dict[str, Any]) -> str:
    return (
        f"- `{evidence_item['source']}` {_source_action(evidence_item['source'])} at {evidence_item['formatted_time']} "
        f"with velocity `{evidence_item['velocity']:.4f}` for `{topic}` "
        f"({_variant_text(topic, evidence_item['observed_topic'])})"
    )


def _build_current_state(
    topic: str,
    aliases: list[str],
    source_counts: Counter[str],
    strongest_signal: dict[str, Any] | None,
) -> str:
    source_summary = ", ".join(f"{source} ({count})" for source, count in sorted(source_counts.items()))
    alias_summary = ", ".join(f"`{alias}`" for alias in aliases[1:4])
    if strongest_signal is None:
        return f"No reliable signals were available for `{topic}`."
    alias_sentence = f" Supporting variants in this cluster include {alias_summary}." if alias_summary else ""
    return (
        f"`{topic}` is appearing in the following monitored sources: {source_summary}. "
        f"The highest current momentum was registered on `{strongest_signal['source']}` {_variant_text(topic, strongest_signal['topic'])}.{alias_sentence}"
    )


def build_article_record(topic: str, supporting_signals: list[dict[str, Any]], aliases: list[str] | None = None) -> ArticleRecord:
    ordered_signals = sorted(supporting_signals, key=lambda item: item["timestamp"])
    sources = [signal["source"] for signal in ordered_signals]
    source_counts = Counter(sources)
    strongest_signal = max(ordered_signals, key=lambda item: item["velocity"], default=None)
    resolved_aliases = aliases or [topic]

    timeline_lines = [_timeline_line(topic, signal) for signal in ordered_signals[:8]]
    evidence = [
        {
            "source": signal["source"],
            "topic": topic,
            "observed_topic": signal["topic"],
            "observed_at": signal["timestamp"],
            "formatted_time": _format_timestamp(signal["timestamp"]),
            "velocity": round(float(signal["velocity"]), 4),
            "is_canonical_match": signal["topic"] == topic,
        }
        for signal in ordered_signals
    ]

    return ArticleRecord(
        title=synthesize_headline(topic, resolved_aliases),
        topic=topic,
        aliases=resolved_aliases,
        summary=_build_summary(topic, source_counts, strongest_signal),
        timeline=timeline_lines,
        current_state=_build_current_state(topic, resolved_aliases, source_counts, strongest_signal),
        related_context=(
            "This article is assembled directly from public activity signals. "
            "It summarizes observed attention patterns and avoids unverified causal claims."
        ),
        evidence=evidence,
    )


def article_record_to_markdown(record: ArticleRecord) -> str:
    evidence_lines = [_evidence_line(record.topic, item) for item in record.evidence[:10]]

    article = [
        f"# {record.title}",
        "",
        "## Summary",
        record.summary,
        "",
        "## Timeline of Emergence",
        *(record.timeline or ["- No timeline data available."]),
        "",
        "## What Is Currently Happening",
        record.current_state,
        "",
        "## Evidence",
        *(evidence_lines or ["- No evidence records available."]),
        "",
        "## Related Context",
        record.related_context,
        "",
    ]
    return "\n".join(article)


def generate_article(topic: str, supporting_signals: list[dict[str, Any]]) -> str:
    return article_record_to_markdown(build_article_record(topic, supporting_signals))


def article_record_to_dict(record: ArticleRecord) -> dict[str, Any]:
    return asdict(record)
