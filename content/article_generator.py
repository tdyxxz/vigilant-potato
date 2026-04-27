from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
import re
from typing import Any


@dataclass(frozen=True)
class ArticleRecord:
    title: str
    subheadline: str
    topic: str
    aliases: list[str]
    summary: str
    what_to_know: list[str]
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
SOURCE_NAMES = {
    "google_trends": "Google Trends",
    "reddit": "Reddit",
    "wikipedia": "Wikipedia",
}
SOURCE_DESCRIPTIONS = {
    "google_trends": "search interest",
    "reddit": "online discussion",
    "wikipedia": "reader traffic",
}


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


def _human_source_name(source: str) -> str:
    return SOURCE_NAMES.get(source, source.replace("_", " ").title())


def _source_description(source: str) -> str:
    return SOURCE_DESCRIPTIONS.get(source, "online attention")


def _looks_like_person(topic: str) -> bool:
    words = [word for word in re.split(r"\s+", topic.strip()) if word]
    return 2 <= len(words) <= 3 and all(word.isalpha() for word in words)


def _plain_topic(topic: str) -> str:
    return re.sub(r"\s+", " ", topic.replace("_", " ")).strip()


def _build_news_headline(topic: str, strongest_signal: dict[str, Any] | None, aliases: list[str]) -> str:
    base = synthesize_headline(topic, [topic]) or synthesize_headline(topic, aliases)
    lowered = _plain_topic(base).lower()
    if "recall" in lowered:
        return f"{base} Gains Traction Online"
    if "cause of death" in lowered or "death" in lowered:
        return f"{base} Searches Surge Again"
    if _looks_like_person(_plain_topic(base)):
        return f"{base} Back in Spotlight as Interest Jumps"
    if strongest_signal and strongest_signal["source"] == "google_trends":
        return f"{base} Surges in Online Searches"
    if strongest_signal and strongest_signal["source"] == "wikipedia":
        return f"{base} Draws Fresh Online Attention"
    return f"{base} Suddenly Back in Focus"


def _build_subheadline(topic: str, strongest_signal: dict[str, Any] | None) -> str:
    plain_topic = _plain_topic(topic)
    if strongest_signal is None:
        return f"Fresh online attention is building around {plain_topic}."
    source_name = _human_source_name(strongest_signal["source"])
    source_description = _source_description(strongest_signal["source"])
    return f"{source_name} is driving a fresh wave of {source_description} around {plain_topic}."


def _build_summary(topic: str, strongest_signal: dict[str, Any] | None) -> str:
    plain_topic = _plain_topic(topic)
    if strongest_signal is None:
        return f"Fresh online attention is gathering around {plain_topic}, although the reason for the latest burst was not immediately clear."
    source_description = _source_description(strongest_signal["source"])
    return (
        f"Online interest in {plain_topic} is climbing sharply, with new data showing a clear jump in {source_description}."
    )


def _build_subject_context(topic: str, aliases: list[str]) -> str:
    plain_topic = _plain_topic(topic)
    lowered = plain_topic.lower()
    if "recall" in lowered:
        return (
            f"The topic appears to center on a consumer recall, a kind of story that often draws attention when shoppers begin looking for updates, warnings or product details."
        )
    if "cause of death" in lowered or "death" in lowered:
        return (
            f"The spike appears tied to renewed curiosity about the circumstances surrounding {plain_topic}, a pattern that often resurfaces when older stories begin circulating again."
        )
    if _looks_like_person(plain_topic):
        return (
            f"{plain_topic} appears to be a public figure, and the latest rise suggests that name is suddenly back in wide circulation online."
        )
    if len(aliases) > 1:
        alias_text = ", ".join(_plain_topic(alias) for alias in aliases[1:3])
        return (
            f"The topic is also appearing under closely related phrasing such as {alias_text}, suggesting attention is spreading across multiple versions of the same story."
        )
    return (
        f"It was not immediately clear what pushed {plain_topic} higher, but the subject is drawing broader curiosity across the web."
    )


def _build_related_context(topic: str, strongest_signal: dict[str, Any] | None, source_counts: Counter[str]) -> str:
    plain_topic = _plain_topic(topic)
    if strongest_signal is None:
        return f"The latest burst of attention around {plain_topic} appears to be building without a single clear public trigger."
    source_name = _human_source_name(strongest_signal["source"])
    source_description = _source_description(strongest_signal["source"])
    source_total = sum(source_counts.values())
    if source_total > 1:
        return (
            f"The strongest push is currently coming from {source_name}, while other public data points suggest curiosity is spreading beyond a single corner of the internet."
        )
    return (
        f"For now, the clearest sign of momentum is coming from {source_name}, where the latest rise in {source_description} suggests organic curiosity rather than a formal announcement."
    )


def _build_what_to_know(topic: str, strongest_signal: dict[str, Any] | None, source_counts: Counter[str]) -> list[str]:
    plain_topic = _plain_topic(topic)
    if strongest_signal is None:
        return [
            f"Interest around {plain_topic} is rising.",
            "The reason for the latest spike is not yet clear.",
        ]
    source_name = _human_source_name(strongest_signal["source"])
    source_description = _source_description(strongest_signal["source"])
    return [
        f"Attention around {plain_topic} is increasing quickly.",
        f"{source_name} is showing the strongest jump in {source_description}.",
        f"The topic is being picked up across {len(source_counts)} source{'s' if len(source_counts) != 1 else ''}.",
    ]


def _timeline_line(topic: str, signal: dict[str, Any]) -> str:
    source_name = _human_source_name(signal["source"])
    source_description = _source_description(signal["source"])
    observed_topic = _plain_topic(str(signal["topic"]))
    plain_topic = _plain_topic(topic)
    if observed_topic != plain_topic:
        return f"- {_format_timestamp(signal['timestamp'])}: {source_name} helped push interest higher around {observed_topic}."
    return f"- {_format_timestamp(signal['timestamp'])}: {source_name} showed a fresh rise in {source_description}."


def _evidence_line(evidence_item: dict[str, Any]) -> str:
    source_name = _human_source_name(evidence_item["source"])
    observed_topic = _plain_topic(str(evidence_item["observed_topic"]))
    return f"- {source_name} was one of the clearest signs of fresh attention around {observed_topic}."


def _build_current_state(
    topic: str,
    aliases: list[str],
    source_counts: Counter[str],
    strongest_signal: dict[str, Any] | None,
) -> str:
    plain_topic = _plain_topic(topic)
    alias_text = ", ".join(_plain_topic(alias) for alias in aliases[1:4])
    if strongest_signal is None:
        return f"Fresh attention is building around {plain_topic}, but the exact spark behind the latest rise remains uncertain."
    if alias_text:
        return f"Online conversation around {plain_topic} is also appearing under related phrasing such as {alias_text}, widening the reach of the story."
    return f"The latest burst suggests {plain_topic} is moving beyond a niche search and into broader public view."


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
            "note": _evidence_line(
                {
                    "source": signal["source"],
                    "observed_topic": signal["topic"],
                }
            ),
        }
        for signal in ordered_signals
    ]

    return ArticleRecord(
        title=_build_news_headline(topic, strongest_signal, resolved_aliases),
        subheadline=_build_subheadline(topic, strongest_signal),
        topic=topic,
        aliases=resolved_aliases,
        summary=_build_summary(topic, strongest_signal),
        what_to_know=_build_what_to_know(topic, strongest_signal, source_counts),
        timeline=timeline_lines,
        current_state=_build_subject_context(topic, resolved_aliases),
        related_context=_build_related_context(topic, strongest_signal, source_counts),
        evidence=evidence,
    )


def article_record_to_markdown(record: ArticleRecord) -> str:
    evidence_lines = [item["note"] for item in record.evidence[:10]]

    article = [
        f"# {record.title}",
        "",
        record.subheadline,
        "",
        "## Summary",
        record.summary,
        "",
        record.current_state,
        "",
        record.related_context,
        "",
        "## What to Know",
        *(f"- {item}" for item in record.what_to_know),
        "",
        "## Timeline of Emergence",
        *(record.timeline or ["- No timeline data available."]),
        "",
        "## Evidence",
        *(evidence_lines or ["- No evidence records available."]),
        "",
    ]
    return "\n".join(article)


def generate_article(topic: str, supporting_signals: list[dict[str, Any]]) -> str:
    return article_record_to_markdown(build_article_record(topic, supporting_signals))


def article_record_to_dict(record: ArticleRecord) -> dict[str, Any]:
    return asdict(record)
