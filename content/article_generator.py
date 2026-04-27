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
SPORTS_MARKERS = {"vs", "v", "at"}
COMPANY_MARKERS = {"energy", "bank", "airlines", "group", "inc", "corp", "company", "holdings"}
PERSON_HEADLINE_PATTERNS = (
    "{topic} Back in Spotlight as Interest Jumps",
    "{topic} Suddenly Surges Back Into Focus",
    "{topic} Draws Fresh Search Frenzy",
)
RECALL_HEADLINE_PATTERNS = (
    "{topic} Gains Traction Online",
    "{topic} Sparks Fresh Online Attention",
    "{topic} Surges Back Into Conversation",
)
MATCHUP_HEADLINE_PATTERNS = (
    "{topic} Draws Fresh Buzz Online",
    "{topic} Suddenly Heats Up Online",
    "{topic} Grabs New Attention Fast",
)
COMPANY_HEADLINE_PATTERNS = (
    "{topic} Draws New Attention Online",
    "{topic} Suddenly Climbs in Searches",
    "{topic} Back in Focus After Search Spike",
)
DEATH_HEADLINE_PATTERNS = (
    "{topic} Searches Surge Again",
    "{topic} Back in Search Spotlight",
    "{topic} Draws Fresh Online Curiosity",
)
GENERIC_SEARCH_PATTERNS = (
    "{topic} Surges in Online Searches",
    "{topic} Suddenly Jumps in Search Interest",
    "{topic} Gains Rapid Search Attention",
)
GENERIC_ATTENTION_PATTERNS = (
    "{topic} Draws Fresh Online Attention",
    "{topic} Suddenly Back in Focus",
    "{topic} Grabs New Attention Online",
)
GOOGLE_SUMMARY_PATTERNS = (
    "Online interest in {topic} is climbing sharply, with new data showing a clear jump in search interest.",
    "Searches tied to {topic} are moving higher quickly, signaling a fresh burst of public curiosity.",
    "Fresh search activity is pushing {topic} higher online, suggesting a fast-building wave of attention.",
)
WIKIPEDIA_SUMMARY_PATTERNS = (
    "Online interest in {topic} is climbing sharply, with new data showing a clear jump in reader traffic.",
    "Reader traffic around {topic} is rising fast, pointing to a new burst of public curiosity.",
    "Fresh pageview activity is pushing {topic} back into focus, suggesting that curiosity is building quickly.",
)
REDDIT_SUMMARY_PATTERNS = (
    "Online attention around {topic} is climbing sharply, with new discussion showing a fast rise in public interest.",
    "Conversation tied to {topic} is heating up quickly, suggesting a fresh burst of online attention.",
    "Fresh discussion momentum is pushing {topic} higher online, pointing to rapidly growing curiosity.",
)
GENERIC_SUMMARY_PATTERNS = (
    "Online interest in {topic} is climbing sharply, drawing fresh public curiosity.",
    "{topic} is suddenly picking up online attention, suggesting a fast-building wave of interest.",
    "Fresh attention is pushing {topic} back into focus as curiosity rises online.",
)
GOOGLE_SUBHEADLINE_PATTERNS = (
    "{source} is driving a fresh wave of {description} around {topic}.",
    "{source} is pushing new attention toward {topic} as interest builds fast.",
    "{source} is fueling a fresh burst of curiosity around {topic}.",
)
WIKIPEDIA_SUBHEADLINE_PATTERNS = (
    "{source} is driving a fresh wave of {description} around {topic}.",
    "{source} is sending new reader attention toward {topic}.",
    "{source} is helping push {topic} back into view.",
)
REDDIT_SUBHEADLINE_PATTERNS = (
    "{source} is driving a fresh wave of {description} around {topic}.",
    "{source} is helping push new online chatter around {topic}.",
    "{source} is fueling a fresh burst of discussion around {topic}.",
)
GENERIC_SUBHEADLINE_PATTERNS = (
    "{source} is driving a fresh wave of {description} around {topic}.",
    "{source} is helping push fresh attention toward {topic}.",
    "{source} is fueling a new burst of curiosity around {topic}.",
)
PERSON_CONTEXT_PATTERNS = (
    "{topic} appears to be a public figure, and the latest rise suggests that name is suddenly back in wide circulation online.",
    "{topic} appears to be a recognizable public figure, and the new spike suggests the name is moving quickly through online conversation again.",
    "{topic} appears to be a public figure, with the latest jump pointing to a renewed burst of curiosity around the name.",
)
RECALL_CONTEXT_PATTERNS = (
    "The topic appears to center on a consumer recall, a kind of story that often draws attention when shoppers begin looking for updates, warnings or product details.",
    "The subject appears tied to a consumer recall, which often starts trending when shoppers scramble for product warnings, updates or safety information.",
    "The topic appears linked to a product recall, a category that can quickly pick up momentum when consumers start searching for answers.",
)
DEATH_CONTEXT_PATTERNS = (
    "The spike appears tied to renewed curiosity about the circumstances surrounding {topic}, a pattern that often resurfaces when older stories begin circulating again.",
    "The rise appears linked to fresh curiosity about {topic}, the kind of attention that often returns when older questions begin moving online again.",
    "The latest jump appears tied to renewed public curiosity around {topic}, especially when earlier stories begin circulating again.",
)
MATCHUP_CONTEXT_PATTERNS = (
    "The topic appears to reference a sports matchup, which often starts trending when fans look for game-time updates, highlights or sudden shifts in attention around a contest.",
    "The subject appears to point to a sports matchup, a category that often spikes when fans rush online for updates, reaction or highlights.",
    "The topic appears tied to a sports contest, the kind of subject that can climb quickly when fan attention suddenly sharpens.",
)
COMPANY_CONTEXT_PATTERNS = (
    "The subject appears to involve a company or business name, a category that often rises when customers, investors or local audiences start looking for updates tied to breaking developments.",
    "The topic appears tied to a company name, the kind of subject that often spikes when customers or local audiences begin searching for fresh developments.",
    "The subject appears connected to a business or company name, which can gain momentum quickly when people start looking for updates or explanations.",
)
ALIAS_CONTEXT_PATTERNS = (
    "The topic is also appearing under closely related phrasing such as {aliases}, suggesting attention is spreading across multiple versions of the same story.",
    "The story is also surfacing under related phrasing such as {aliases}, a sign that curiosity is spreading across several versions of the topic.",
    "Related phrasing such as {aliases} is also picking up attention, suggesting the same story is circulating under multiple labels.",
)
GENERIC_CONTEXT_PATTERNS = (
    "It was not immediately clear what pushed {topic} higher, but the subject is drawing broader curiosity across the web.",
    "The exact trigger behind the latest rise was not immediately obvious, though {topic} is clearly drawing new attention online.",
    "What pushed {topic} higher was not immediately clear, but the subject is attracting a wider burst of curiosity online.",
)
MATCHUP_RELATED_PATTERNS = (
    "For now, the clearest push is coming from {source}, where the rise in {description} suggests fans are suddenly paying closer attention to the matchup.",
    "The strongest visible lift is coming from {source}, suggesting fans are zeroing in on the matchup as attention builds.",
    "Right now, {source} is showing the clearest push, pointing to a fresh rise in fan attention around the matchup.",
)
COMPANY_RELATED_PATTERNS = (
    "For now, the clearest push is coming from {source}, where the latest jump in {description} suggests new curiosity around the company or its latest developments.",
    "The strongest visible lift is coming from {source}, suggesting people are searching for new information tied to the company.",
    "Right now, {source} is showing the clearest push, pointing to a fresh rise in curiosity around the company and its latest developments.",
)
MULTI_SOURCE_RELATED_PATTERNS = (
    "The strongest push is currently coming from {source}, while other public data points suggest curiosity is spreading beyond a single corner of the internet.",
    "The clearest lift is coming from {source}, but other signals suggest the topic is beginning to travel more broadly online.",
    "{source} is leading the current rise, while other public indicators suggest the story is starting to spread more widely.",
)
SINGLE_SOURCE_RELATED_PATTERNS = (
    "For now, the clearest sign of momentum is coming from {source}, where the latest rise in {description} suggests organic curiosity rather than a formal announcement.",
    "The strongest visible movement is coming from {source}, where the latest jump in {description} points to organic curiosity more than a formal event.",
    "At the moment, {source} is showing the clearest burst of momentum, with the rise in {description} suggesting curiosity is building naturally.",
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


def _human_source_name(source: str) -> str:
    return SOURCE_NAMES.get(source, source.replace("_", " ").title())


def _source_description(source: str) -> str:
    return SOURCE_DESCRIPTIONS.get(source, "online attention")


def _looks_like_person(topic: str) -> bool:
    words = [word for word in re.split(r"\s+", topic.strip()) if word]
    return 2 <= len(words) <= 3 and all(word.isalpha() for word in words)


def _looks_like_matchup(topic: str) -> bool:
    words = {word.lower() for word in re.split(r"\s+", topic.strip()) if word}
    return bool(words & SPORTS_MARKERS)


def _looks_like_company(topic: str) -> bool:
    words = {word.lower() for word in re.split(r"\s+", topic.strip()) if word}
    return bool(words & COMPANY_MARKERS)


def _plain_topic(topic: str) -> str:
    return re.sub(r"\s+", " ", topic.replace("_", " ")).strip()


def _pick_pattern(topic: str, patterns: tuple[str, ...]) -> str:
    seed = sum(ord(char) for char in topic)
    return patterns[seed % len(patterns)]


def _build_news_headline(topic: str, strongest_signal: dict[str, Any] | None, aliases: list[str]) -> str:
    base = synthesize_headline(topic, [topic]) or synthesize_headline(topic, aliases)
    plain_base = _plain_topic(base)
    lowered = plain_base.lower()
    if "recall" in lowered:
        return _pick_pattern(plain_base, RECALL_HEADLINE_PATTERNS).format(topic=plain_base)
    if "cause of death" in lowered or "death" in lowered:
        return _pick_pattern(plain_base, DEATH_HEADLINE_PATTERNS).format(topic=plain_base)
    if _looks_like_matchup(plain_base):
        return _pick_pattern(plain_base, MATCHUP_HEADLINE_PATTERNS).format(topic=plain_base)
    if _looks_like_company(plain_base):
        return _pick_pattern(plain_base, COMPANY_HEADLINE_PATTERNS).format(topic=plain_base)
    if _looks_like_person(plain_base):
        return _pick_pattern(plain_base, PERSON_HEADLINE_PATTERNS).format(topic=plain_base)
    if strongest_signal and strongest_signal["source"] == "google_trends":
        return _pick_pattern(plain_base, GENERIC_SEARCH_PATTERNS).format(topic=plain_base)
    if strongest_signal and strongest_signal["source"] == "wikipedia":
        return _pick_pattern(plain_base, GENERIC_ATTENTION_PATTERNS).format(topic=plain_base)
    return _pick_pattern(plain_base, GENERIC_ATTENTION_PATTERNS).format(topic=plain_base)


def _build_subheadline(topic: str, strongest_signal: dict[str, Any] | None) -> str:
    plain_topic = _plain_topic(topic)
    if strongest_signal is None:
        return f"Fresh online attention is building around {plain_topic}."
    source_name = _human_source_name(strongest_signal["source"])
    source_description = _source_description(strongest_signal["source"])
    source = strongest_signal["source"]
    if source == "google_trends":
        return _pick_pattern(plain_topic, GOOGLE_SUBHEADLINE_PATTERNS).format(
            source=source_name,
            description=source_description,
            topic=plain_topic,
        )
    if source == "wikipedia":
        return _pick_pattern(plain_topic, WIKIPEDIA_SUBHEADLINE_PATTERNS).format(
            source=source_name,
            description=source_description,
            topic=plain_topic,
        )
    if source == "reddit":
        return _pick_pattern(plain_topic, REDDIT_SUBHEADLINE_PATTERNS).format(
            source=source_name,
            description=source_description,
            topic=plain_topic,
        )
    return _pick_pattern(plain_topic, GENERIC_SUBHEADLINE_PATTERNS).format(
        source=source_name,
        description=source_description,
        topic=plain_topic,
    )


def _build_summary(topic: str, strongest_signal: dict[str, Any] | None) -> str:
    plain_topic = _plain_topic(topic)
    if strongest_signal is None:
        return f"Fresh online attention is gathering around {plain_topic}, although the reason for the latest burst was not immediately clear."
    source = strongest_signal["source"]
    if source == "google_trends":
        return _pick_pattern(plain_topic, GOOGLE_SUMMARY_PATTERNS).format(topic=plain_topic)
    if source == "wikipedia":
        return _pick_pattern(plain_topic, WIKIPEDIA_SUMMARY_PATTERNS).format(topic=plain_topic)
    if source == "reddit":
        return _pick_pattern(plain_topic, REDDIT_SUMMARY_PATTERNS).format(topic=plain_topic)
    return _pick_pattern(plain_topic, GENERIC_SUMMARY_PATTERNS).format(topic=plain_topic)


def _build_subject_context(topic: str, aliases: list[str]) -> str:
    plain_topic = _plain_topic(topic)
    lowered = plain_topic.lower()
    if "recall" in lowered:
        return _pick_pattern(plain_topic, RECALL_CONTEXT_PATTERNS).format(topic=plain_topic)
    if "cause of death" in lowered or "death" in lowered:
        return _pick_pattern(plain_topic, DEATH_CONTEXT_PATTERNS).format(topic=plain_topic)
    if _looks_like_matchup(plain_topic):
        return _pick_pattern(plain_topic, MATCHUP_CONTEXT_PATTERNS).format(topic=plain_topic)
    if _looks_like_company(plain_topic):
        return _pick_pattern(plain_topic, COMPANY_CONTEXT_PATTERNS).format(topic=plain_topic)
    if _looks_like_person(plain_topic):
        return _pick_pattern(plain_topic, PERSON_CONTEXT_PATTERNS).format(topic=plain_topic)
    if len(aliases) > 1:
        alias_text = ", ".join(_plain_topic(alias) for alias in aliases[1:3])
        return _pick_pattern(plain_topic, ALIAS_CONTEXT_PATTERNS).format(topic=plain_topic, aliases=alias_text)
    return _pick_pattern(plain_topic, GENERIC_CONTEXT_PATTERNS).format(topic=plain_topic)


def _build_related_context(topic: str, strongest_signal: dict[str, Any] | None, source_counts: Counter[str]) -> str:
    plain_topic = _plain_topic(topic)
    if strongest_signal is None:
        return f"The latest burst of attention around {plain_topic} appears to be building without a single clear public trigger."
    source_name = _human_source_name(strongest_signal["source"])
    source_description = _source_description(strongest_signal["source"])
    source_total = sum(source_counts.values())
    if _looks_like_matchup(plain_topic):
        return _pick_pattern(plain_topic, MATCHUP_RELATED_PATTERNS).format(
            topic=plain_topic,
            source=source_name,
            description=source_description,
        )
    if _looks_like_company(plain_topic):
        return _pick_pattern(plain_topic, COMPANY_RELATED_PATTERNS).format(
            topic=plain_topic,
            source=source_name,
            description=source_description,
        )
    if source_total > 1:
        return _pick_pattern(plain_topic, MULTI_SOURCE_RELATED_PATTERNS).format(
            topic=plain_topic,
            source=source_name,
            description=source_description,
        )
    return _pick_pattern(plain_topic, SINGLE_SOURCE_RELATED_PATTERNS).format(
        topic=plain_topic,
        source=source_name,
        description=source_description,
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
    if _looks_like_matchup(plain_topic):
        return [
            f"Attention around {plain_topic} is moving higher quickly.",
            f"{source_name} is showing the strongest lift in {source_description}.",
            "The matchup is drawing renewed fan curiosity online.",
        ]
    if _looks_like_company(plain_topic):
        return [
            f"Attention around {plain_topic} is rising quickly.",
            f"{source_name} is showing the strongest lift in {source_description}.",
            "The company name is drawing fresh online curiosity.",
        ]
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
