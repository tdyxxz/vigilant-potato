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
    "{topic} Is Suddenly Everywhere - Here's Why",
    "The {topic} Hype Is Real - Here's What's Behind It",
    "Everyone's Talking About {topic} - Here's The Story",
    "{topic} Just Blew Up - Here's What You Need To Know",
    "Why {topic} Is Suddenly Taking Over Your Feed",
)
RECALL_HEADLINE_PATTERNS = (
    "{topic} Recall: What You Need To Know Right Now",
    "Breaking: {topic} Safety Alert - Consumer Warning",
    "{topic} Recall Spreads - Here's Who's Affected",
    "Urgent {topic} Recall Update - Check Your Items Now",
)
MATCHUP_HEADLINE_PATTERNS = (
    "{topic}: The Game Everyone's Watching",
    "{topic} Is Breaking The Internet - Here's Why",
    "Can't Miss This {topic} Showdown - Here's The Buzz",
    "{topic} Has Fans Going Wild - Here's The Drama",
)
COMPANY_HEADLINE_PATTERNS = (
    "{topic} Is Suddenly Headline News - Here's What Happened",
    "Breaking: {topic} Shakes Up The Industry",
    "{topic} Just Made A Major Move - Here's The Impact",
    "Why {topic} Is Suddenly Everyone's Business",
)
DEATH_HEADLINE_PATTERNS = (
    "{topic}: The Questions Everyone's Asking Again",
    "Why {topic} Is Suddenly Trending - The Full Story",
    "{topic} Mystery Resurfaces - Here's What We Know",
    "The {topic} Conversation Is Back - Here's Why",
)
GENERIC_SEARCH_PATTERNS = (
    "{topic} Is Suddenly Everywhere - Here's The Story",
    "Breaking: Why {topic} Is Suddenly Trending",
    "{topic} Just Exploded Online - Here's What's Going On",
    "The {topic} Phenomenon - Here's Why It's Viral",
)
GENERIC_ATTENTION_PATTERNS = (
    "{topic} Is Suddenly Breaking The Internet",
    "Why {topic} Is Suddenly Everyone's Obsession",
    "{topic} Just Went Viral - Here's The Real Story",
    "The {topic} Buzz: Here's What's Actually Happening",
)
GOOGLE_SUMMARY_PATTERNS = (
    "Something big is happening with {topic} - Google searches are suddenly spiking and everyone wants to know what's going on.",
    "The internet can't stop searching for {topic} right now - here's what's driving this massive surge of interest.",
    "{topic} just exploded on Google search trends - we've got the inside scoop on what's behind this viral moment.",
)
WIKIPEDIA_SUMMARY_PATTERNS = (
    "Everyone's suddenly rushing to read about {topic} on Wikipedia - here's why this topic is blowing up right now.",
    "{topic} Wikipedia pages are getting hammered with traffic - something major is happening and people need answers.",
    "Massive spike in {topic} Wikipedia readership - here's the story behind this sudden surge of interest.",
)
REDDIT_SUMMARY_PATTERNS = (
    "Reddit is absolutely blowing up with {topic} discussions - here's the viral thread that's got everyone talking.",
    "{topic} is taking over Reddit right now - we've tracked down the conversations that are breaking the internet.",
    "From zero to viral: {topic} just exploded across Reddit communities - here's what started this digital wildfire.",
)
GENERIC_SUMMARY_PATTERNS = (
    "{topic} is suddenly everywhere online - here's what's behind this viral moment that's got everyone talking.",
    "The internet just discovered {topic} and now it's blowing up - we've got the story behind this viral sensation.",
    "From nowhere to everywhere: {topic} just went viral and here's everything you need to know about this sudden explosion of interest.",
)
GOOGLE_SUBHEADLINE_PATTERNS = (
    "Google searches for {topic} are going absolutely viral right now - here's what's driving this explosion.",
    "Everyone's suddenly Googling {topic} - we've got the data behind this massive search surge.",
    "Google trends show {topic} is breaking the internet - here's the story behind this viral moment.",
)
WIKIPEDIA_SUBHEADLINE_PATTERNS = (
    "Wikipedia pages for {topic} are getting absolutely hammered with readers - here's why everyone needs to know.",
    "Massive traffic spike on {topic} Wikipedia pages - something big is happening and people are scrambling for answers.",
    "{topic} Wikipedia articles are blowing up - here's the story behind this sudden surge of readers.",
)
REDDIT_SUBHEADLINE_PATTERNS = (
    "Reddit is absolutely on fire with {topic} discussions - here's the viral threads that are breaking the internet.",
    "{topic} is taking over Reddit right now - we've found the conversations that everyone's talking about.",
    "From zero to viral: {topic} just exploded across Reddit - here's what started this digital wildfire.",
)
GENERIC_SUBHEADLINE_PATTERNS = (
    "{topic} is suddenly everywhere online - here's what's behind this viral explosion.",
    "The internet just discovered {topic} and now it's blowing up - here's the real story.",
    "{topic} just went viral across the web - here's everything you need to know about this sudden explosion.",
)
PERSON_CONTEXT_PATTERNS = (
    "{topic} is clearly a major public figure, and this viral spike shows they're back in the spotlight in a huge way.",
    "The internet can't stop talking about {topic} right now - this person is suddenly everywhere across social media and search.",
    "{topic} is having a major viral moment - everyone's suddenly searching for info about this public figure.",
)
RECALL_CONTEXT_PATTERNS = (
    "This recall alert is spreading like wildfire across the internet - everyone's scrambling to check if they're affected.",
    "Consumer safety warnings about {topic} are going viral - people everywhere are searching for recall details and safety info.",
    "The {topic} recall is breaking the internet as shoppers rush to find out what products are affected and what to do next.",
)
DEATH_CONTEXT_PATTERNS = (
    "The internet is suddenly obsessed with {topic} again - here's what's driving this renewed wave of curiosity.",
    "Old questions about {topic} are resurfacing in a big way - everyone's suddenly searching for answers again.",
    "{topic} is back in the spotlight as renewed interest spreads across social media and search platforms.",
)
MATCHUP_CONTEXT_PATTERNS = (
    "This game is absolutely breaking the internet right now - fans can't stop talking about {topic} across every platform.",
    "Sports fans are going absolutely wild for {topic} - this matchup is suddenly everyone's must-watch event.",
    "The {topic} game is blowing up online as fans rush to find updates, highlights and real-time buzz about this showdown.",
)
COMPANY_CONTEXT_PATTERNS = (
    "{topic} is suddenly headline news - something major is happening and everyone needs to know what's going on.",
    "Business news about {topic} is spreading like wildfire - investors and customers are scrambling for details.",
    "The internet is exploding with {topic} discussions - this company is suddenly at the center of everyone's attention.",
)
ALIAS_CONTEXT_PATTERNS = (
    "People are also searching for {aliases} - this story is spreading across multiple keywords and platforms.",
    "The viral conversation includes {aliases} - showing this story is blowing up under different names and searches.",
    "Multiple versions like {aliases} are trending - proving this topic has gone completely viral across the web.",
)
GENERIC_CONTEXT_PATTERNS = (
    "Something huge is happening with {topic} right now - the internet is absolutely buzzing and everyone wants answers.",
    "{topic} just exploded out of nowhere and now it's everywhere - here's what's behind this viral sensation.",
    "The internet can't stop talking about {topic} - this sudden surge of interest has everyone wondering what's going on.",
)
MATCHUP_RELATED_PATTERNS = (
    "The biggest buzz is coming from {source} where fans are going absolutely wild over this {topic} matchup.",
    "{source} is leading the viral charge as sports fans can't stop talking about this {topic} showdown.",
    "All eyes are on {source} where this {topic} game is absolutely breaking the internet right now.",
)
COMPANY_RELATED_PATTERNS = (
    "The biggest surge is coming from {source} where everyone's scrambling for details about {topic}.",
    "{source} is driving this viral moment as people rush to find out what's happening with {topic}.",
    "All the buzz is coming from {source} where {topic} is suddenly everyone's top search priority.",
)
MULTI_SOURCE_RELATED_PATTERNS = (
    "This {topic} story is absolutely exploding across multiple platforms - {source} is leading the charge but it's going viral everywhere.",
    "The {topic} buzz is spreading like wildfire - {source} is the biggest driver but this story is breaking the entire internet.",
    "{source} sparked this {topic} viral moment but now it's absolutely everywhere across multiple platforms and communities.",
)
SINGLE_SOURCE_RELATED_PATTERNS = (
    "The entire {topic} viral wave is coming from {source} - this platform is absolutely on fire with this story.",
    "{source} is the epicenter of this {topic} explosion - everyone's flocking there to see what's happening.",
    "This {topic} phenomenon started on {source} and it's absolutely breaking records with engagement and interest.",
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
            f"{plain_topic} is suddenly trending across multiple platforms.",
            "Everyone's scrambling to find out what's behind this viral moment.",
        ]
    source_name = _human_source_name(strongest_signal["source"])
    source_description = _source_description(strongest_signal["source"])
    if _looks_like_matchup(plain_topic):
        return [
            f"This {plain_topic} game is absolutely breaking the internet right now.",
            f"{source_name} is showing massive spikes in {source_description} as fans go wild.",
            "Sports betting sites and fan forums are also blowing up with this matchup.",
        ]
    if _looks_like_company(plain_topic):
        return [
            f"{plain_topic} is suddenly headline news across every platform.",
            f"{source_name} is driving massive {source_description} as investors and customers scramble for details.",
            "Stock forums and business news sites are also covering this heavily.",
        ]
    return [
        f"{plain_topic} just exploded online and everyone's talking about it.",
        f"{source_name} is showing massive spikes in {source_description} right now.",
        f"This story is trending across {len(source_counts)} major platform{'s' if len(source_counts) != 1 else ''}.",
    ]


def _timeline_line(topic: str, signal: dict[str, Any]) -> str:
    source_name = _human_source_name(signal["source"])
    source_description = _source_description(signal["source"])
    observed_topic = _plain_topic(str(signal["topic"]))
    plain_topic = _plain_topic(topic)
    if observed_topic != plain_topic:
        return f"- {_format_timestamp(signal['timestamp'])}: {source_name} helped spark viral interest around {observed_topic}."
    return f"- {_format_timestamp(signal['timestamp'])}: {source_name} showed massive spike in {source_description} that started this viral wave."


def _evidence_line(evidence_item: dict[str, Any]) -> str:
    source_name = _human_source_name(evidence_item["source"])
    observed_topic = _plain_topic(str(evidence_item["observed_topic"]))
    return f"- {source_name} was one of the first platforms to break this {observed_topic} viral story."


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
