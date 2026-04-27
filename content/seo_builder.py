from __future__ import annotations

import re
from collections import Counter

STOPWORDS = {
    "across",
    "activity",
    "article",
    "attention",
    "canonical",
    "comes",
    "currently",
    "elevated",
    "for",
    "from",
    "generated",
    "google",
    "interest",
    "label",
    "measured",
    "momentum",
    "normalized",
    "observed",
    "pageview",
    "registered",
    "rising",
    "search",
    "signal",
    "showed",
    "source",
    "sources",
    "strongest",
    "summary",
    "accelerating",
    "suggests",
    "that",
    "the",
    "this",
    "topic",
    "utc",
    "using",
    "variant",
    "velocity",
    "wikipedia",
    "reddit",
    "with",
}


def _extract_terms(text: str) -> list[str]:
    return [
        word.lower()
        for word in re.findall(r"\b[a-zA-Z][a-zA-Z0-9-]+\b", text)
        if len(word) > 2 and word.lower() not in STOPWORDS
    ]


def build_seo_metadata(article_title: str, article_content: str) -> dict[str, object]:
    lines = article_content.splitlines()
    body_lines = [line for line in lines if not line.strip().startswith("## ")]
    plain_text = re.sub(r"[#`*]", "", "\n".join(body_lines))
    sentences = [segment.strip() for segment in re.split(r"[.!?]\s+", plain_text) if segment.strip()]
    description = (sentences[0] if sentences else plain_text[:155]).strip()
    description = description[:155]

    title = article_title.strip()
    seo_title = title[:60]

    title_words = _extract_terms(title)
    summary_text = " ".join(body_lines[:4])
    summary_words = _extract_terms(summary_text)
    evidence_lines = [line for line in body_lines if line.strip().startswith("-")]
    evidence_words = _extract_terms(" ".join(evidence_lines))
    words = (title_words * 4) + (summary_words * 2) + evidence_words
    keyword_counts = Counter(words)
    keywords = [word for word, _ in keyword_counts.most_common(8)]

    if len(title_words) >= 2:
        title_phrase = " ".join(title_words[: min(4, len(title_words))])
        if title_phrase and title_phrase not in keywords:
            keywords.insert(0, title_phrase)
    keywords = keywords[:10]

    return {
        "seo_title": seo_title,
        "meta_description": description,
        "keywords": keywords,
    }
