from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import requests

try:
    from pytrends.request import TrendReq
except ImportError:  # pragma: no cover - exercised in runtime fallback
    TrendReq = None

RSS_URL = "https://trends.google.com/trending/rss?geo=US"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _build_signal(query: str, interest_score: int, timestamp: str) -> dict[str, Any]:
    return {
        "source": "google_trends",
        "topic": query,
        "velocity": interest_score / 100,
        "timestamp": timestamp,
    }


def _fetch_from_rss(timeout: int = 10, retries: int = 3, backoff_seconds: float = 1.5) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(RSS_URL, timeout=timeout)
            if response.status_code in RETRYABLE_STATUS_CODES:
                response.raise_for_status()
            response.raise_for_status()
            root = ET.fromstring(response.text)
            now = datetime.now(timezone.utc).isoformat()
            items = root.findall("./channel/item")
            signals: list[dict[str, Any]] = []

            for index, item in enumerate(items):
                query = (item.findtext("title") or "").strip()
                if not query:
                    continue
                interest_score = max(100 - (index * 3), 1)
                signals.append(_build_signal(query, interest_score, now))

            return signals
        except requests.HTTPError as exc:
            last_error = exc
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code not in RETRYABLE_STATUS_CODES or attempt == retries:
                break
        except (requests.RequestException, ET.ParseError) as exc:
            last_error = exc
            if attempt == retries:
                break

        time.sleep(backoff_seconds * attempt)

    raise RuntimeError(f"Google Trends RSS fallback failed after {retries} attempts") from last_error


def fetch_google_trends(
    retries: int = 3,
    backoff_seconds: float = 1.5,
) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    if TrendReq is not None:
        for attempt in range(1, retries + 1):
            try:
                pytrends = TrendReq(hl="en-US", tz=360)
                trending_frame = pytrends.trending_searches(pn="united_states")
                now = datetime.now(timezone.utc).isoformat()
                signals: list[dict[str, Any]] = []

                for index, row in trending_frame.iterrows():
                    query = str(row.iloc[0]).strip()
                    if not query:
                        continue
                    interest_score = max(100 - (index * 2), 1)
                    signals.append(_build_signal(query, interest_score, now))
                return signals
            except Exception as exc:  # pragma: no cover - network timing dependent
                last_error = exc
                if attempt < retries:
                    time.sleep(backoff_seconds * attempt)

    try:
        return _fetch_from_rss(retries=retries, backoff_seconds=backoff_seconds)
    except Exception as exc:  # pragma: no cover - network timing dependent
        if last_error is None:
            last_error = exc
        raise RuntimeError(f"Google Trends fetch failed after {retries} attempts and RSS fallback") from last_error
