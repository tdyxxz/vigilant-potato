from __future__ import annotations

from datetime import UTC, datetime, timedelta
import time
from typing import Any

import requests

WIKIPEDIA_TOP_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/{year}/{month}/{day}"
USER_AGENT = "viral-public-data/1.0"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _fetch_for_date(target_date: datetime.date, headers: dict[str, str], timeout: int, retries: int, backoff_seconds: float) -> list[dict[str, Any]]:
    url = WIKIPEDIA_TOP_URL.format(
        year=target_date.year,
        month=f"{target_date.month:02d}",
        day=f"{target_date.day:02d}",
    )
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code in RETRYABLE_STATUS_CODES:
                response.raise_for_status()
            response.raise_for_status()
            payload = response.json()
            return payload.get("items", [{}])[0].get("articles", [])
        except requests.HTTPError as exc:
            last_error = exc
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code not in RETRYABLE_STATUS_CODES or attempt == retries:
                break
        except requests.RequestException as exc:
            last_error = exc
            if attempt == retries:
                break

        time.sleep(backoff_seconds * attempt)

    raise RuntimeError(f"Wikipedia fetch failed for {target_date.isoformat()} after {retries} attempts") from last_error


def fetch_wikipedia_top_pages(limit: int = 25, timeout: int = 10, retries: int = 3, backoff_seconds: float = 1.0) -> list[dict[str, Any]]:
    headers = {"User-Agent": USER_AGENT}
    start_date = datetime.now(UTC).date() - timedelta(days=1)
    last_error: Exception | None = None

    for offset in range(0, 7):
        target_date = start_date - timedelta(days=offset)
        try:
            articles = _fetch_for_date(target_date, headers=headers, timeout=timeout, retries=retries, backoff_seconds=backoff_seconds)
            signals: list[dict[str, Any]] = []
            for article in articles[:limit]:
                title = article.get("article", "").replace("_", " ").strip()
                if not title or title in {"Main Page", "Special:Search"}:
                    continue
                views = float(article.get("views", 0))
                signals.append(
                    {
                        "source": "wikipedia",
                        "topic": title,
                        "velocity": views / 24.0,
                        "timestamp": datetime.combine(target_date, datetime.min.time(), tzinfo=UTC).isoformat(),
                    }
                )
            if signals:
                return signals
        except Exception as exc:  # pragma: no cover - network timing dependent
            last_error = exc

    raise RuntimeError("Wikipedia top page fetch failed across fallback dates") from last_error
