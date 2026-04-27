from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any

import requests

SUBREDDITS = ["technology", "worldnews", "programming", "artificial", "startups"]
USER_AGENT = "viral-public-data/1.0"
BASE_URL = "https://www.reddit.com/r/{subreddit}/hot.json"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _hours_since(created_utc: float, now: datetime | None = None) -> float:
    current_time = now or datetime.now(timezone.utc)
    created_time = datetime.fromtimestamp(created_utc, tz=timezone.utc)
    delta_hours = (current_time - created_time).total_seconds() / 3600
    return max(delta_hours, 1 / 60)


def _normalize_post(post: dict[str, Any], now: datetime | None = None) -> dict[str, Any] | None:
    data = post.get("data", {})
    title = data.get("title")
    created_utc = data.get("created_utc")
    if not title or created_utc is None:
        return None

    score = max(float(data.get("score", 0)), 0.0)
    comments = max(float(data.get("num_comments", 0)), 0.0)
    engagement_velocity = (score + comments) / _hours_since(float(created_utc), now=now)

    return {
        "source": "reddit",
        "topic": title,
        "velocity": engagement_velocity,
        "timestamp": datetime.fromtimestamp(float(created_utc), tz=timezone.utc).isoformat(),
    }


def _fetch_subreddit_posts(
    subreddit: str,
    headers: dict[str, str],
    limit: int,
    timeout: int,
    retries: int,
    backoff_seconds: float,
) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(
                BASE_URL.format(subreddit=subreddit),
                headers=headers,
                params={"limit": limit},
                timeout=timeout,
            )
            if response.status_code in RETRYABLE_STATUS_CODES:
                response.raise_for_status()
            response.raise_for_status()
            payload = response.json()
            return payload.get("data", {}).get("children", [])
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

    raise RuntimeError(f"Reddit fetch failed for r/{subreddit} after {retries} attempts") from last_error


def fetch_trending_reddit_posts(
    subreddits: list[str] | None = None,
    limit: int = 10,
    timeout: int = 10,
    retries: int = 3,
    backoff_seconds: float = 1.0,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    headers = {"User-Agent": USER_AGENT}
    now = datetime.now(timezone.utc)

    for subreddit in subreddits or SUBREDDITS:
        try:
            children = _fetch_subreddit_posts(
                subreddit=subreddit,
                headers=headers,
                limit=limit,
                timeout=timeout,
                retries=retries,
                backoff_seconds=backoff_seconds,
            )
        except RuntimeError:
            continue
        for child in children:
            normalized = _normalize_post(child, now=now)
            if normalized:
                signals.append(normalized)

    return signals
