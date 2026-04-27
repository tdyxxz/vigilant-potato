from __future__ import annotations

import base64
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import re
from typing import Any

import requests
from content.article_generator import generate_article

GITHUB_API_ROOT = "https://api.github.com"
RUNTIME_SYNC_PATHS = (
    ".gitignore",
    "README.md",
    "index.html",
    "article.html",
    "requirements.txt",
    "main.py",
    "github_publisher.py",
    ".github/workflows/pipeline.yml",
    "collectors/google_trends.py",
    "collectors/reddit.py",
    "collectors/wikipedia_views.py",
    "content/article_generator.py",
    "content/seo_builder.py",
    "detection/trend_detector.py",
    "processing/merge_signals.py",
    "processing/normalize.py",
    "processing/saturation.py",
    "processing/scoring.py",
)


def _slugify(value: str) -> str:
    collapsed = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return collapsed or "article"


def _split_repo(repo: str) -> tuple[str, str]:
    owner, _, name = repo.partition("/")
    if not owner or not name:
        raise ValueError("PUBLISH_REPO must be in the format 'owner/repo'")
    return owner, name


def _headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _contents_url(repo: str, path: str) -> str:
    owner, name = _split_repo(repo)
    normalized_path = path.lstrip("/")
    return f"{GITHUB_API_ROOT}/repos/{owner}/{name}/contents/{normalized_path}"


def _extract_title(markdown: str, fallback: str) -> str:
    for line in markdown.splitlines():
        if line.startswith("# "):
            title = line[2:].strip()
            if title:
                return title
    return fallback


def _extract_summary(markdown: str) -> str:
    lines = [line.strip() for line in markdown.splitlines()]
    for index, line in enumerate(lines):
        if line == "## Summary":
            for candidate in lines[index + 1 :]:
                if candidate and not candidate.startswith("#"):
                    return candidate[:280]
    for line in lines:
        if line and not line.startswith("#"):
            return line[:280]
    return ""


def _build_remote_path(*, slug: str, target_directory: str) -> str:
    return f"{target_directory.strip('/')}/{slug}.md"


def _decode_json_content(encoded_content: str) -> Any:
    raw = base64.b64decode(encoded_content.encode("utf-8")).decode("utf-8")
    return json.loads(raw)


def _decode_text_content(encoded_content: str) -> str:
    return base64.b64decode(encoded_content.encode("utf-8")).decode("utf-8")


def _extract_topic_from_markdown(markdown: str) -> str:
    title = _extract_title(markdown, fallback="Article")
    return re.sub(r"\s+(Surges in Online Searches|Draws Fresh Online Attention|Back in Spotlight as Interest Jumps|Suddenly Back in Focus|Searches Surge Again|Gains Traction Online)$", "", title).strip()


def _extract_signal_from_markdown(markdown: str) -> dict[str, Any]:
    lowered = markdown.lower()
    source = "google_trends"
    if "wikipedia" in lowered:
        source = "wikipedia"
    elif "reddit" in lowered:
        source = "reddit"

    formatted_time_match = re.search(r"- ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:]{5} UTC)", markdown)
    formatted_time = formatted_time_match.group(1) if formatted_time_match else "2026-01-01 00:00 UTC"
    timestamp_match = re.match(r"([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9:]{5}) UTC", formatted_time)
    if timestamp_match:
        timestamp = f"{timestamp_match.group(1)}T{timestamp_match.group(2)}:00+00:00"
    else:
        timestamp = "2026-01-01T00:00:00+00:00"

    velocity_match = re.search(r"velocity `([\d.]+)`", markdown)
    velocity = float(velocity_match.group(1)) if velocity_match else 1.0
    topic = _extract_topic_from_markdown(markdown).lower()
    return {
        "topic": topic,
        "source": source,
        "velocity": velocity,
        "timestamp": timestamp,
    }


def file_exists_check(
    repo: str,
    token: str,
    path: str,
    branch: str = "main",
    session: Any = requests,
) -> dict[str, Any] | None:
    response = session.get(
        _contents_url(repo, path),
        headers=_headers(token),
        params={"ref": branch},
        timeout=20,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def _load_existing_index_entries(
    repo: str,
    token: str,
    *,
    branch: str = "main",
    index_path: str = "index.json",
    session: Any = requests,
) -> tuple[list[dict[str, Any]], str | None]:
    existing_index = file_exists_check(repo, token, index_path, branch=branch, session=session)
    existing_entries: list[dict[str, Any]] = []
    existing_sha: str | None = None

    if existing_index:
        existing_sha = existing_index.get("sha")
        encoded_content = existing_index.get("content")
        if encoded_content:
            decoded = _decode_json_content(encoded_content)
            if isinstance(decoded, list):
                existing_entries = [item for item in decoded if isinstance(item, dict)]

    return existing_entries, existing_sha


def publish_file(
    file_path: Path,
    repo: str,
    token: str,
    *,
    branch: str = "main",
    target_directory: str = "posts",
    remote_path: str | None = None,
    published_at: datetime | None = None,
    session: Any = requests,
) -> dict[str, Any]:
    markdown = file_path.read_text(encoding="utf-8")
    slug = _slugify(file_path.stem)
    publish_time = published_at or datetime.now(timezone.utc)
    target_path = remote_path or _build_remote_path(slug=slug, target_directory=target_directory)

    existing_file = file_exists_check(repo, token, target_path, branch=branch, session=session)
    payload = {
        "message": f"auto: publish article {publish_time.isoformat()}",
        "content": base64.b64encode(markdown.encode("utf-8")).decode("utf-8"),
        "branch": branch,
    }
    if existing_file and existing_file.get("sha"):
        payload["sha"] = existing_file["sha"]

    response = session.put(
        _contents_url(repo, target_path),
        headers=_headers(token),
        json=payload,
        timeout=20,
    )
    response.raise_for_status()
    return {
        "title": _extract_title(markdown, fallback=file_path.stem.replace("-", " ").title()),
        "slug": slug,
        "summary": _extract_summary(markdown),
        "timestamp": publish_time.isoformat(),
        "path": target_path,
    }


def update_index(
    entries: list[dict[str, Any]],
    repo: str,
    token: str,
    *,
    branch: str = "main",
    index_path: str = "index.json",
    session: Any = requests,
) -> list[dict[str, Any]]:
    existing_entries, existing_sha = _load_existing_index_entries(
        repo,
        token,
        branch=branch,
        index_path=index_path,
        session=session,
    )

    merged: dict[str, dict[str, Any]] = {str(item.get("slug")): item for item in existing_entries if item.get("slug")}
    for entry in entries:
        merged[entry["slug"]] = entry

    ordered_entries = sorted(merged.values(), key=lambda item: str(item.get("timestamp", "")), reverse=True)
    payload = {
        "message": f"auto: update article index {datetime.now(timezone.utc).isoformat()}",
        "content": base64.b64encode(json.dumps(ordered_entries, indent=2).encode("utf-8")).decode("utf-8"),
        "branch": branch,
    }
    if existing_sha:
        payload["sha"] = existing_sha

    response = session.put(
        _contents_url(repo, index_path),
        headers=_headers(token),
        json=payload,
        timeout=20,
    )
    response.raise_for_status()
    return ordered_entries


def refresh_remote_index_from_posts(
    repo: str,
    token: str,
    *,
    branch: str = "main",
    index_path: str = "index.json",
    session: Any = requests,
) -> list[dict[str, Any]]:
    existing_entries, existing_sha = _load_existing_index_entries(
        repo,
        token,
        branch=branch,
        index_path=index_path,
        session=session,
    )
    refreshed_entries: list[dict[str, Any]] = []

    for entry in existing_entries:
        path = str(entry.get("path") or "")
        if not path:
            refreshed_entries.append(entry)
            continue

        remote_post = file_exists_check(repo, token, path, branch=branch, session=session)
        if not remote_post or not remote_post.get("content"):
            refreshed_entries.append(entry)
            continue

        markdown = _decode_text_content(remote_post["content"])
        refreshed_entries.append(
            {
                "title": _extract_title(markdown, fallback=str(entry.get("title") or entry.get("slug") or "Article")),
                "slug": str(entry.get("slug") or _slugify(Path(path).stem)),
                "summary": _extract_summary(markdown),
                "timestamp": entry.get("timestamp"),
                "path": path,
            }
        )

    payload = {
        "message": f"auto: refresh article index {datetime.now(timezone.utc).isoformat()}",
        "content": base64.b64encode(json.dumps(refreshed_entries, indent=2).encode("utf-8")).decode("utf-8"),
        "branch": branch,
    }
    if existing_sha:
        payload["sha"] = existing_sha

    response = session.put(
        _contents_url(repo, index_path),
        headers=_headers(token),
        json=payload,
        timeout=20,
    )
    response.raise_for_status()
    return refreshed_entries


def refresh_remote_posts(
    repo: str,
    token: str,
    *,
    branch: str = "main",
    target_directory: str = "posts",
    session: Any = requests,
) -> list[str]:
    response = session.get(
        _contents_url(repo, target_directory),
        headers=_headers(token),
        params={"ref": branch},
        timeout=20,
    )
    response.raise_for_status()
    post_items = response.json()
    refreshed_paths: list[str] = []

    for item in post_items:
        path = str(item.get("path") or "")
        name = str(item.get("name") or "")
        if not path.endswith(".md") or name == ".keep":
            continue

        remote_post = file_exists_check(repo, token, path, branch=branch, session=session)
        if not remote_post or not remote_post.get("content"):
            continue
        current_markdown = _decode_text_content(remote_post["content"])
        if "Observed activity suggests" not in current_markdown:
            continue

        signal = _extract_signal_from_markdown(current_markdown)
        regenerated_markdown = generate_article(signal["topic"], [signal])
        payload = {
            "message": f"auto: refresh legacy article {name}",
            "content": base64.b64encode(regenerated_markdown.encode("utf-8")).decode("utf-8"),
            "branch": branch,
            "sha": remote_post.get("sha"),
        }
        put_response = session.put(
            _contents_url(repo, path),
            headers=_headers(token),
            json=payload,
            timeout=20,
        )
        put_response.raise_for_status()
        refreshed_paths.append(path)

    return refreshed_paths


def sync_repository_file(
    local_path: Path,
    repo: str,
    token: str,
    *,
    remote_path: str | None = None,
    branch: str = "main",
    session: Any = requests,
) -> str:
    target_path = (remote_path or local_path.as_posix()).lstrip("/")
    existing_file = file_exists_check(repo, token, target_path, branch=branch, session=session)
    payload = {
        "message": f"auto: sync runtime file {target_path}",
        "content": base64.b64encode(local_path.read_bytes()).decode("utf-8"),
        "branch": branch,
    }
    if existing_file and existing_file.get("sha"):
        payload["sha"] = existing_file["sha"]

    response = session.put(
        _contents_url(repo, target_path),
        headers=_headers(token),
        json=payload,
        timeout=20,
    )
    response.raise_for_status()
    return target_path


def sync_runtime_files(
    base_dir: Path,
    *,
    repo: str | None = None,
    token: str | None = None,
    branch: str = "main",
    session: Any = requests,
) -> dict[str, list[str]]:
    resolved_repo = repo or os.getenv("PUBLISH_REPO")
    resolved_token = token or os.getenv("GH_PUBLISH_TOKEN")
    if not resolved_repo or not resolved_token:
        raise RuntimeError("GitHub repo sync is not configured; expected GH_PUBLISH_TOKEN and PUBLISH_REPO")

    synced_paths: list[str] = []
    failed_paths: list[str] = []
    for relative_path in RUNTIME_SYNC_PATHS:
        local_path = base_dir / relative_path
        if not local_path.exists():
            raise RuntimeError(f"Runtime sync source file does not exist: {local_path.as_posix()}")
        try:
            synced_paths.append(
                sync_repository_file(
                    local_path,
                    resolved_repo,
                    resolved_token,
                    remote_path=relative_path,
                    branch=branch,
                    session=session,
                )
            )
        except requests.RequestException as exc:
            failed_paths.append(relative_path)
            logging.warning("GitHub repo sync failed for %s: %s", relative_path, exc)

    return {"synced_paths": synced_paths, "failed_paths": failed_paths}


def publish_generated_articles(
    articles_dir: Path,
    *,
    repo: str | None = None,
    token: str | None = None,
    branch: str = "main",
    target_directory: str = "posts",
    session: Any = requests,
) -> list[dict[str, Any]]:
    resolved_repo = repo or os.getenv("PUBLISH_REPO")
    resolved_token = token or os.getenv("GH_PUBLISH_TOKEN")
    if not resolved_repo or not resolved_token:
        raise RuntimeError("GitHub publishing is not configured; expected GH_PUBLISH_TOKEN and PUBLISH_REPO")
    if not articles_dir.exists():
        raise RuntimeError(f"Articles directory does not exist: {articles_dir.as_posix()}")

    article_paths = sorted(articles_dir.glob("*.md"))
    if not article_paths:
        logging.info("No generated articles available for GitHub publishing")
        return []

    existing_index = file_exists_check(resolved_repo, resolved_token, "index.json", branch=branch, session=session)
    existing_entries: list[dict[str, Any]] = []
    if existing_index and existing_index.get("content"):
        decoded = _decode_json_content(existing_index["content"])
        if isinstance(decoded, list):
            existing_entries = [item for item in decoded if isinstance(item, dict)]
    remote_paths_by_slug = {
        str(item.get("slug")): str(item.get("path"))
        for item in existing_entries
        if item.get("slug") and item.get("path")
    }

    published_entries: list[dict[str, Any]] = []
    for article_path in article_paths:
        slug = _slugify(article_path.stem)
        remote_path = remote_paths_by_slug.get(slug)
        try:
            published_entry = publish_file(
                article_path,
                resolved_repo,
                resolved_token,
                branch=branch,
                target_directory=target_directory,
                remote_path=remote_path,
                session=session,
            )
            published_entries.append(published_entry)
            logging.info("Published article %s to %s", slug, published_entry["path"])
        except requests.RequestException as exc:
            logging.exception("GitHub publish failed for %s: %s", article_path.name, exc)

    if published_entries:
        update_index(published_entries, resolved_repo, resolved_token, branch=branch, session=session)
        logging.info("Updated GitHub article index with %s entry(s)", len(published_entries))
    else:
        logging.warning("GitHub publishing completed with no successful article uploads")

    return published_entries
