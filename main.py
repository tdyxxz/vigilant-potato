from __future__ import annotations

import argparse
import atexit
from datetime import datetime, timezone
import json
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from collectors.google_trends import fetch_google_trends
from collectors.reddit import fetch_trending_reddit_posts
from collectors.wikipedia_views import fetch_wikipedia_top_pages
from content.article_generator import article_record_to_dict, article_record_to_markdown, build_article_record
from content.seo_builder import build_seo_metadata
from detection.trend_detector import derive_detection_threshold, detect_emerging_topics
from github_publisher import publish_generated_articles, sync_runtime_files
from processing.merge_signals import merge_signals
from processing.normalize import is_high_quality_topic, normalize_signals
from processing.saturation import estimate_saturation
from processing.scoring import apply_historical_deltas, apply_topic_ledger, score_topics


@dataclass(frozen=True)
class PipelineConfig:
    output_dir: Path = Path("output")
    threshold: float = 0.45
    saturation_limit: float = 0.7
    top_n: int = 10

    @property
    def articles_dir(self) -> Path:
        return self.output_dir / "articles"

    @property
    def records_dir(self) -> Path:
        return self.output_dir / "records"

    @property
    def history_dir(self) -> Path:
        return self.output_dir / "history"

    @property
    def lock_path(self) -> Path:
        return self.output_dir / ".run.lock"

    @property
    def ledger_path(self) -> Path:
        return self.output_dir / "ledger.json"

    @property
    def summary_path(self) -> Path:
        return self.output_dir / "summary.json"

    @property
    def history_manifest_path(self) -> Path:
        return self.output_dir / "history_manifest.json"


def _slugify(value: str) -> str:
    return "-".join("".join(char.lower() if char.isalnum() else " " for char in value).split())[:80] or "topic"


def run_collector(name: str, collector: Callable[[], list[dict]]) -> tuple[list[dict], dict[str, object]]:
    try:
        signals = collector()
        logging.info("Collector %s produced %s signal(s)", name, len(signals))
        return signals, {"name": name, "status": "ok", "signal_count": len(signals), "error": None}
    except Exception as exc:
        logging.exception("Collector %s failed: %s", name, exc)
        return [], {"name": name, "status": "failed", "signal_count": 0, "error": str(exc)}


def acquire_run_lock(config: PipelineConfig) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    if config.lock_path.exists():
        raise RuntimeError(f"Pipeline run already in progress: {config.lock_path.as_posix()}")
    config.lock_path.write_text(str(datetime.now(timezone.utc).isoformat()), encoding="utf-8")
    atexit.register(release_run_lock, config)


def release_run_lock(config: PipelineConfig) -> None:
    if config.lock_path.exists():
        config.lock_path.unlink()


def load_previous_index(config: PipelineConfig) -> list[dict]:
    index_path = config.output_dir / "index.json"
    if not index_path.exists():
        return []
    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            topics = payload.get("topics", [])
            return topics if isinstance(topics, list) else []
        return []
    except json.JSONDecodeError:
        logging.warning("Previous index exists but could not be parsed; ignoring historical baseline")
        return []


def load_topic_ledger(config: PipelineConfig) -> dict[str, list[dict]]:
    if not config.ledger_path.exists():
        return {}
    try:
        ledger = json.loads(config.ledger_path.read_text(encoding="utf-8"))
        return {topic: history for topic, history in ledger.items() if is_high_quality_topic(topic)}
    except json.JSONDecodeError:
        logging.warning("Topic ledger exists but could not be parsed; ignoring ledger baseline")
        return {}


def archive_previous_run(config: PipelineConfig, previous_index: list[dict]) -> None:
    if not previous_index and not config.summary_path.exists():
        return
    config.history_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if previous_index:
        archive_path = config.history_dir / f"index-{timestamp}.json"
        archive_path.write_text(json.dumps(previous_index, indent=2), encoding="utf-8")
    if config.summary_path.exists():
        summary_archive_path = config.history_dir / f"summary-{timestamp}.json"
        summary_archive_path.write_text(config.summary_path.read_text(encoding="utf-8"), encoding="utf-8")


def build_history_manifest(config: PipelineConfig) -> list[dict[str, object]]:
    manifest: list[dict[str, object]] = []
    if not config.history_dir.exists():
        return manifest

    summaries_by_stamp: dict[str, dict[str, object]] = {}
    for summary_path in sorted(config.history_dir.glob("summary-*.json")):
        stamp = summary_path.stem.removeprefix("summary-")
        try:
            summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            summary_payload = {}
        summaries_by_stamp[stamp] = summary_payload

    for index_path in sorted(config.history_dir.glob("index-*.json")):
        stamp = index_path.stem.removeprefix("index-")
        summary_path = config.history_dir / f"summary-{stamp}.json"
        summary_payload = summaries_by_stamp.get(stamp, {})
        manifest.append(
            {
                "timestamp": stamp,
                "index_path": index_path.as_posix(),
                "summary_path": summary_path.as_posix() if summary_path.exists() else None,
                "generated_at": summary_payload.get("generated_at"),
                "raw_signal_count": summary_payload.get("raw_signal_count"),
                "normalized_signal_count": summary_payload.get("normalized_signal_count"),
                "cluster_count": summary_payload.get("cluster_count"),
                "emerging_topic_count": summary_payload.get("emerging_topic_count"),
                "adaptive_threshold": summary_payload.get("adaptive_threshold"),
                "is_degraded": summary_payload.get("is_degraded"),
                "is_backfilled": summary_payload.get("is_backfilled"),
                "failed_collectors": summary_payload.get("failed_collectors", []),
            }
        )

    return manifest


def _load_archived_index_topics(index_path: Path) -> list[dict]:
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        topics = payload.get("topics", [])
        return topics if isinstance(topics, list) else []
    return []


def _summary_from_archived_index(index_path: Path, topics: list[dict]) -> dict[str, object]:
    timestamp = index_path.stem.removeprefix("index-")
    generated_at = None
    try:
        generated_at = datetime.strptime(timestamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        generated_at = None

    thresholds = [topic.get("detection_threshold") for topic in topics if topic.get("detection_threshold") is not None]
    adaptive_threshold = thresholds[0] if thresholds else None
    collector_names = sorted({source for topic in topics for source in topic.get("sources", [])})

    return {
        "generated_at": generated_at,
        "raw_signal_count": None,
        "normalized_signal_count": None,
        "cluster_count": None,
        "scored_topic_count": len(topics),
        "emerging_topic_count": len(topics),
        "adaptive_threshold": adaptive_threshold,
        "collector_count": len(collector_names),
        "failed_collectors": [],
        "is_degraded": None,
        "is_backfilled": True,
        "config": None,
    }


def backfill_missing_history_summaries(config: PipelineConfig) -> None:
    if not config.history_dir.exists():
        return
    for index_path in sorted(config.history_dir.glob("index-*.json")):
        stamp = index_path.stem.removeprefix("index-")
        summary_path = config.history_dir / f"summary-{stamp}.json"
        if summary_path.exists():
            continue
        try:
            topics = _load_archived_index_topics(index_path)
        except json.JSONDecodeError:
            continue
        summary_payload = _summary_from_archived_index(index_path, topics)
        summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")


def run_history_maintenance(config: PipelineConfig) -> dict[str, object]:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    backfill_missing_history_summaries(config)
    manifest = build_history_manifest(config)
    config.history_manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    result = {
        "backfilled_summary_count": len(
            [
                item
                for item in manifest
                if item.get("summary_path")
                and item.get("is_backfilled") is True
            ]
        ),
        "manifest_entry_count": len(manifest),
        "history_manifest_path": config.history_manifest_path.as_posix(),
    }
    logging.info(
        "History maintenance completed: %s manifest entry(s), %s backfilled summary file(s)",
        result["manifest_entry_count"],
        result["backfilled_summary_count"],
    )
    return result


def update_topic_ledger(
    scored_topics: list[dict],
    topic_ledger: dict[str, list[dict]],
    observed_at: str,
    history_window: int = 12,
) -> dict[str, list[dict]]:
    updated_ledger = {key: list(value) for key, value in topic_ledger.items()}
    for topic in scored_topics:
        ledger_key = str(topic.get("topic", "")).strip().lower()
        if not ledger_key:
            continue
        entry = {
            "observed_at": observed_at,
            "trend_score": topic.get("trend_score"),
            "velocity": topic.get("velocity"),
            "weighted_velocity": topic.get("weighted_velocity"),
            "saturation": topic.get("saturation"),
            "source_count": topic.get("source_count"),
        }
        updated_ledger.setdefault(ledger_key, []).append(entry)
        updated_ledger[ledger_key] = updated_ledger[ledger_key][-history_window:]
    return updated_ledger


def build_run_summary(
    *,
    collector_health: list[dict[str, object]],
    raw_signal_count: int,
    normalized_signal_count: int,
    cluster_count: int,
    scored_topic_count: int,
    emerging_topic_count: int,
    adaptive_threshold: float,
    config: PipelineConfig,
) -> dict[str, object]:
    failed_collectors = [item["name"] for item in collector_health if item.get("status") != "ok"]
    degraded = bool(failed_collectors)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "raw_signal_count": raw_signal_count,
        "normalized_signal_count": normalized_signal_count,
        "cluster_count": cluster_count,
        "scored_topic_count": scored_topic_count,
        "emerging_topic_count": emerging_topic_count,
        "adaptive_threshold": adaptive_threshold,
        "collector_count": len(collector_health),
        "failed_collectors": failed_collectors,
        "is_degraded": degraded,
        "is_backfilled": False,
        "config": {
            "threshold_floor": config.threshold,
            "saturation_limit": config.saturation_limit,
            "top_n": config.top_n,
            "output_dir": config.output_dir.as_posix(),
        },
    }


def save_outputs(
    emerging_topics: list[dict],
    topic_ledger: dict[str, list[dict]],
    collector_health: list[dict[str, object]],
    run_summary: dict[str, object],
    config: PipelineConfig,
) -> None:
    previous_index = load_previous_index(config)
    archive_previous_run(config, previous_index)
    backfill_missing_history_summaries(config)
    history_snapshot: list[tuple[str, str]] = []
    if config.history_dir.exists():
        for snapshot in config.history_dir.glob("*.json"):
            history_snapshot.append((snapshot.name, snapshot.read_text(encoding="utf-8")))
    ledger_snapshot = config.ledger_path.read_text(encoding="utf-8") if config.ledger_path.exists() else None
    if config.output_dir.exists():
        shutil.rmtree(config.output_dir)
    config.articles_dir.mkdir(parents=True, exist_ok=True)
    config.records_dir.mkdir(parents=True, exist_ok=True)
    config.history_dir.mkdir(parents=True, exist_ok=True)
    for snapshot_name, snapshot_content in history_snapshot:
        preserved_path = config.history_dir / snapshot_name
        preserved_path.write_text(snapshot_content, encoding="utf-8")
    if ledger_snapshot:
        config.ledger_path.write_text(ledger_snapshot, encoding="utf-8")
    output_payload = {
        "generated_at": run_summary["generated_at"],
        "collector_health": collector_health,
        "summary_path": config.summary_path.as_posix(),
        "topics": [],
    }

    for topic_data in emerging_topics:
        record = build_article_record(topic_data["topic"], topic_data.get("signals", []), aliases=topic_data.get("aliases"))
        article = article_record_to_markdown(record)
        seo = build_seo_metadata(record.title, article)
        slug = _slugify(topic_data["topic"])
        article_path = config.articles_dir / f"{slug}.md"
        record_path = config.records_dir / f"{slug}.json"
        article_path.write_text(article, encoding="utf-8")
        record_path.write_text(json.dumps(article_record_to_dict(record), indent=2), encoding="utf-8")

        output_payload["topics"].append(
            {
                "topic": topic_data["topic"],
                "title": record.title,
                "aliases": topic_data.get("aliases", [topic_data["topic"]]),
                "trend_score": topic_data["trend_score"],
                "detection_threshold": topic_data.get("detection_threshold"),
                "velocity": topic_data["velocity"],
                "weighted_velocity": topic_data.get("weighted_velocity"),
                "corroboration_bonus": topic_data.get("corroboration_bonus"),
                "previous_trend_score": topic_data.get("previous_trend_score"),
                "trend_delta": topic_data.get("trend_delta"),
                "velocity_delta": topic_data.get("velocity_delta"),
                "delta_bonus": topic_data.get("delta_bonus"),
                "rolling_trend_average": topic_data.get("rolling_trend_average"),
                "rolling_velocity_average": topic_data.get("rolling_velocity_average"),
                "momentum_delta": topic_data.get("momentum_delta"),
                "acceleration_delta": topic_data.get("acceleration_delta"),
                "ledger_bonus": topic_data.get("ledger_bonus"),
                "saturation": topic_data["saturation"],
                "sources": topic_data.get("sources", []),
                "article_path": article_path.as_posix(),
                "record_path": record_path.as_posix(),
                "seo": seo,
            }
        )

    index_path = config.output_dir / "index.json"
    index_path.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")
    config.ledger_path.write_text(json.dumps(topic_ledger, indent=2), encoding="utf-8")
    config.summary_path.write_text(json.dumps(run_summary, indent=2), encoding="utf-8")
    config.history_manifest_path.write_text(json.dumps(build_history_manifest(config), indent=2), encoding="utf-8")
    logging.info("Saved %s article(s) and index file", len(output_payload["topics"]))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the viral public data pipeline.")
    subparsers = parser.add_subparsers(dest="command")

    def add_shared_output_arg(target: argparse.ArgumentParser) -> None:
        target.add_argument("--output-dir", default="output", help="Directory for generated artifacts.")

    run_parser = subparsers.add_parser("run", help="Run the full viral signal pipeline.")
    add_shared_output_arg(run_parser)
    run_parser.add_argument("--threshold", type=float, default=0.45, help="Minimum floor for the adaptive trend threshold.")
    run_parser.add_argument("--saturation-limit", type=float, default=0.7, help="Maximum saturation allowed.")
    run_parser.add_argument("--top-n", type=int, default=10, help="Maximum number of emerging topics to save.")

    backfill_parser = subparsers.add_parser(
        "backfill-history",
        help="Backfill missing archived summaries and rebuild the history manifest.",
    )
    add_shared_output_arg(backfill_parser)

    manifest_parser = subparsers.add_parser(
        "rebuild-manifest",
        help="Rebuild the history manifest from the current archive state.",
    )
    add_shared_output_arg(manifest_parser)

    publish_parser = subparsers.add_parser(
        "publish",
        help="Publish generated articles in the output directory to GitHub without rerunning collectors.",
    )
    add_shared_output_arg(publish_parser)

    sync_parser = subparsers.add_parser(
        "sync-repo",
        help="Sync the pipeline runtime files and workflow into the target GitHub repository.",
    )
    add_shared_output_arg(sync_parser)

    parser.set_defaults(
        command="run",
        output_dir="output",
        threshold=0.45,
        saturation_limit=0.7,
        top_n=10,
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = PipelineConfig(
        output_dir=Path(args.output_dir),
        threshold=args.threshold,
        saturation_limit=args.saturation_limit,
        top_n=args.top_n,
    )
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.info("Starting viral signal aggregation pipeline")
    acquire_run_lock(config)

    try:
        if args.command == "backfill-history":
            run_history_maintenance(config)
            return
        if args.command == "rebuild-manifest":
            config.output_dir.mkdir(parents=True, exist_ok=True)
            manifest = build_history_manifest(config)
            config.history_manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            logging.info("Rebuilt history manifest with %s entry(s)", len(manifest))
            return
        if args.command == "publish":
            published_entries = publish_generated_articles(config.articles_dir)
            logging.info("Publish-only command completed with %s published article(s)", len(published_entries))
            return
        if args.command == "sync-repo":
            sync_result = sync_runtime_files(Path("."))
            logging.info(
                "Repo sync completed with %s synced file(s) and %s failed file(s)",
                len(sync_result["synced_paths"]),
                len(sync_result["failed_paths"]),
            )
            return

        raw_signals: list[dict] = []
        collector_health: list[dict[str, object]] = []

        reddit_signals, reddit_health = run_collector("reddit", fetch_trending_reddit_posts)
        raw_signals.extend(reddit_signals)
        collector_health.append(reddit_health)

        google_signals, google_health = run_collector("google_trends", fetch_google_trends)
        raw_signals.extend(google_signals)
        collector_health.append(google_health)

        wiki_signals, wiki_health = run_collector("wikipedia", fetch_wikipedia_top_pages)
        raw_signals.extend(wiki_signals)
        collector_health.append(wiki_health)

        logging.info("Total raw signals collected: %s", len(raw_signals))
        normalized = normalize_signals(raw_signals)
        logging.info("Normalized signals: %s", len(normalized))

        clusters = merge_signals(normalized)
        logging.info("Clusters formed: %s", len(clusters))

        saturated = estimate_saturation(clusters)
        previous_index = load_previous_index(config)
        topic_ledger = load_topic_ledger(config)
        scored = score_topics(saturated)
        scored = apply_historical_deltas(scored, previous_index)
        scored = apply_topic_ledger(scored, topic_ledger)
        threshold = derive_detection_threshold(scored, minimum_threshold=config.threshold)
        logging.info("Adaptive detection threshold: %.4f", threshold)
        emerging = detect_emerging_topics(
            scored,
            threshold=config.threshold,
            saturation_limit=config.saturation_limit,
            top_n=config.top_n,
            adaptive=True,
        )
        logging.info("Emerging topics detected: %s", len(emerging))

        observed_at = datetime.now(timezone.utc).isoformat()
        updated_ledger = update_topic_ledger(scored, topic_ledger, observed_at=observed_at)
        run_summary = build_run_summary(
            collector_health=collector_health,
            raw_signal_count=len(raw_signals),
            normalized_signal_count=len(normalized),
            cluster_count=len(clusters),
            scored_topic_count=len(scored),
            emerging_topic_count=len(emerging),
            adaptive_threshold=threshold,
            config=config,
        )
        save_outputs(emerging, updated_ledger, collector_health, run_summary, config)
        try:
            publish_generated_articles(config.articles_dir)
        except RuntimeError as exc:
            logging.info("Skipping GitHub publish: %s", exc)
        except Exception as exc:  # pragma: no cover - network timing dependent
            logging.exception("GitHub publish step failed: %s", exc)
        logging.info("Pipeline completed")
    finally:
        release_run_lock(config)


if __name__ == "__main__":
    main()
