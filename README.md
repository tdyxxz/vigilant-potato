# Viral Public Data

Real-time viral signal aggregation pipeline for early neutral topic coverage.

## Run

```bash
python main.py
python main.py run
```

Options:

```bash
python main.py run --threshold 0.45 --saturation-limit 0.65 --top-n 5 --output-dir output
```

History maintenance without live collectors:

```bash
python main.py backfill-history --output-dir output
python main.py rebuild-manifest --output-dir output
python main.py publish --output-dir output
python main.py sync-repo --output-dir output
```

## Output

- `output/articles/*.md`
- `output/records/*.json`
- `output/history/*.json`
- `output/ledger.json`
- `output/index.json`
- `output/summary.json`
- `output/history_manifest.json`

Each run rewrites the current output set and archives the previous `index.json` and `summary.json` into `output/history/` for delta comparisons.
Concurrent runs are blocked with `output/.run.lock` so the history and current batch cannot overwrite each other.
`backfill-history` reconstructs missing legacy `summary-*.json` files where possible and rewrites `output/history_manifest.json`.
`rebuild-manifest` only regenerates `output/history_manifest.json` from the current archive state.

## GitHub Publishing

The pipeline can publish generated markdown articles to a GitHub repository through the GitHub Contents API.

Required environment variables:

- `GH_PUBLISH_TOKEN`
- `PUBLISH_REPO` in `owner/repo` format, here `tdyxxz/vigilant-potato`

Behavior:

- generated articles from `output/articles/` are uploaded to `posts/<slug>.md`
- duplicate slugs reuse the previously published remote path
- remote `index.json` is updated after successful article uploads
- if publishing is not configured or a publish request fails, the local pipeline still completes
- `python main.py publish` pushes the current local `output/articles/` set without collecting new signals
- `python main.py sync-repo` pushes the workflow and runtime Python files into the target GitHub repository

GitHub Actions workflow:

- `.github/workflows/pipeline.yml`
- runs every 2 hours
- supports `workflow_dispatch`
- expects repository secrets named `GH_PUBLISH_TOKEN` and `PUBLISH_REPO`

Do not commit a personal access token into the repository. The local `github token.txt` file is ignored and should be treated as temporary setup material only.

## Test

```bash
pytest
```
