## Larry Recsys – Semantic ID Prototype

This repo is a **semantic ID prototype** for the RPG POC. The current work focuses on data foundations; the goal is to evolve this into an **openclaw agent** once the full pipeline and semantics are in place.

---

## Data Foundations (Overture + OSM) – RPG TDD

This repo contains the **Data Foundation** layer for the RPG POC, focused on:

- Sampling **Overture Places** and **OSM** data for local development.
- Building **Silver** (standardized + conflated) and **Gold** (natural language) layers.
- Mirroring the same logic into **Cloud Composer + GCS** for scalable runs.

### Configuration

Configuration is driven by environment variables. Copy `.env.example` to `.env`, set values, and load it (e.g. with `dotenv` or your shell) before running the DAG or tests.

| Purpose | Env vars |
|--------|----------|
| **Environment** | `RPG_ENV` = `local` \| `composer-dev` \| `composer-prod` |
| **GCP / GCS** | `RPG_GCP_PROJECT`, `RPG_GCS_BUCKET_RAW`, `RPG_GCS_BUCKET_SILVER`, `RPG_GCS_BUCKET_GOLD` |
| **Dataset sources** | `RPG_OVERTURE_PLACES_BASE_URL`, `RPG_OVERTURE_PLACES_URI`, `RPG_OVERTURE_RELEASE_DATE`, `RPG_OSM_EXTRACT_URI` (empty = use DAG default local paths) |
| **Local output** | `RPG_LOCAL_OUTPUT_FORMAT` = `parquet` \| `text` (use `text` to write silver as JSONL and gold as `.txt` for inspection) |
| **Optional GCS sync** | `RPG_ENABLE_LOCAL_GCS_SYNC`, `RPG_GCS_GOLD_URI` |

See `.env.example` for all variables and comments.

### Layout

- `src/config/data_foundation.py`: Environment-aware configuration (local vs GCP, dataset URIs, output format).
- `src/data/overture_ingest.py`: Overture sampling utilities (DuckDB); URL built from base + release date.
- `src/data/osm_ingest.py`: OSM POI extraction helpers.
- `src/data/conflation.py`: Standardization, spatial conflation, and Gold formatting.
- `src/pipelines/airflow/dags/rpg_data_foundation_dag.py`: Airflow/Composer DAG.
- `src/io/gcs.py`: Small GCS I/O wrapper used by the pipeline.
- `tests/`: Pytest suite driving development (TDD).

### Development

Install dependencies (from `pyproject.toml`):

```bash
pip install -e .
```

The **Makefile** is the main entry point for tests, local data pulls, and Airflow wrappers (no `pip install` needed for these if you run from repo root with `make`).

#### Make commands

| Command | Description |
|--------|--------------|
| `make` or `make help` | Show available targets and default params |
| **Tests** | |
| `make test` | Run all test groups (config, data, pipelines, io) |
| `make test-config` | Run config tests only |
| `make test-data` | Run data tests (conflation, overture, osm ingest) |
| `make test-pipelines` | Run pipeline/DAG and local-runner tests |
| `make test-io` | Run I/O (GCS) tests |
| **Data pull** | |
| `make pull-data` | Pull Overture + OSM, build silver & gold locally (defaults below) |
| `make pull-data DATE=2024-03-01 SAMPLE_SIZE=5000` | Override Overture release date and row limit |
| `make pull-data DATA_DIR=./my_data OSM_SOURCE=/path/to/osm.parquet` | Custom data dir and OSM source |
| **Airflow** | |
| `make airflow-trigger` | Trigger DAG `rpg_data_foundation` (uses same DATE / SAMPLE_SIZE) |
| `make airflow-unpause` | Unpause the DAG |
| `make airflow-list-dags` | List DAGs |
| `make pull-and-trigger` | Run `pull-data` then `airflow-trigger` |

**Data pull defaults:** `DATE=2026-01-21`, `SAMPLE_SIZE=10000`, `DATA_DIR=`. Overture release IDs use a `.0` suffix (e.g. `2026-01-21.0`); a plain date is normalized automatically. To see available releases: `aws s3 ls s3://overturemaps-us-west-2/release/ --no-sign-request`. Make does not persist these between runs—if you use custom values for `pull-data`, pass the same `DATE` and `SAMPLE_SIZE` when you run `airflow-trigger` (e.g. `make airflow-trigger DATE=2024-03-01 SAMPLE_SIZE=5000`), or use `make pull-and-trigger DATE=... SAMPLE_SIZE=...` to run both with the same params. For the triggered DAG run to use these values, set `RPG_OVERTURE_RELEASE_DATE` and `RPG_OVERTURE_SAMPLE_LIMIT` in the environment where Airflow workers run (e.g. Composer env vars).

For local runs without Make, use the default local paths under `data/raw` (e.g. pre-download Overture/OSM fixtures), or set the dataset env vars / `RPG_OVERTURE_RELEASE_DATE` to pull from URLs. Set `RPG_LOCAL_OUTPUT_FORMAT=text` to write silver and gold as plain text for inspection.

