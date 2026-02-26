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

Install dependencies (from `pyproject.toml`) and run tests:

```bash
pip install -e .
pytest
```

For local runs, use the default local paths under `data/raw` (e.g. pre-download Overture/OSM fixtures), or set the dataset env vars / `RPG_OVERTURE_RELEASE_DATE` to pull from URLs. Set `RPG_LOCAL_OUTPUT_FORMAT=text` to write silver and gold as plain text for inspection.

