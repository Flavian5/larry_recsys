## Larry Recsys – Semantic ID Prototype

This repo is a **semantic ID prototype** for the RPG POC. The current work focuses on data foundations; the goal is to evolve this into an **openclaw agent** once the full pipeline and semantics are in place.

**Primary interface:** use the **Makefile** from the repo root for tests, data pulls, and Airflow. No `pip install` is required for tests or local data runs—the Makefile sets `PYTHONPATH=src` automatically.

---

## Using the repo (Makefile)

From the repo root, run `make` or `make help` to see targets and defaults.

### Quick start

```bash
make test                    # run all tests
make fetch-osm               # download OSM (Overpass → data/raw/osm/mini_region.parquet)
make pull-overture           # sample Overture only (S3 → data/raw/overture/temp)
make extract-osm             # run OSM extraction only (mini_region → data/raw/osm/temp/osm_pois)
make build-silver            # conflate raw → silver only (requires overture_sample + osm_pois)
make build-gold              # silver → gold only (requires silver to exist)
make pull-data               # full pipeline (fetches OSM if needed, then Overture + OSM → silver → gold)
```

### Make commands

| Command | Description |
|--------|--------------|
| `make` / `make help` | Show targets and default params |
| **Tests** | |
| `make test` | Run all test groups |
| `make test-config` | Config tests only |
| `make test-data` | Data tests (conflation, overture, osm ingest) |
| `make test-pipelines` | Pipeline/DAG and local-runner tests |
| `make test-io` | I/O (GCS) tests |
| `make test-scripts` | Script tests (e.g. fetch_osm_mini_region) |
| **Data (separate steps)** | |
| `make pull-overture` | Sample Overture Places only → `data/raw/overture/temp/overture_sample.parquet` |
| `make fetch-osm` | Download OSM via Overpass → `data/raw/osm/mini_region.parquet` |
| `make extract-osm` | Run OSM extraction only → `data/raw/osm/temp/osm_pois.parquet` |
| `make build-silver` | Conflate raw (overture_sample + osm_pois) → `data/silver/venues.*` only |
| `make build-gold` | Build gold from silver → `data/gold/venues.*` only |
| **Data (full pipeline)** | |
| `make pull-data` | Fetch OSM (if `OSM_SOURCE` unset), then Overture + OSM extract → silver → gold |
| `make pull-data DATE=2024-03-01 SAMPLE_SIZE=5000` | Override Overture release date and row limit |
| `make pull-data DATA_DIR=./my_data OSM_SOURCE=/path/to/osm.parquet` | Custom data dir and OSM source |
| **Airflow** | |
| `make airflow-trigger` | Trigger DAG `rpg_data_foundation` (uses same DATE / SAMPLE_SIZE) |
| `make airflow-unpause` | Unpause the DAG |
| `make airflow-list-dags` | List DAGs |
| `make pull-and-trigger` | Run `pull-data` then `airflow-trigger` |

**Defaults:** `DATE=2026-01-21`, `SAMPLE_SIZE=10000`, `DATA_DIR=.`, `OSM_BBOX=37.2,-122.52,37.82,-122.35`. Override with `make pull-overture DATE=2024-03-01`, etc. For `pull-data`, if `OSM_SOURCE` is not set, the Makefile runs `fetch-osm` first so the pipeline has `data/raw/osm/mini_region.parquet`. To use your own OSM file, set `OSM_SOURCE=/path/to/file.parquet`.

**Conflation and sample size:** Silver has **one row per Overture place** (OSM POIs are attached only when they fall within the match radius of an Overture centroid). If `SAMPLE_SIZE` is too small, you get fewer silver rows and many OSM POIs never get merged. Use a large enough `SAMPLE_SIZE` for your bbox so that Overture coverage is sufficient to merge the OSM data you care about.

For Airflow/Composer, set `RPG_OVERTURE_RELEASE_DATE` and `RPG_OVERTURE_SAMPLE_LIMIT` in the worker environment so the DAG uses the same date and sample size. Use `make pull-and-trigger DATE=... SAMPLE_SIZE=...` to pull and trigger with the same params.

---

## Configuration

Configuration is driven by environment variables. Copy `.env.example` to `.env`, set values, and load it before running the DAG or tests.

| Purpose | Env vars |
|--------|----------|
| **Environment** | `RPG_ENV` = `local` \| `composer-dev` \| `composer-prod` |
| **GCP / GCS** | `RPG_GCP_PROJECT`, `RPG_GCS_BUCKET_RAW`, `RPG_GCS_BUCKET_SILVER`, `RPG_GCS_BUCKET_GOLD` |
| **Dataset sources** | `RPG_OVERTURE_PLACES_BASE_URL`, `RPG_OVERTURE_PLACES_URI`, `RPG_OVERTURE_RELEASE_DATE`, `RPG_OSM_EXTRACT_URI` (empty = use default local paths) |
| **Local output** | `RPG_LOCAL_OUTPUT_FORMAT` = `parquet` \| `text` (use `text` for silver as JSONL and gold as `.txt`) |
| **Raw temp cleanup** | `RPG_CLEANUP_RAW_TEMP` = `true` (default) deletes raw temp dirs after the pipeline; set to `false` to keep them. |
| **NA tiled DAG** | `RPG_OVERTURE_RELEASE_DATE`, `RPG_OSM_EXTRACT_URI` (GCS URI for North America OSM extract), optional `RPG_NA_TILE_LIMIT` to run only N tiles. |

See `.env.example` for all variables and comments.

---

## Layout

- `src/config/data_foundation.py`: Environment-aware configuration (local vs GCP, dataset URIs, output format).
- `src/data/overture_ingest.py`: Overture sampling (DuckDB); URL from base + release date.
- `src/data/osm_ingest.py`: OSM POI extraction.
- `src/data/conflation.py`: Standardization, H3-based spatial conflation, and Gold formatting.
- `src/data/na_tiles.py`: North America H3 tile list (res 3) and tile-to-bbox helper for the tiled DAG.
- `src/data/tiled_conflation.py`: Per-tile conflation (Overture bbox + OSM bbox filter then conflate).
- `src/pipelines/airflow/dags/rpg_data_foundation_dag.py`: Local/single-bbox DAG (no upload to GCS; cleanup runs).
- `src/pipelines/airflow/dags/rpg_data_foundation_na_dag.py`: Composer-only tiled DAG for North America (one task per H3 tile, then merge and gold).
- `src/pipelines/run_local.py`: Local pipeline runner (invoked by Makefile with `--only` for single-task runs).
- `src/io/gcs.py`: GCS I/O wrapper (upload, download) used by the pipeline and tiled DAG.
- `scripts/fetch_osm_mini_region.py`: Fetch a small OSM region via Overpass API → Parquet (used by `make fetch-osm`).
- `tests/`: Pytest suite.

---

## OSM mini region

The pipeline expects an OSM Parquet at `data/raw/osm/mini_region.parquet` unless you set `OSM_SOURCE` or `RPG_OSM_EXTRACT_URI`. Use the Makefile:

```bash
make fetch-osm                                    # default bbox (SF Bay Area)
make fetch-osm OSM_BBOX=37.2,-122.52,37.82,-122.35
make fetch-osm DATA_DIR=./my_data                 # write under my_data/data/raw/osm/
```

Or run the script directly:

```bash
python scripts/fetch_osm_mini_region.py -o data/raw/osm/mini_region.parquet
python scripts/fetch_osm_mini_region.py --bbox 37.2,-122.52,37.82,-122.35 -o data/raw/osm/mini_region.parquet
```

The script uses the [Overpass API](https://wiki.openstreetmap.org/wiki/Overpass_API) and writes Parquet with `osm_id`, `lat`, `lon`, `amenity`, `cuisine`, `dog_friendly`. For larger regions or full Geofabrik extracts, use a `.osm.pbf` and convert to Parquet (e.g. with [osmium](https://osmcode.org/osmium-tool/) or pyosmium), then point the pipeline at that file via `OSM_SOURCE`.

---

## North America tiled pipeline (Composer)

The **`rpg_data_foundation_na`** DAG runs conflation for North America by splitting the continent into H3 tiles (res 3), running one conflation task per tile, then merging tile silvers and building gold. Use this DAG on Composer for continental-scale runs; for local or single-bbox runs use **`rpg_data_foundation`** instead.

**OSM source:** The tiled DAG expects a North America OSM extract available at a **GCS URI** (e.g. `gs://your-bucket/osm/na_extract.parquet`). Each task downloads and bbox-filters that extract for its tile. Set `RPG_OSM_EXTRACT_URI` in the Composer environment. For very large NA extracts, consider pre-partitioning OSM by H3 tile and storing one Parquet per tile on GCS so each task reads only its tile.

**Required env (Composer):** `RPG_OVERTURE_RELEASE_DATE`, `RPG_OSM_EXTRACT_URI`, and GCS bucket vars (`RPG_GCS_BUCKET_SILVER`, `RPG_GCS_BUCKET_GOLD`). Optional `RPG_NA_TILE_LIMIT=N` limits the run to the first N tiles (for testing).

**Triggering from local CLI:** See the plan or use:

```bash
gcloud composer environments run COMPOSER_ENV --location LOCATION dags trigger -- rpg_data_foundation_na
```

**Scaling beyond Composer:** When tile count or per-tile size outgrows Composer workers, the next step is **Apache Sedona on Dataproc**: a Composer task can trigger a Dataproc job that runs Spark + Sedona, reads Overture and OSM from GCS/S3, uses Sedona’s spatial partitioning and H3 functions, and writes silver/gold back to GCS. This plan does not implement that; it is the documented scaling path.

---

## Development

Install dependencies (from `pyproject.toml`) for local editing and optional CLI usage:

```bash
pip install -e .
```

Prefer **Makefile targets** for day-to-day use. For runs without Make (e.g. custom `run_local` options), use `PYTHONPATH=src` and the same env vars; you can pass `--only overture_sample` or `--only osm_extract` to run a single pipeline task.
