## Larry Recsys – Semantic ID Prototype

This repo is a **semantic ID prototype** for the RPG POC. The current work focuses on data foundations; the goal is to evolve this into an **openclaw agent** once the full pipeline and semantics are in place.

---

## Data Foundations (Overture + OSM) – RPG TDD

This repo contains the **Data Foundation** layer for the RPG POC, focused on:

- Sampling **Overture Places** and **OSM** data for local development.
- Building **Silver** (standardized + conflated) and **Gold** (natural language) layers.
- Mirroring the same logic into **Cloud Composer + GCS** for scalable runs.

### Layout

- `src/config/data_foundation.py`: Environment-aware configuration (local vs GCP).
- `src/data/overture_ingest.py`: Overture sampling utilities (DuckDB).
- `src/data/osm_ingest.py`: OSM POI extraction helpers.
- `src/data/conflation.py`: Standardization, spatial conflation, and Gold formatting.
- `src/pipelines/airflow/dags/rpg_data_foundation_dag.py`: Airflow/Composer DAG.
- `src/io/gcs.py`: Small GCS I/O wrapper used by the pipeline.
- `tests/`: Pytest suite driving development (TDD).

### Development

- Install dependencies (from `pyproject.toml`) and run tests:

```bash
pip install -e .
pytest
```

