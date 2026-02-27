# Data foundation pipeline: tests, local data pull, and Airflow wrappers.
# Run from repo root. Uses PYTHONPATH=src so no need to pip install for tests/run_local.

.DEFAULT_GOAL := help
.PHONY: help

help:
	@echo "Data foundation Makefile"
	@echo ""
	@echo "Tests:          make test  (or test-config, test-data, test-pipelines, test-io, test-scripts)"
	@echo "Data (separate): make pull-overture   - sample Overture only (S3 -> data/raw/overture/temp)"
	@echo "                 make fetch-osm      - download OSM only (Overpass -> data/raw/osm/mini_region.parquet)"
	@echo "                 make extract-osm   - run OSM extraction only (mini_region -> data/raw/osm/temp/osm_pois)"
	@echo "                 make build-silver  - conflate raw -> silver only; make build-gold - silver -> gold only"
	@echo "Data (full):    make pull-data [DATE=...] [SAMPLE_SIZE=10000] [DATA_DIR=.] [OSM_BBOX=...] [OSM_SOURCE=path]"
	@echo "                (If OSM_SOURCE unset, fetches OSM first via fetch-osm, then runs full pipeline.)"
	@echo "Airflow:        make airflow-trigger  (same DATE/SAMPLE_SIZE); airflow-unpause; airflow-list-dags"
	@echo "Convenience:    make pull-and-trigger  (pull then trigger DAG)"
	@echo ""
	@echo "Defaults: DATE=2026-01-21  SAMPLE_SIZE=10000  DATA_DIR=."

PYTHON    ?= python
PYTEST    := $(PYTHON) -m pytest
RUN_LOCAL := PYTHONPATH=src $(PYTHON) -m pipelines.run_local

# Data pull defaults (override via: make pull-data DATE=2026-01-21 SAMPLE_SIZE=5000)
# Overture release is YYYY-MM-DD.0; plain DATE gets .0 appended. List with: aws s3 ls s3://overturemaps-us-west-2/release/ --no-sign-request
DATE        ?= 2026-01-21
SAMPLE_SIZE ?= 10000
DATA_DIR    ?= .
OSM_SOURCE  ?=
# OSM mini-region bbox (south,west,north,east). Used when OSM_SOURCE is not set; fetch script writes to $(DATA_DIR)/data/raw/osm/mini_region.parquet
OSM_BBOX    ?= 37.2,-122.52,37.82,-122.35

# ------------------------------------------------------------------------------
# Tests (key groups; run all with 'test')
# ------------------------------------------------------------------------------

.PHONY: test test-config test-data test-pipelines test-io test-scripts
test: test-config test-data test-pipelines test-io test-scripts
	@echo "All test groups passed."

test-config:
	$(PYTEST) tests/config/ -v

test-data:
	$(PYTEST) tests/data/ -v

test-pipelines:
	$(PYTEST) tests/pipelines/ -v

test-io:
	$(PYTEST) tests/io/ -v

test-scripts:
	$(PYTEST) tests/scripts/ -v

# ------------------------------------------------------------------------------
# Local data: separate download/extract steps and full pipeline
# Overture: no separate "extraction" — source is already structured; we only sample by bbox+limit and write Parquet.
# OSM: download (Overpass -> mini_region.parquet) then extract (normalize to POI schema -> osm_pois.parquet).
# ------------------------------------------------------------------------------

.PHONY: pull-data pull-overture fetch-osm extract-osm build-silver build-gold

# Sample Overture Places only (reads from S3 or --overture-source, writes data/raw/overture/temp/overture_sample.parquet).
pull-overture:
	@echo "Pulling Overture sample only..."
	$(RUN_LOCAL) --date "$(DATE)" --sample-size $(SAMPLE_SIZE) --data-dir "$(DATA_DIR)" --only overture_sample

# Download OSM only via Overpass (writes data/raw/osm/mini_region.parquet).
fetch-osm:
	@echo "Fetching OSM mini-region (bbox $(OSM_BBOX))..."
	$(PYTHON) scripts/fetch_osm_mini_region.py --bbox "$(OSM_BBOX)" -o "$(DATA_DIR)/data/raw/osm/mini_region.parquet"

# Run OSM extraction only (reads mini_region.parquet or OSM_SOURCE, writes data/raw/osm/temp/osm_pois.parquet).
extract-osm:
	@echo "Running OSM extraction only..."
	$(RUN_LOCAL) --date "$(DATE)" --data-dir "$(DATA_DIR)" --only osm_extract \
		$(if $(OSM_SOURCE),--osm-source "$(OSM_SOURCE)",)

# Build silver only (conflate overture_sample + osm_pois -> data/silver/venues.*). Requires raw temp files.
build-silver:
	@echo "Building silver only..."
	$(RUN_LOCAL) --date "$(DATE)" --data-dir "$(DATA_DIR)" --only build_silver

# Build gold only (silver -> data/gold/venues.*). Requires silver to exist.
build-gold:
	@echo "Building gold only..."
	$(RUN_LOCAL) --date "$(DATE)" --data-dir "$(DATA_DIR)" --only build_gold

# Full pipeline: optionally fetch OSM if OSM_SOURCE unset, then Overture sample + OSM extract + silver + gold.
pull-data:
	@if [ -z "$(OSM_SOURCE)" ]; then $(MAKE) fetch-osm; fi
	@echo "Running full pipeline (Overture + OSM extract → silver → gold)..."
	$(RUN_LOCAL) --date "$(DATE)" --sample-size $(SAMPLE_SIZE) --data-dir "$(DATA_DIR)" \
		$(if $(OSM_SOURCE),--osm-source "$(OSM_SOURCE)",)

# ------------------------------------------------------------------------------
# Airflow CLI wrappers (for the newly pulled data / same params)
# Set RPG_OVERTURE_RELEASE_DATE and RPG_OVERTURE_SAMPLE_LIMIT in the environment
# where your Airflow workers run so the DAG run uses this date and sample size.
# ------------------------------------------------------------------------------

.PHONY: airflow-trigger airflow-list-dags airflow-unpause airflow-dag-state
airflow-trigger:
	RPG_OVERTURE_RELEASE_DATE="$(DATE)" RPG_OVERTURE_SAMPLE_LIMIT="$(SAMPLE_SIZE)" \
	$(if $(OSM_SOURCE),RPG_OSM_EXTRACT_URI="$(OSM_SOURCE)",) \
	airflow dags trigger rpg_data_foundation

airflow-list-dags:
	airflow dags list

airflow-unpause:
	airflow dags unpause rpg_data_foundation

# Show state of the latest run (optional; requires dag_id and run_id or use list)
airflow-dag-state:
	@echo "Use: airflow dags list-runs -d rpg_data_foundation"

# ------------------------------------------------------------------------------
# Convenience: pull data then trigger DAG (same DATE/SAMPLE_SIZE)
# ------------------------------------------------------------------------------

.PHONY: pull-and-trigger
pull-and-trigger: pull-data airflow-trigger
