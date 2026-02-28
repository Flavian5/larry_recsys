# Data foundation pipeline: tests, local data pull, and Airflow wrappers.
# Run from repo root. Uses PYTHONPATH=src so no need to pip install for tests/run_local.

.DEFAULT_GOAL := help
.PHONY: help

help:
	@echo "Data foundation Makefile"
	@echo ""
	@echo "Tests:          make test  (or test-config, test-data, test-pipelines, test-io, test-scripts)"
	@echo "Data (separate): make pull-overture   - sample Overture only (S3 -> data/raw/overture/temp)"
	@echo "                 make pull-osm       - fetch OSM (Overpass) + extract (-> data/raw/osm/temp/osm_pois); bbox from .env RPG_BBOX"
	@echo "                 make build-silver  - conflate raw -> silver only; make build-gold - silver -> gold only"
	@echo "Data (full):    make pull-data [DATE=...] [SAMPLE_SIZE=10000] [DATA_DIR=.] [OSM_SOURCE=path]"
	@echo "                (If OSM_SOURCE unset, runs pull-osm first, then full pipeline.)"
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
# Bbox for Overture sample and OSM fetch: set RPG_BBOX in .env (south,west,north,east). Same for both sources.

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
# Local data: separate pull steps and full pipeline
# Overture: sample by bbox+limit and write Parquet. OSM: fetch (Overpass) + extract (-> osm_pois) in one step.
# ------------------------------------------------------------------------------

.PHONY: pull-data pull-overture pull-osm build-silver build-gold

# Sample Overture Places only (reads from S3 or --overture-source, writes data/raw/overture/temp/overture_sample.parquet).
pull-overture:
	@echo "Pulling Overture sample only..."
	$(RUN_LOCAL) --date "$(DATE)" --sample-size $(SAMPLE_SIZE) --data-dir "$(DATA_DIR)" --only overture_sample

# Fetch OSM via Overpass (bbox from .env RPG_BBOX) then extract to osm_pois.parquet. Override: RPG_BBOX=s,w,n,e make pull-osm
pull-osm:
	@echo "Pulling OSM (fetch + extract)..."
	$(RUN_LOCAL) --date "$(DATE)" --data-dir "$(DATA_DIR)" --only fetch_osm,osm_extract

# Build silver only (conflate overture_sample + osm_pois -> data/silver/venues.*). Requires raw temp files.
build-silver:
	@echo "Building silver only..."
	$(RUN_LOCAL) --date "$(DATE)" --data-dir "$(DATA_DIR)" --only build_silver

# Build gold only (silver -> data/gold/venues.*). Requires silver to exist.
build-gold:
	@echo "Building gold only..."
	$(RUN_LOCAL) --date "$(DATE)" --data-dir "$(DATA_DIR)" --only build_gold

# Full pipeline: run pull-osm if OSM_SOURCE unset, then Overture sample + OSM extract + silver + gold.
pull-data:
	@if [ -z "$(OSM_SOURCE)" ]; then $(MAKE) pull-osm; fi
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
# Benchmarks: conflation speed testing
# ------------------------------------------------------------------------------

.PHONY: benchmark-conflation
benchmark-conflation:
	@echo "Running conflation benchmarks (10K -> 100K -> 250K)..."
	PYTHONPATH=src $(PYTHON) -m benchmarks.conflation_speed --sample-sizes 10000,100000,250000 --date "$(DATE)" --data-dir "$(DATA_DIR)"

# ------------------------------------------------------------------------------
# Convenience: pull data then trigger DAG (same DATE/SAMPLE_SIZE)
# ------------------------------------------------------------------------------

.PHONY: pull-and-trigger
pull-and-trigger: pull-data airflow-trigger
