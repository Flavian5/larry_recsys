# Data foundation pipeline: tests, local data pull, and Airflow wrappers.
# Run from repo root. Uses PYTHONPATH=src so no need to pip install for tests/run_local.

.DEFAULT_GOAL := help
.PHONY: help

help:
	@echo "Data foundation Makefile"
	@echo ""
	@echo "Tests:          make test  (or test-config, test-data, test-pipelines, test-io)"
	@echo "Data pull:      make pull-data [DATE=2024-01-01] [SAMPLE_SIZE=10000] [DATA_DIR=.] [OSM_SOURCE=path]"
	@echo "Airflow:        make airflow-trigger  (same DATE/SAMPLE_SIZE); airflow-unpause; airflow-list-dags"
	@echo "Convenience:    make pull-and-trigger  (pull then trigger DAG)"
	@echo ""
	@echo "Defaults: DATE=2024-01-01  SAMPLE_SIZE=10000  DATA_DIR=."

PYTHON    ?= python
PYTEST    := $(PYTHON) -m pytest
RUN_LOCAL := PYTHONPATH=src $(PYTHON) -m pipelines.run_local

# Data pull defaults (override via: make pull-data DATE=2024-03-01 SAMPLE_SIZE=5000)
DATE        ?= 2024-01-01
SAMPLE_SIZE ?= 10000
DATA_DIR    ?= .
OSM_SOURCE  ?=

# ------------------------------------------------------------------------------
# Tests (key groups; run all with 'test')
# ------------------------------------------------------------------------------

.PHONY: test test-config test-data test-pipelines test-io
test: test-config test-data test-pipelines test-io
	@echo "All test groups passed."

test-config:
	$(PYTEST) tests/config/ -v

test-data:
	$(PYTEST) tests/data/ -v

test-pipelines:
	$(PYTEST) tests/pipelines/ -v

test-io:
	$(PYTEST) tests/io/ -v

# ------------------------------------------------------------------------------
# Local data pull (Overture + OSM → silver → gold in process)
# ------------------------------------------------------------------------------

.PHONY: pull-data
pull-data:
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
