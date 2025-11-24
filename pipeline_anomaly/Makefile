POETRY := $(shell which poetry)
PYTHON=$(POETRY) run python
AIRFLOW_HOME ?= $(CURDIR)/.airflow
AIRFLOW_EXECUTION ?= $(shell date -u +"%Y-%m-%dT%H:%M:%S")

.PHONY: install infra-up infra-down pipeline pipeline-% airflow-dag-test test format lint

install:
	$(POETRY) install --no-root

infra-up:
	docker compose up -d

infra-down:
	docker compose down -v

pipeline:
	$(PYTHON) -m pipeline_anomaly.presentation.cli --config config/pipeline.yaml

pipeline-%:
	$(PYTHON) -m pipeline_anomaly.presentation.cli --config config/$*.yaml

airflow-dag-test:
	@if ! command -v airflow >/dev/null 2>&1; then \
		echo "Airflow CLI not found. Install apache-airflow to run DAG tests."; \
		exit 1; \
	fi
	AIRFLOW_HOME=$(AIRFLOW_HOME) PIPELINE_CONFIG=$(CURDIR)/config/pipeline.yaml airflow dags test pipeline_anomaly $(AIRFLOW_EXECUTION)

test:
	$(POETRY) run pytest
