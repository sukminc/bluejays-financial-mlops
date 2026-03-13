# Blue Jays Moneyball ETL

Production-style Airflow and PostgreSQL pipeline built around one principle: if the data is not trustworthy, the pipeline should stop.

This project ingests Blue Jays salary data into a staging layer, applies SQL-based data quality checks, and only materializes the fact table after the guardrails pass. It also includes a regression DAG that intentionally feeds bad data into the system to verify that the checks still fail when they should.

## Why This Project Matters

This is the strongest data-engineering repo in my portfolio because it shows more than ingestion:

- raw-to-staging-to-fact flow
- explicit data quality gates
- fail-fast behavior
- regression validation for the guardrails themselves

It is less about baseball specifically and more about how I think about production pipelines: reliability first, quiet failure never.

## Architecture

### Production pipeline

`bluejays_v2_simple_pipeline`

1. Load raw CSV into a text-first staging table
2. Run SQL DQ checks against the staging table
3. Stop immediately if checks fail
4. Transform clean rows into the fact table

### Regression suite

`bluejays_regression_suite`

1. Load a clean dataset and assert the DQ gate passes
2. Load a bad dataset and assert the DQ gate fails

That second step is the key design choice. The project does not just have data quality rules; it proves they still work.

## Core Design Decisions

### 1. Text-first ingestion

Raw salary data is loaded as text first.

Why:

- source systems are messy
- currency formatting and stray values break typed ingestion
- losing raw source data early is expensive

So the sequence is:

`load as text -> validate -> clean -> cast`

### 2. SQL-based DQ gate

Checks live under [src/dq/sql](/Users/chrisyoon/GitHub/bluejays-financial-mlops/src/dq/sql) and include:

- null checks
- duplicate checks
- invalid type / format checks

The gate is executed by [src/dq/checks.py](/Users/chrisyoon/GitHub/bluejays-financial-mlops/src/dq/checks.py) and fails the pipeline if bad rows are detected.

### 3. Regression testing for data quality logic

The Airflow regression suite intentionally loads `spotrac_bad_data.csv` and expects the DQ step to fail.

That means the project verifies not only the happy path, but also whether the protection layer is still alive.

## Repo Structure

```text
bluejays-financial-mlops/
├── dags/
│   ├── bluejays_simple_dag.py
│   └── bluejays_regression_suite.py
├── src/
│   ├── db/
│   ├── dq/
│   │   ├── checks.py
│   │   └── sql/
│   └── load/
├── data/
│   └── raw/manual/
├── docker-compose.yaml
└── requirements.txt
```

## Important Files

- [dags/bluejays_simple_dag.py](/Users/chrisyoon/GitHub/bluejays-financial-mlops/dags/bluejays_simple_dag.py)
  Production flow: ingest -> DQ -> transform.
- [dags/bluejays_regression_suite.py](/Users/chrisyoon/GitHub/bluejays-financial-mlops/dags/bluejays_regression_suite.py)
  Regression flow for clean and bad datasets.
- [src/dq/checks.py](/Users/chrisyoon/GitHub/bluejays-financial-mlops/src/dq/checks.py)
  Gate execution logic and pass/fail behavior.
- [src/load/ingest_raw.py](/Users/chrisyoon/GitHub/bluejays-financial-mlops/src/load/ingest_raw.py)
  Raw ingestion into staging.
- [src/load/transform_fact_salary.py](/Users/chrisyoon/GitHub/bluejays-financial-mlops/src/load/transform_fact_salary.py)
  Post-validation transformation into the fact layer.

## Stack

- Python
- Apache Airflow
- PostgreSQL
- SQLAlchemy
- Docker Compose

## Running Locally

```bash
docker-compose build
docker-compose up -d
```

Airflow UI:

- `http://localhost:8080`
- username: `airflow`
- password: `airflow`

Run:

- `bluejays_v2_simple_pipeline`
- `bluejays_regression_suite`

## Current Status

This repo is in strong portfolio shape.

What it demonstrates well:

- Airflow orchestration
- warehouse-minded staging and transformation
- data quality as a gate, not a dashboard
- regression thinking applied to data engineering

## Hiring Signal

If you want to know how I think as a data engineer, this is one of the clearest repos to read.
