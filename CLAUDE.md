# CLAUDE.md — Blue Jays Moneyball ETL

## Brand Hub
**onepercentbetter.poker** — this project is listed there as `Blue Jays Moneyball ETL`.
If it's removed from the site, it's no longer a brand asset.
Owner: Chris S. Yoon · github.com/sukminc

## What this is
Production-grade ELT pipeline. Integrates Spotrac payroll data vs. MLB stats.
Status: `live` (100% MVP)
Slug on hub: `bluejays-moneyball` · Repo: `sukminc/bluejays-financial-mlops`

## Core Value
Proof-of-skill in data engineering: self-validating pipelines, CI/CD-native DQ gates,
Airflow orchestration. Same "find the edge" philosophy applied to sports analytics.

## Stack
- Apache Airflow (orchestration)
- PostgreSQL (warehouse)
- Python + Pandas (transforms)
- Docker (local + CI parity)
- GitHub Actions (CI/CD)

## Key Architecture
- DQ gates block all downstream transforms until checks pass — no silent failures
- Regression DAG fires known-bad datasets against DQ gates, asserting failure
- Guardrails are CI/CD citizens: if the safety net breaks, the build breaks

## Commands
```bash
docker-compose up -d          # Start Airflow + Postgres
# Access Airflow UI: localhost:8080
```
