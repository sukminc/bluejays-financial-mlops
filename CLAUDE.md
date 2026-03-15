# CLAUDE.md — Blue Jays Moneyball ETL

## Repo Role

Archive proof-of-work repo for data engineering depth.

This repo is not part of the poker hub.
Its job is to stay legible as a strong data-engineering example:

- Airflow orchestration
- PostgreSQL pipeline design
- fail-fast data quality gates
- regression validation of those gates

## Guardrails

- Preserve clarity over expansion.
- Keep the repo useful as hiring proof, not as a new active product line.
- Do not add story layers that distract from the pipeline quality signal.

## Current Truth

Trust `README.md` for current implementation and why it matters.

The strongest signal in this repo is:

- raw -> staging -> fact flow
- DQ gates block downstream work
- regression DAG proves the guardrails still fail when they should

## Commands

```bash
docker-compose up -d
```
