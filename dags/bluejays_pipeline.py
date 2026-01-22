"""Compatibility DAG entrypoint.

This file keeps legacy DAG filename references stable while delegating to the
canonical DAG implementation in `dags/bluejays_pipeline.py`.
"""

from dags.bluejays_pipeline import dag  # noqa: F401
