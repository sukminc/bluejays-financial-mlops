import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd

try:
    # Playwright is expected to be installed in the Airflow image.
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except Exception:  # pragma: no cover
    sync_playwright = None  # type: ignore
    PlaywrightTimeoutError = Exception  # type: ignore


DEFAULT_SPOTRAC_URL = "https://www.spotrac.com/mlb/toronto-blue-jays/payroll/"


def _sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize column names for downstream stability and consistent schema.
    df = df.copy()
    df.columns = [
        re.sub(r"\s+", " ", str(c)).strip().lower().replace(" ", "_")
        for c in df.columns
    ]
    return df


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _context_get(
    context: Dict[str, Any],
    key: str,
    default: Any = None,
) -> Any:
    # Airflow passes a large context dict; this helper prevents KeyError noise.
    return context.get(key, default)


def fetch_spotrac_payroll(**context: Any) -> Dict[str, Any]:
    """Extract Spotrac payroll data for the Toronto Blue Jays.

    Design goals:
    - Avoid DAG parse-time imports that can break the whole DAG.
    - Run reliably in an Airflow worker container.
    - Persist a raw snapshot for QA/reconciliation.

    Output:
    - Writes a JSON snapshot under /opt/airflow/data/raw/spotrac
    - Returns lightweight metadata for Airflow logs/XCom.
    """

    if sync_playwright is None:
        raise ImportError(
            "Playwright is not available. Ensure 'playwright' is installed "
            "in the Airflow image and browsers are installed."
        )

    run_id = str(_context_get(context, "run_id", "manual"))
    logical_date = _context_get(context, "logical_date")
    ds = str(_context_get(context, "ds", ""))

    # Prefer logical_date when available (Airflow 2+)
    if isinstance(logical_date, datetime):
        snapshot_ts = logical_date.astimezone(timezone.utc)
    else:
        snapshot_ts = datetime.now(timezone.utc)

    url = os.getenv("SPOTRAC_PAYROLL_URL", DEFAULT_SPOTRAC_URL)
    timeout_ms = int(os.getenv("SPOTRAC_PLAYWRIGHT_TIMEOUT_MS", "45000"))

    raw_base_dir = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data")
    raw_dir = os.path.join(raw_base_dir, "raw", "spotrac")
    _ensure_dir(raw_dir)

    # Use ds (YYYY-MM-DD) when available for deterministic snapshots.
    date_tag = ds if ds else snapshot_ts.strftime("%Y-%m-%d")
    file_name = f"spotrac_payroll_{date_tag}_{run_id}.json".replace(":", "_")
    out_path = os.path.join(raw_dir, file_name)

    # Minimal, stable browser settings for container execution.
    user_agent = os.getenv(
        "SPOTRAC_USER_AGENT",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context_pw = browser.new_context(
            user_agent=user_agent,
            viewport={"width": 1365, "height": 768},
        )
        page = context_pw.new_page()

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            # Spotrac can be slow; wait for network idle when possible.
            page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except PlaywrightTimeoutError:
            # Best-effort: capture whatever HTML is available.
            pass

        html = page.content()
        browser.close()

    # Parse the primary payroll table(s) from HTML.
    tables = pd.read_html(html)
    if not tables:
        raise ValueError(
            "No HTML tables detected on the Spotrac payroll page."
        )

    # Heuristic: choose the widest table as the main payroll table.
    payroll_df = max(tables, key=lambda t: t.shape[1]).copy()
    payroll_df = _sanitize_column_names(payroll_df)

    # Persist a raw snapshot (records plus minimal metadata).
    payload: Dict[str, Any] = {
        "source": {
            "name": "spotrac",
            "url": url,
            "fetched_at_utc": snapshot_ts.isoformat(),
            "run_id": run_id,
            "ds": ds,
        },
        "schema": {"columns": list(payroll_df.columns)},
        "records": payroll_df.to_dict(orient="records"),
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return {
        "status": "ok",
        "url": url,
        "output_path": out_path,
        "row_count": int(len(payroll_df)),
        "column_count": int(payroll_df.shape[1]),
    }
