
"""Spotrac payroll extractor (player-level).

Purpose
- Extract the *player-level* payroll rows from Spotrac
  (not CBT tier summaries).
- Persist a reproducible raw JSON artifact under
  /opt/airflow/data/raw/spotrac/.

Output JSON schema:
{
  "source": {
    "name": "spotrac",
    "url": "...",
    "fetched_at_utc": "...",
    "season": 2025,
    "run_id": "manual__..."
  },
  "schema": {
    "record_type": "spotrac_payroll_player",
    "version": 1
  },
  "records": [
    {
      "player": "George Springer",
      "salary_text": "$24,166,666",
      "salary": 24166666,
      "season": 2025,
      "snapshot_date": "2026-01-22"
    }
  ]
}

Notes
- This is intentionally simple and portfolio-friendly.
- Mapping Spotrac player names -> MLBAM player_id is a later step.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


@dataclass(frozen=True)
class SourceMeta:
    name: str
    url: str
    fetched_at_utc: str
    season: int
    run_id: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _today_iso() -> str:
    return date.today().isoformat()


def _parse_money(value: str) -> int:
    """Convert currency-like strings to int dollars.

    Examples:
    - "$24,166,666" -> 24166666
    - "24,166,666" -> 24166666
    - "--" / "" -> 0
    """
    if value is None:
        return 0
    raw = str(value).strip()
    if raw in {"", "-", "--", "N/A"}:
        return 0
    cleaned = re.sub(r"[^0-9]", "", raw)
    if cleaned == "":
        return 0
    return int(cleaned)


def _pick_salary_cell(headers: list[str], cells: list[str]) -> str:
    """Choose the most appropriate 'salary' cell from a row.

    Spotrac tables can change, so we prefer a header match first.
    Fallback: use the last numeric-looking cell.
    """
    header_to_idx: dict[str, int] = {}
    for i, h in enumerate(headers):
        header_to_idx[h.lower()] = i

    candidates = [
        "luxury tax salary",
        "tax salary",
        "luxury",
        "salary",
        "base salary",
        "aav",
        "avg/yr",
        "average",
    ]

    for cand in candidates:
        for h, idx in header_to_idx.items():
            if cand in h and idx < len(cells):
                return cells[idx]

    # Fallback: last cell that looks like money
    for v in reversed(cells):
        if re.search(r"\$|\d", v or ""):
            return v

    return ""


def _scrape_table(page) -> list[dict[str, Any]]:
    """Scrape payroll table rows from Spotrac."""
    page.wait_for_selector("table.datatable", timeout=20000)

    # Extract headers + row cells in a resilient way.
    payload = page.evaluate(
        """() => {
        const table = document.querySelector('table.datatable');
        if (!table) return { headers: [], rows: [] };

        const headers = Array.from(
          table.querySelectorAll('thead th')
        ).map(th => th.innerText.trim());

        const rows = Array.from(table.querySelectorAll('tbody tr')).map(tr => {
          const tds = Array.from(tr.querySelectorAll('td'));
          const cells = tds.map(td => td.innerText.trim());
          return { cells };
        });

        return { headers, rows };
      }"""
    )

    headers = payload.get("headers") or []
    rows = payload.get("rows") or []

    out: list[dict[str, Any]] = []
    for r in rows:
        cells = r.get("cells") or []
        if len(cells) < 2:
            continue

        player = (cells[0] or "").strip()
        if not player:
            continue

        salary_text = _pick_salary_cell(headers, cells).strip()
        out.append({"player": player, "salary_text": salary_text})

    return out


def fetch_spotrac_payroll(
    season: int,
    run_id: str | None = None,
    limit: int | None = None,
) -> str:
    """Scrape Blue Jays player payroll rows from Spotrac.

    Returns:
        Absolute path to the saved JSON artifact inside the container.
    """
    url = "https://www.spotrac.com/mlb/toronto-blue-jays/payroll/"
    run_id = run_id or os.environ.get("AIRFLOW_CTX_DAG_RUN_ID") or "manual"

    out_dir = Path("/opt/airflow/data/raw/spotrac")
    out_dir.mkdir(parents=True, exist_ok=True)

    fetched_at_utc = _utc_now_iso()
    snapshot_date = _today_iso()

    # Make filename deterministic-ish but unique per run.
    token = uuid.uuid4().hex[:8]
    filename = (
        f"spotrac_payroll_players_{season}_"
        f"{snapshot_date}_{run_id.replace(':', '_').replace('/', '_')}_"
        f"{token}.json"
    )
    output_path = out_dir / filename

    print(f"Connecting to {url} ...")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-gpu",
            ],
        )

        user_agent = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        context = browser.new_context(user_agent=user_agent)
        page = context.new_page()

        try:
            page.goto(url, timeout=90000)
            rows = _scrape_table(page)
        except PlaywrightTimeoutError as exc:
            raise RuntimeError(
                "Timeout while loading Spotrac payroll page"
            ) from exc
        except Exception:
            raise
        finally:
            browser.close()

    if limit is not None:
        rows = rows[: int(limit)]

    records: list[dict[str, Any]] = []
    for r in rows:
        salary_text = r.get("salary_text") or ""
        records.append(
            {
                "player": r.get("player"),
                "salary_text": salary_text,
                "salary": _parse_money(salary_text),
                "season": int(season),
                "snapshot_date": snapshot_date,
            }
        )

    doc = {
        "source": SourceMeta(
            name="spotrac",
            url=url,
            fetched_at_utc=fetched_at_utc,
            season=int(season),
            run_id=run_id,
        ).__dict__,
        "schema": {"record_type": "spotrac_payroll_player", "version": 1},
        "records": records,
    }

    output_path.write_text(
        json.dumps(doc, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(
        "spotrac payroll extract complete. "
        f"output={output_path} "
        f"records={len(records)}"
    )

    # Fail fast: avoid "green but empty" extractions.
    if len(records) == 0:
        raise RuntimeError("No payroll rows extracted from Spotrac")

    return str(output_path)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Extract Spotrac player payroll rows (Blue Jays) "
            "and write raw JSON artifact."
        )
    )
    parser.add_argument(
        "--season",
        type=int,
        default=int(os.environ.get("SEASON", "2025")),
        help="Season label to attach to extracted records.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit for quick debugging.",
    )

    args = parser.parse_args()
    fetch_spotrac_payroll(season=args.season, limit=args.limit)


if __name__ == "__main__":
    main()
