#!/usr/bin/env python3
"""Validate Phase 1 Definition of Done (DoD) gates.

Runs pipeline, parses telemetry, and validates output artifacts:
- phase1_player_features.csv schema/content
- row count vs FPL bootstrap-static players
- override coverage threshold
- element-summary error count
- understat team mapping sanity
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.request import Request, urlopen

FPL_BOOTSTRAP_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"
EXPECTED_COLUMNS = [
    "player",
    "team",
    "position",
    "price",
    "minutes_avg",
    "goals",
    "assists",
    "xg90",
    "xa90",
    "next_opponent",
]


def run_cmd(cmd: List[str]) -> Tuple[int, str]:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)
    return proc.returncode, proc.stdout


def fetch_fpl_player_count() -> int:
    req = Request(
        FPL_BOOTSTRAP_URL,
        headers={
            "User-Agent": "Fantasy-Suggestor-DoD-Check/1.0",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    elements = payload.get("elements", [])
    if not isinstance(elements, list):
        return 0
    return len(elements)


def parse_pipeline_stats(output: str) -> Tuple[Optional[int], Optional[float]]:
    fetch_match = re.search(
        r"element-summary fetch stats:\s*api=(\d+),\s*cache=(\d+),\s*errors=(\d+)",
        output,
    )
    override_match = re.search(
        r"override stats:\s*matched=(\d+),\s*unmatched=(\d+),\s*coverage=([0-9]+(?:\.[0-9]+)?)%",
        output,
    )

    errors = int(fetch_match.group(3)) if fetch_match else None
    coverage = float(override_match.group(3)) if override_match else None
    return errors, coverage


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != EXPECTED_COLUMNS:
            raise ValueError(
                f"Schema mismatch. expected={EXPECTED_COLUMNS}, got={reader.fieldnames}"
            )
        return list(reader)


def count_empty(rows: List[Dict[str, str]], columns: List[str]) -> Dict[str, int]:
    return {
        col: sum(1 for row in rows if (row.get(col) or "").strip() == "")
        for col in columns
    }


def count_understat_unresolved_teams(path: Path) -> int:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return sum(1 for row in reader if len((row.get("team") or "").strip()) > 3)


def main() -> int:
    parser = argparse.ArgumentParser(description="Check Phase 1 DoD criteria")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/phase1_player_features.csv"),
        help="Phase 1 output CSV path",
    )
    parser.add_argument(
        "--understat-csv",
        type=Path,
        default=Path("data/understat_xgxa.csv"),
        help="Understat override CSV path",
    )
    parser.add_argument(
        "--min-override-coverage",
        type=float,
        default=40.0,
        help="Minimum override coverage percent gate",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=2025,
        help="Understat season start year used when --refresh-understat is enabled",
    )
    parser.add_argument(
        "--refresh-understat",
        action="store_true",
        help="Refresh Understat CSV before checking",
    )
    args = parser.parse_args()

    checks: List[Tuple[str, bool, str]] = []

    if args.refresh_understat:
        fetch_cmd = [
            "python3",
            "data_pipeline/fetch_understat_xgxa.py",
            "--season",
            str(args.season),
            "--output",
            str(args.understat_csv),
        ]
        code, out = run_cmd(fetch_cmd)
        checks.append(("Understat fetch command exits 0", code == 0, out.strip()))
        if code != 0:
            print_report(checks)
            return 1

    pipeline_cmd = [
        "python3",
        "data_pipeline/phase1_pipeline.py",
        "--understat-csv",
        str(args.understat_csv),
    ]
    code, pipeline_out = run_cmd(pipeline_cmd)
    checks.append(("Pipeline command exits 0", code == 0, pipeline_out.strip()))

    errors, coverage = parse_pipeline_stats(pipeline_out)
    checks.append(
        (
            "Pipeline prints fetch stats",
            errors is not None,
            f"errors={errors}" if errors is not None else "missing fetch stats log",
        )
    )
    checks.append(
        (
            "Pipeline prints override stats",
            coverage is not None,
            f"coverage={coverage}%" if coverage is not None else "missing override stats log",
        )
    )
    checks.append(
        (
            "element-summary errors == 0",
            errors == 0,
            f"errors={errors}",
        )
    )
    checks.append(
        (
            f"override coverage >= {args.min_override_coverage:.1f}%",
            coverage is not None and coverage >= args.min_override_coverage,
            f"coverage={coverage}%",
        )
    )

    checks.append(("Output CSV exists", args.output.exists(), str(args.output)))
    if not args.output.exists():
        print_report(checks)
        return 1

    try:
        rows = read_csv_rows(args.output)
        checks.append(("Output CSV schema matches expected columns", True, ",".join(EXPECTED_COLUMNS)))
    except Exception as exc:  # noqa: BLE001
        checks.append(("Output CSV schema matches expected columns", False, str(exc)))
        print_report(checks)
        return 1

    try:
        fpl_player_count = fetch_fpl_player_count()
    except Exception as exc:  # noqa: BLE001
        checks.append(("FPL bootstrap player count fetched", False, str(exc)))
        print_report(checks)
        return 1

    checks.append(
        (
            "Output row count equals FPL players count",
            len(rows) == fpl_player_count,
            f"rows={len(rows)}, fpl_players={fpl_player_count}",
        )
    )

    empties = count_empty(rows, EXPECTED_COLUMNS)
    total_empty = sum(empties.values())
    checks.append(
        (
            "No empty values in required columns",
            total_empty == 0,
            ", ".join(f"{k}={v}" for k, v in empties.items()),
        )
    )

    tbd_count = sum(1 for row in rows if row.get("next_opponent") == "TBD")
    checks.append(("next_opponent has no TBD", tbd_count == 0, f"tbd={tbd_count}"))

    checks.append(("Understat CSV exists", args.understat_csv.exists(), str(args.understat_csv)))
    if args.understat_csv.exists():
        unresolved = count_understat_unresolved_teams(args.understat_csv)
        checks.append(
            (
                "Understat team mapping resolved (team code length <= 3)",
                unresolved == 0,
                f"unresolved={unresolved}",
            )
        )

    print_report(checks)
    return 0 if all(result for _, result, _ in checks) else 1


def print_report(checks: List[Tuple[str, bool, str]]) -> None:
    print("Phase 1 DoD Report")
    print("=" * 60)
    for name, ok, detail in checks:
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {name}")
        if detail:
            print(f"       {detail}")


if __name__ == "__main__":
    raise SystemExit(main())
