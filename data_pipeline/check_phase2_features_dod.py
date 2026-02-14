#!/usr/bin/env python3
"""Validate DoD for Phase 2 feature engineering (player x GW)."""

from __future__ import annotations

import argparse
import hashlib
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
BUILD_SCRIPT = REPO_ROOT / "data_pipeline/build_feature_table.py"
DEFAULT_DB = REPO_ROOT / "data/fpl_mvp.db"
DEFAULT_OUTPUT = REPO_ROOT / "data/player_gw_features.parquet"
REQUIRED_COLUMNS = [
    "player_id",
    "player_name",
    "team_id",
    "team",
    "position",
    "fixture_id",
    "target_event",
    "kickoff_time",
    "is_home",
    "opponent_team_id",
    "opponent_team",
    "opponent_difficulty",
    "dgw_count_in_event",
    "is_dgw",
    "rest_days",
    "horizon",
    "horizon_fixture_count_team",
    "horizon_avg_fixture_difficulty",
    "horizon_home_count",
    "recent_points_avg_3",
    "recent_points_avg_5",
    "recent_minutes_avg_3",
    "recent_minutes_avg_5",
    "recent_xgi_avg_3",
    "recent_xgi_avg_5",
    "minutes_volatility_5",
    "points_volatility_5",
    "benching_probability",
    "risk_score",
    "last_price",
    "current_status",
]


def run_cmd(cmd: List[str]) -> Tuple[int, str]:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)
    return proc.returncode, proc.stdout


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 64), b""):
            digest.update(chunk)
    return digest.hexdigest()


def print_report(checks: List[Tuple[str, bool, str]]) -> None:
    print("Phase 2 Features DoD Report")
    print("=" * 60)
    for name, ok, detail in checks:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}")
        if detail:
            print(f"       {detail}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Check Phase 2 FE DoD")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="SQLite DB path")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Feature parquet output")
    parser.add_argument("--horizon", type=int, default=3, help="Horizon passed into build script")
    args = parser.parse_args()

    checks: List[Tuple[str, bool, str]] = []

    from build_feature_table import (  # noqa: PLC0415
        compute_benching_probability,
        compute_risk_score,
        rolling_mean,
        rolling_std,
    )

    checks.append(
        ("rolling_mean sample", abs(rolling_mean([1.0, 2.0, 3.0], 2) - 2.5) < 1e-9, "expected=2.5")
    )
    checks.append(
        ("rolling_std zero-window sample", abs(rolling_std([2.0, 2.0, 2.0], 3) - 0.0) < 1e-9, "expected=0")
    )
    checks.append(
        (
            "benching_probability bounded",
            0.0 <= compute_benching_probability([90, 0, 30, 0, 90]) <= 1.0,
            "range check",
        )
    )
    checks.append(
        (
            "risk_score bounded",
            0.0 <= compute_risk_score(benching_probability=0.6, minutes_volatility_5=0.4, injury_flag=1) <= 1.0,
            "range check",
        )
    )

    build_cmd = [
        sys.executable,
        str(BUILD_SCRIPT),
        "--db",
        str(args.db),
        "--output",
        str(args.output),
        "--horizon",
        str(max(1, args.horizon)),
    ]

    code, out = run_cmd(build_cmd)
    checks.append(("build_feature_table exits 0", code == 0, out.strip()))
    if code != 0:
        print_report(checks)
        return 1

    checks.append(("Feature parquet exists", args.output.exists(), str(args.output)))
    if not args.output.exists():
        print_report(checks)
        return 1

    df = pd.read_parquet(args.output)
    checks.append(("Output is non-empty", len(df) > 0, f"rows={len(df)}"))
    checks.append(
        (
            "Schema exact-match",
            list(df.columns) == REQUIRED_COLUMNS,
            f"columns={list(df.columns)}",
        )
    )

    key_nulls = int(df[["player_id", "fixture_id", "target_event", "team_id"]].isna().sum().sum())
    checks.append(("No nulls in key columns", key_nulls == 0, f"key_nulls={key_nulls}"))

    dedup_ok = not df.duplicated(subset=["player_id", "fixture_id"]).any()
    checks.append(("No duplicates on (player_id, fixture_id)", dedup_ok, "unique constraint"))

    dgw_ok = bool(((df["is_dgw"] == (df["dgw_count_in_event"] > 1).astype(int))).all())
    checks.append(("DGW consistency", dgw_ok, "is_dgw == dgw_count_in_event > 1"))

    bench_ok = bool(df["benching_probability"].between(0.0, 1.0, inclusive="both").all())
    risk_ok = bool(df["risk_score"].between(0.0, 1.0, inclusive="both").all())
    checks.append(("benching_probability in [0,1]", bench_ok, "range check"))
    checks.append(("risk_score in [0,1]", risk_ok, "range check"))

    rolling_cols = [
        "recent_points_avg_3",
        "recent_points_avg_5",
        "recent_minutes_avg_3",
        "recent_minutes_avg_5",
        "recent_xgi_avg_3",
        "recent_xgi_avg_5",
        "minutes_volatility_5",
        "points_volatility_5",
    ]
    rolling_nulls = int(df[rolling_cols].isna().sum().sum())
    checks.append(("No nulls in rolling/risk columns", rolling_nulls == 0, f"nulls={rolling_nulls}"))

    horizon_match = bool((df["horizon"] == max(1, args.horizon)).all())
    checks.append(("Horizon column matches input", horizon_match, f"horizon={max(1, args.horizon)}"))

    with sqlite3.connect(str(args.db)) as conn:
        fixture_status = pd.read_sql_query(
            "SELECT id AS fixture_id, finished FROM fixtures",
            conn,
        )
    merged = df[["fixture_id"]].drop_duplicates().merge(fixture_status, on="fixture_id", how="left")
    unfinished_only = bool((merged["finished"].fillna(1).astype(int) == 0).all())
    checks.append(("Targets use unfinished fixtures only", unfinished_only, "finished == 0"))

    hash_1 = hash_file(args.output)
    code2, out2 = run_cmd(build_cmd)
    checks.append(("build_feature_table rerun exits 0", code2 == 0, out2.strip()))
    if code2 == 0:
        hash_2 = hash_file(args.output)
        checks.append(("Deterministic parquet SHA256", hash_1 == hash_2, f"first={hash_1}, second={hash_2}"))

    print_report(checks)
    return 0 if all(ok for _, ok, _ in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
