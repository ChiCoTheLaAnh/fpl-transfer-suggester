#!/usr/bin/env python3
"""Validate Phase 2 DoD gates for ranking MVP."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
PHASE2_SCRIPT = REPO_ROOT / "data_pipeline/phase2_rank_players.py"
DEFAULT_INPUT = REPO_ROOT / "data/phase1_player_features.csv"
DEFAULT_OUTPUT_CSV = REPO_ROOT / "data/phase2_ranked_players.csv"
DEFAULT_OUTPUT_JSON = REPO_ROOT / "data/phase2_ranked_players.meta.json"

EXPECTED_CSV_COLUMNS = [
    "player",
    "team",
    "position",
    "price",
    "next_opponent",
    "minutes_avg",
    "xg90",
    "xa90",
    "attack_raw",
    "value_raw",
    "fixture_home",
    "score",
    "rank_position",
]
EXPECTED_JSON_TOP_LEVEL_KEYS = {
    "schema_version",
    "input_file",
    "filters",
    "weights",
    "counts",
}
EXPECTED_JSON_FILTER_KEYS = {"positions", "min_minutes_avg", "top_n_per_position"}
EXPECTED_JSON_WEIGHT_KEYS = {"w_attack", "w_minutes", "w_value", "w_fixture"}
EXPECTED_JSON_COUNT_KEYS = {"input_rows", "eligible_rows", "output_rows", "rows_by_position"}


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
    print("Phase 2 DoD Report")
    print("=" * 60)
    for name, ok, detail in checks:
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {name}")
        if detail:
            print(f"       {detail}")


def integration_sort_key(row: Dict[str, str]) -> Tuple[float, float, float, str, str]:
    return (
        -float(row["score"]),
        -float(row["value_raw"]),
        float(row["price"]),
        row["player"],
        row["team"],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Check Phase 2 DoD criteria")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Phase 1 input CSV")
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV, help="Phase 2 output CSV")
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON, help="Phase 2 metadata JSON")
    parser.add_argument("--top-n-per-position", type=int, default=20, help="Expected top N per position")
    parser.add_argument(
        "--positions",
        type=str,
        default="DEF,MID,FWD",
        help="Expected comma-separated output positions",
    )
    parser.add_argument(
        "--min-minutes-avg",
        type=float,
        default=30.0,
        help="Minimum minutes_avg to pass into phase2 script",
    )
    args = parser.parse_args()

    positions = [item.strip().upper() for item in args.positions.split(",") if item.strip()]
    checks: List[Tuple[str, bool, str]] = []

    # Unit-like checks.
    from phase2_rank_players import (  # noqa: PLC0415
        compute_weighted_score,
        minmax_normalize,
        parse_fixture_home,
    )

    checks.append(("parse_fixture_home handles (H)", parse_fixture_home("ARS (H)") == 1, "ARS (H) -> 1"))
    checks.append(("parse_fixture_home handles (A)", parse_fixture_home("ARS (A)") == 0, "ARS (A) -> 0"))
    checks.append(("parse_fixture_home handles unknown", parse_fixture_home("TBD") == 0, "TBD -> 0"))

    norm_values = minmax_normalize([2.0, 2.0, 2.0])
    checks.append(
        (
            "minmax_normalize max==min => 0.5",
            norm_values == [0.5, 0.5, 0.5],
            f"result={norm_values}",
        )
    )

    score = compute_weighted_score(
        attack_norm=0.8,
        minutes_norm=0.5,
        value_norm=0.6,
        fixture_norm=1.0,
        w_attack=0.50,
        w_minutes=0.25,
        w_value=0.20,
        w_fixture=0.05,
    )
    checks.append(
        (
            "weighted score calc sample",
            abs(score - 0.695) < 1e-9,
            f"score={score}",
        )
    )

    # Run phase 2 once (integration).
    base_cmd = [
        sys.executable,
        str(PHASE2_SCRIPT),
        "--input",
        str(args.input),
        "--output-csv",
        str(args.output_csv),
        "--output-json",
        str(args.output_json),
        "--top-n-per-position",
        str(args.top_n_per_position),
        "--min-minutes-avg",
        str(args.min_minutes_avg),
        "--positions",
        ",".join(positions),
    ]

    code, out = run_cmd(base_cmd)
    checks.append(("phase2_rank_players exits 0", code == 0, out.strip()))
    if code != 0:
        print_report(checks)
        return 1

    checks.append(("Output CSV exists", args.output_csv.exists(), str(args.output_csv)))
    checks.append(("Output JSON exists", args.output_json.exists(), str(args.output_json)))
    if not args.output_csv.exists() or not args.output_json.exists():
        print_report(checks)
        return 1

    with args.output_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)
    checks.append(
        (
            "CSV schema exact-match",
            fieldnames == EXPECTED_CSV_COLUMNS,
            f"columns={fieldnames}",
        )
    )

    with args.output_json.open("r", encoding="utf-8") as f:
        meta: Dict[str, Any] = json.load(f)

    checks.append(
        (
            "JSON top-level schema exact-match",
            set(meta.keys()) == EXPECTED_JSON_TOP_LEVEL_KEYS,
            f"keys={sorted(meta.keys())}",
        )
    )
    filters = meta.get("filters", {})
    weights = meta.get("weights", {})
    counts = meta.get("counts", {})
    checks.append(
        (
            "JSON filters schema exact-match",
            set(filters.keys()) == EXPECTED_JSON_FILTER_KEYS,
            f"keys={sorted(filters.keys()) if isinstance(filters, dict) else 'invalid'}",
        )
    )
    checks.append(
        (
            "JSON weights schema exact-match",
            set(weights.keys()) == EXPECTED_JSON_WEIGHT_KEYS,
            f"keys={sorted(weights.keys()) if isinstance(weights, dict) else 'invalid'}",
        )
    )
    checks.append(
        (
            "JSON counts schema exact-match",
            set(counts.keys()) == EXPECTED_JSON_COUNT_KEYS,
            f"keys={sorted(counts.keys()) if isinstance(counts, dict) else 'invalid'}",
        )
    )

    no_gkp = all(row.get("position") != "GKP" for row in rows)
    checks.append(("No GKP in output", no_gkp, f"rows={len(rows)}"))

    rows_by_position: Dict[str, List[Dict[str, str]]] = {pos: [] for pos in positions}
    unexpected_positions = set()
    for row in rows:
        pos = row.get("position", "")
        if pos in rows_by_position:
            rows_by_position[pos].append(row)
        else:
            unexpected_positions.add(pos)

    checks.append(
        (
            "Output positions are within requested set",
            len(unexpected_positions) == 0,
            f"unexpected={sorted(unexpected_positions)}",
        )
    )

    top_n_ok = all(len(group) <= max(0, args.top_n_per_position) for group in rows_by_position.values())
    checks.append(
        (
            f"Top N per position <= {args.top_n_per_position}",
            top_n_ok,
            ", ".join(f"{pos}={len(rows_by_position[pos])}" for pos in positions),
        )
    )

    ranks_ok = True
    rank_detail: List[str] = []
    for pos in positions:
        group = rows_by_position[pos]
        ranks = [int(row["rank_position"]) for row in group]
        expected = list(range(1, len(group) + 1))
        ok = ranks == expected
        ranks_ok = ranks_ok and ok
        rank_detail.append(f"{pos}={'ok' if ok else 'bad'}")
    checks.append(("rank_position is continuous from 1", ranks_ok, ", ".join(rank_detail)))

    sorted_ok = True
    sort_detail: List[str] = []
    for pos in positions:
        group = rows_by_position[pos]
        expected_sorted = sorted(group, key=integration_sort_key)
        ok = group == expected_sorted
        sorted_ok = sorted_ok and ok
        sort_detail.append(f"{pos}={'ok' if ok else 'bad'}")
    checks.append(("Rows sorted by score/value/price/player", sorted_ok, ", ".join(sort_detail)))

    # Deterministic gate.
    hash_csv_1 = hash_file(args.output_csv)
    hash_json_1 = hash_file(args.output_json)

    code2, out2 = run_cmd(base_cmd)
    checks.append(("phase2_rank_players rerun exits 0", code2 == 0, out2.strip()))
    if code2 == 0:
        hash_csv_2 = hash_file(args.output_csv)
        hash_json_2 = hash_file(args.output_json)
        checks.append(
            (
                "Deterministic CSV SHA256",
                hash_csv_1 == hash_csv_2,
                f"first={hash_csv_1}, second={hash_csv_2}",
            )
        )
        checks.append(
            (
                "Deterministic JSON SHA256",
                hash_json_1 == hash_json_2,
                f"first={hash_json_1}, second={hash_json_2}",
            )
        )

    print_report(checks)
    return 0 if all(ok for _, ok, _ in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
