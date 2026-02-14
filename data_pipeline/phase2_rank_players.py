#!/usr/bin/env python3
"""Phase 2 ranking MVP for outfield players (DEF/MID/FWD)."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = REPO_ROOT / "data/phase1_player_features.csv"
DEFAULT_OUTPUT_CSV = REPO_ROOT / "data/phase2_ranked_players.csv"
DEFAULT_OUTPUT_JSON = REPO_ROOT / "data/phase2_ranked_players.meta.json"

INPUT_REQUIRED_COLUMNS = {
    "player",
    "team",
    "position",
    "price",
    "next_opponent",
    "minutes_avg",
    "xg90",
    "xa90",
}
OUTPUT_COLUMNS = [
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


@dataclass
class Candidate:
    player: str
    team: str
    position: str
    price: float
    next_opponent: str
    minutes_avg: float
    xg90: float
    xa90: float
    attack_raw: float
    minutes_raw: float
    value_raw: float
    fixture_home: int
    attack_norm: float = 0.0
    minutes_norm: float = 0.0
    value_norm: float = 0.0
    score: float = 0.0
    rank_position: int = 0


def parse_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(value, high))


def parse_fixture_home(next_opponent: str) -> int:
    return 1 if str(next_opponent).strip().endswith("(H)") else 0


def parse_positions(raw: str) -> List[str]:
    allowed = {"GKP", "DEF", "MID", "FWD"}
    items = [item.strip().upper() for item in raw.split(",") if item.strip()]
    unique: List[str] = []
    for item in items:
        if item not in allowed:
            raise ValueError(f"Invalid position '{item}'. Allowed: {sorted(allowed)}")
        if item not in unique:
            unique.append(item)
    if not unique:
        raise ValueError("At least one position is required")
    return unique


def minmax_normalize(values: List[float]) -> List[float]:
    if not values:
        return []
    min_v = min(values)
    max_v = max(values)
    if max_v == min_v:
        return [0.5 for _ in values]
    span = max_v - min_v
    return [(v - min_v) / span for v in values]


def compute_weighted_score(
    attack_norm: float,
    minutes_norm: float,
    value_norm: float,
    fixture_norm: float,
    w_attack: float,
    w_minutes: float,
    w_value: float,
    w_fixture: float,
) -> float:
    score = (
        w_attack * attack_norm
        + w_minutes * minutes_norm
        + w_value * value_norm
        + w_fixture * fixture_norm
    )
    return round(score, 6)


def read_phase1_rows(input_path: Path) -> List[Dict[str, str]]:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    with input_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fields = set(reader.fieldnames or [])
        missing = INPUT_REQUIRED_COLUMNS - fields
        if missing:
            raise ValueError(
                "Input CSV missing required columns: " + ", ".join(sorted(missing))
            )
        return list(reader)


def build_candidates(
    rows: Iterable[Dict[str, str]],
    positions: List[str],
    min_minutes_avg: float,
) -> List[Candidate]:
    allowed_positions = set(positions)
    candidates: List[Candidate] = []
    for row in rows:
        position = str(row.get("position", "")).strip().upper()
        if position not in allowed_positions:
            continue

        minutes_avg = parse_float(row.get("minutes_avg", "0"))
        xg90 = parse_float(row.get("xg90", "0"))
        xa90 = parse_float(row.get("xa90", "0"))
        if minutes_avg < min_minutes_avg:
            continue
        if (xg90 + xa90) <= 0:
            continue

        price = parse_float(row.get("price", "0"))
        attack_raw = xg90 + (0.7 * xa90)
        minutes_raw = clamp(minutes_avg / 90.0, 0.0, 1.0)
        value_raw = (attack_raw / price) if price > 0 else 0.0
        fixture_home = parse_fixture_home(row.get("next_opponent", ""))

        candidates.append(
            Candidate(
                player=str(row.get("player", "")).strip(),
                team=str(row.get("team", "")).strip(),
                position=position,
                price=price,
                next_opponent=str(row.get("next_opponent", "")).strip(),
                minutes_avg=minutes_avg,
                xg90=xg90,
                xa90=xa90,
                attack_raw=attack_raw,
                minutes_raw=minutes_raw,
                value_raw=value_raw,
                fixture_home=fixture_home,
            )
        )
    return candidates


def normalize_by_position(candidates: List[Candidate], positions: List[str]) -> None:
    grouped: Dict[str, List[Candidate]] = {pos: [] for pos in positions}
    for candidate in candidates:
        grouped.setdefault(candidate.position, []).append(candidate)

    for position in positions:
        group = grouped.get(position, [])
        if not group:
            continue
        attack_norms = minmax_normalize([c.attack_raw for c in group])
        minutes_norms = minmax_normalize([c.minutes_raw for c in group])
        value_norms = minmax_normalize([c.value_raw for c in group])
        for idx, candidate in enumerate(group):
            candidate.attack_norm = attack_norms[idx]
            candidate.minutes_norm = minutes_norms[idx]
            candidate.value_norm = value_norms[idx]


def rank_candidates(
    candidates: List[Candidate],
    positions: List[str],
    top_n_per_position: int,
    w_attack: float,
    w_minutes: float,
    w_value: float,
    w_fixture: float,
) -> Tuple[List[Candidate], Dict[str, int]]:
    for candidate in candidates:
        candidate.score = compute_weighted_score(
            attack_norm=candidate.attack_norm,
            minutes_norm=candidate.minutes_norm,
            value_norm=candidate.value_norm,
            fixture_norm=float(candidate.fixture_home),
            w_attack=w_attack,
            w_minutes=w_minutes,
            w_value=w_value,
            w_fixture=w_fixture,
        )

    grouped: Dict[str, List[Candidate]] = {pos: [] for pos in positions}
    for candidate in candidates:
        grouped.setdefault(candidate.position, []).append(candidate)

    ranked: List[Candidate] = []
    rows_by_position: Dict[str, int] = {}
    for position in positions:
        group = grouped.get(position, [])
        group.sort(
            key=lambda c: (
                -c.score,
                -c.value_raw,
                c.price,
                c.player,
                c.team,
            )
        )
        top_group = group[: max(0, top_n_per_position)]
        for idx, candidate in enumerate(top_group, start=1):
            candidate.rank_position = idx
            ranked.append(candidate)
        rows_by_position[position] = len(top_group)

    return ranked, rows_by_position


def write_output_csv(rows: List[Candidate], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_COLUMNS)
        for row in rows:
            writer.writerow(
                [
                    row.player,
                    row.team,
                    row.position,
                    f"{row.price:.1f}",
                    row.next_opponent,
                    f"{row.minutes_avg:.2f}",
                    f"{row.xg90:.3f}",
                    f"{row.xa90:.3f}",
                    f"{row.attack_raw:.6f}",
                    f"{row.value_raw:.6f}",
                    str(row.fixture_home),
                    f"{row.score:.6f}",
                    str(row.rank_position),
                ]
            )


def write_output_json(
    output_json: Path,
    input_file: Path,
    positions: List[str],
    min_minutes_avg: float,
    top_n_per_position: int,
    w_attack: float,
    w_minutes: float,
    w_value: float,
    w_fixture: float,
    input_rows: int,
    eligible_rows: int,
    output_rows: int,
    rows_by_position: Dict[str, int],
) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "1.0",
        "input_file": str(input_file.resolve()),
        "filters": {
            "positions": positions,
            "min_minutes_avg": min_minutes_avg,
            "top_n_per_position": top_n_per_position,
        },
        "weights": {
            "w_attack": w_attack,
            "w_minutes": w_minutes,
            "w_value": w_value,
            "w_fixture": w_fixture,
        },
        "counts": {
            "input_rows": input_rows,
            "eligible_rows": eligible_rows,
            "output_rows": output_rows,
            "rows_by_position": rows_by_position,
        },
    }
    with output_json.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 2 ranking MVP for DEF/MID/FWD")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Phase 1 input CSV")
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
        help="Ranked output CSV path",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=DEFAULT_OUTPUT_JSON,
        help="Metadata JSON output path",
    )
    parser.add_argument(
        "--top-n-per-position",
        type=int,
        default=20,
        help="Top N rows to keep per position",
    )
    parser.add_argument(
        "--min-minutes-avg",
        type=float,
        default=30.0,
        help="Minimum minutes_avg to be eligible",
    )
    parser.add_argument(
        "--positions",
        type=str,
        default="DEF,MID,FWD",
        help="Comma-separated positions (e.g. DEF,MID,FWD)",
    )
    parser.add_argument("--w-attack", type=float, default=0.50, help="Weight for attack_norm")
    parser.add_argument("--w-minutes", type=float, default=0.25, help="Weight for minutes_norm")
    parser.add_argument("--w-value", type=float, default=0.20, help="Weight for value_norm")
    parser.add_argument("--w-fixture", type=float, default=0.05, help="Weight for fixture_norm")
    args = parser.parse_args()

    positions = parse_positions(args.positions)
    rows = read_phase1_rows(args.input)
    candidates = build_candidates(
        rows=rows,
        positions=positions,
        min_minutes_avg=max(0.0, args.min_minutes_avg),
    )
    normalize_by_position(candidates, positions)
    ranked, rows_by_position = rank_candidates(
        candidates=candidates,
        positions=positions,
        top_n_per_position=max(0, args.top_n_per_position),
        w_attack=args.w_attack,
        w_minutes=args.w_minutes,
        w_value=args.w_value,
        w_fixture=args.w_fixture,
    )
    write_output_csv(ranked, args.output_csv)
    write_output_json(
        output_json=args.output_json,
        input_file=args.input,
        positions=positions,
        min_minutes_avg=max(0.0, args.min_minutes_avg),
        top_n_per_position=max(0, args.top_n_per_position),
        w_attack=args.w_attack,
        w_minutes=args.w_minutes,
        w_value=args.w_value,
        w_fixture=args.w_fixture,
        input_rows=len(rows),
        eligible_rows=len(candidates),
        output_rows=len(ranked),
        rows_by_position=rows_by_position,
    )

    print(f"Wrote {len(ranked)} rows -> {args.output_csv}")
    print(f"Wrote metadata -> {args.output_json}")
    print(f"Input rows: {len(rows)}")
    print(f"Eligible rows: {len(candidates)}")
    print(
        "Rows by position: "
        + ", ".join(f"{position}={rows_by_position.get(position, 0)}" for position in positions)
    )


def _entrypoint() -> int:
    try:
        main()
        return 0
    except Exception as exc:
        print(f"Phase 2 ranking failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(_entrypoint())
