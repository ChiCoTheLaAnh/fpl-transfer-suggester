#!/usr/bin/env python3
"""Build Phase 2 feature table (player x GW fixture) from SQLite."""

from __future__ import annotations

import argparse
import math
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = REPO_ROOT / "data/fpl_mvp.db"
DEFAULT_OUTPUT = REPO_ROOT / "data/player_gw_features.parquet"

OUTPUT_COLUMNS = [
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


def rolling_mean(values: List[float], n: int) -> float:
    if n <= 0:
        return 0.0
    window = values[-n:]
    if not window:
        return 0.0
    return float(sum(window) / len(window))


def rolling_std(values: List[float], n: int) -> float:
    if n <= 0:
        return 0.0
    window = values[-n:]
    if len(window) <= 1:
        return 0.0
    mean = sum(window) / len(window)
    variance = sum((x - mean) ** 2 for x in window) / len(window)
    return float(math.sqrt(variance))


def compute_benching_probability(last_minutes: List[float]) -> float:
    if not last_minutes:
        return 1.0
    n = len(last_minutes)
    start_rate = sum(1 for m in last_minutes if m >= 60.0) / n
    cameo_rate = sum(1 for m in last_minutes if 0.0 < m < 60.0) / n
    dnp_rate = sum(1 for m in last_minutes if m == 0.0) / n
    bench_prob = (0.6 * dnp_rate) + (0.3 * cameo_rate) + (0.1 * (1.0 - start_rate))
    return max(0.0, min(1.0, float(bench_prob)))


def compute_risk_score(
    benching_probability: float,
    minutes_volatility_5: float,
    injury_flag: int,
) -> float:
    vol_component = max(0.0, min(1.0, minutes_volatility_5))
    risk = (0.55 * benching_probability) + (0.30 * vol_component) + (0.15 * float(injury_flag))
    return max(0.0, min(1.0, float(risk)))


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _is_prior_match(
    match_dt: pd.Timestamp,
    match_round: int,
    target_dt: pd.Timestamp,
    target_event: int,
) -> bool:
    if pd.notna(match_dt) and pd.notna(target_dt):
        return bool(match_dt < target_dt)
    return int(match_round) < int(target_event)


def _build_team_fixture_rows(fixtures: pd.DataFrame) -> pd.DataFrame:
    home = fixtures.rename(
        columns={
            "team_h": "team_id",
            "team_a": "opponent_team_id",
            "team_h_difficulty": "opponent_difficulty",
        }
    )[["fixture_id", "event", "kickoff_time", "finished", "team_id", "opponent_team_id", "opponent_difficulty"]]
    home["is_home"] = 1

    away = fixtures.rename(
        columns={
            "team_a": "team_id",
            "team_h": "opponent_team_id",
            "team_a_difficulty": "opponent_difficulty",
        }
    )[["fixture_id", "event", "kickoff_time", "finished", "team_id", "opponent_team_id", "opponent_difficulty"]]
    away["is_home"] = 0

    team_fixtures = pd.concat([home, away], ignore_index=True)
    team_fixtures["event"] = team_fixtures["event"].fillna(0).astype(int)
    team_fixtures["opponent_difficulty"] = team_fixtures["opponent_difficulty"].fillna(0).astype(int)
    team_fixtures["kickoff_dt"] = pd.to_datetime(team_fixtures["kickoff_time"], utc=True, errors="coerce")
    return team_fixtures


def _build_rest_days_map(team_fixtures: pd.DataFrame) -> Dict[Tuple[int, int], Optional[float]]:
    rest_days_map: Dict[Tuple[int, int], Optional[float]] = {}
    for team_id, group in team_fixtures.groupby("team_id", sort=False):
        group_sorted = group.sort_values(
            by=["kickoff_dt", "event", "fixture_id"],
            kind="mergesort",
            na_position="last",
        )
        previous_dt: Optional[pd.Timestamp] = None
        for row in group_sorted.itertuples(index=False):
            current_dt = row.kickoff_dt
            rest_days: Optional[float] = None
            if previous_dt is not None and pd.notna(current_dt) and pd.notna(previous_dt):
                delta_days = (current_dt - previous_dt).total_seconds() / 86400.0
                rest_days = float(max(0.0, delta_days))
            rest_days_map[(int(team_id), int(row.fixture_id))] = rest_days
            if pd.notna(current_dt):
                previous_dt = current_dt
    return rest_days_map


def _build_horizon_map(team_fixtures: pd.DataFrame, horizon: int) -> Dict[Tuple[int, int], Tuple[int, float, int]]:
    horizon_map: Dict[Tuple[int, int], Tuple[int, float, int]] = {}
    for team_id, group in team_fixtures.groupby("team_id", sort=False):
        by_event = group.groupby("event", as_index=False).agg(
            fixture_count=("fixture_id", "count"),
            avg_diff=("opponent_difficulty", "mean"),
            home_count=("is_home", "sum"),
        )
        event_stats = {
            int(row.event): (
                int(row.fixture_count),
                float(row.avg_diff),
                int(row.home_count),
            )
            for row in by_event.itertuples(index=False)
        }
        all_events = sorted(event_stats.keys())
        for event in all_events:
            total_fixtures = 0
            total_home = 0
            weighted_diff_sum = 0.0
            for candidate_event in range(event, event + max(1, horizon)):
                if candidate_event not in event_stats:
                    continue
                fixture_count, avg_diff, home_count = event_stats[candidate_event]
                total_fixtures += fixture_count
                total_home += home_count
                weighted_diff_sum += avg_diff * fixture_count
            avg_difficulty = (weighted_diff_sum / total_fixtures) if total_fixtures > 0 else 0.0
            horizon_map[(int(team_id), int(event))] = (
                int(total_fixtures),
                float(avg_difficulty),
                int(total_home),
            )
    return horizon_map


def build_feature_table(
    db_path: Path,
    output_path: Path,
    horizon: int,
    include_finished: bool,
) -> Tuple[int, int]:
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite DB not found: {db_path}")

    with sqlite3.connect(str(db_path)) as conn:
        players = pd.read_sql_query(
            """
            SELECT p.id AS player_id,
                   (p.first_name || ' ' || p.second_name) AS player_name,
                   p.team_id,
                   t.short_name AS team,
                   p.position,
                   p.now_cost,
                   p.status
            FROM players p
            JOIN teams t ON t.id = p.team_id
            """,
            conn,
        )

        fixtures = pd.read_sql_query(
            """
            SELECT id AS fixture_id,
                   event,
                   kickoff_time,
                   finished,
                   team_h,
                   team_a,
                   team_h_difficulty,
                   team_a_difficulty
            FROM fixtures
            """,
            conn,
        )

        history = pd.read_sql_query(
            """
            SELECT player_id,
                   fixture_id,
                   round,
                   kickoff_time,
                   minutes,
                   total_points,
                   expected_goal_involvements,
                   expected_goals,
                   expected_assists,
                   value
            FROM gw_player_stats
            """,
            conn,
        )

    if players.empty or fixtures.empty:
        raise ValueError("Missing source data in DB (players/fixtures)")

    fixtures["finished"] = fixtures["finished"].fillna(0).astype(int)
    team_fixtures = _build_team_fixture_rows(fixtures)

    team_name_map = {int(row.team_id): str(row.team) for row in players[["team_id", "team"]].drop_duplicates().itertuples(index=False)}
    dgw_counts = (
        team_fixtures.groupby(["team_id", "event"], as_index=False)
        .agg(dgw_count_in_event=("fixture_id", "count"))
    )
    dgw_count_map = {
        (int(row.team_id), int(row.event)): int(row.dgw_count_in_event)
        for row in dgw_counts.itertuples(index=False)
    }
    rest_days_map = _build_rest_days_map(team_fixtures)
    horizon_map = _build_horizon_map(team_fixtures, horizon=max(1, horizon))

    target_team_fixtures = team_fixtures.copy()
    if not include_finished:
        target_team_fixtures = target_team_fixtures[target_team_fixtures["finished"] == 0].copy()
    target_team_fixtures = target_team_fixtures[target_team_fixtures["event"] > 0].copy()
    if target_team_fixtures.empty:
        raise ValueError("No target fixtures found after filtering")

    target_rows = players.merge(
        target_team_fixtures[
            [
                "fixture_id",
                "event",
                "kickoff_time",
                "kickoff_dt",
                "team_id",
                "opponent_team_id",
                "opponent_difficulty",
                "is_home",
            ]
        ],
        on="team_id",
        how="inner",
    )

    history["round"] = history["round"].fillna(0).astype(int)
    history["kickoff_dt"] = pd.to_datetime(history["kickoff_time"], utc=True, errors="coerce")
    history["minutes"] = history["minutes"].fillna(0).astype(float)
    history["total_points"] = history["total_points"].fillna(0).astype(float)
    history["xgi_proxy"] = history["expected_goal_involvements"].fillna(
        history["expected_goals"].fillna(0.0) + history["expected_assists"].fillna(0.0)
    ).astype(float)
    history["price"] = history["value"].fillna(0).astype(float) / 10.0

    player_history: Dict[int, List[Dict[str, Any]]] = {}
    for player_id, group in history.groupby("player_id", sort=False):
        group_sorted = group.sort_values(
            by=["kickoff_dt", "round", "fixture_id"],
            kind="mergesort",
            na_position="last",
        )
        player_history[int(player_id)] = [
            {
                "kickoff_dt": row.kickoff_dt,
                "round": int(row.round),
                "minutes": float(row.minutes),
                "points": float(row.total_points),
                "xgi": float(row.xgi_proxy),
                "price": float(row.price),
            }
            for row in group_sorted.itertuples(index=False)
        ]

    feature_rows: List[Dict[str, Any]] = []
    for row in target_rows.itertuples(index=False):
        player_id = int(row.player_id)
        target_event = int(row.event)
        target_dt = row.kickoff_dt
        history_rows = player_history.get(player_id, [])

        prior = [
            h
            for h in history_rows
            if _is_prior_match(
                match_dt=h["kickoff_dt"],
                match_round=int(h["round"]),
                target_dt=target_dt,
                target_event=target_event,
            )
        ]

        prior_minutes = [float(h["minutes"]) for h in prior]
        prior_points = [float(h["points"]) for h in prior]
        prior_xgi = [float(h["xgi"]) for h in prior]
        prior_prices = [float(h["price"]) for h in prior if float(h["price"]) > 0]

        minutes_volatility_5 = rolling_std(prior_minutes, 5) / 90.0
        points_volatility_5 = rolling_std(prior_points, 5)
        benching_probability = compute_benching_probability(prior_minutes[-5:])
        injury_flag = 0 if str(row.status).lower() == "a" else 1
        risk_score = compute_risk_score(
            benching_probability=benching_probability,
            minutes_volatility_5=minutes_volatility_5,
            injury_flag=injury_flag,
        )

        dgw_count = dgw_count_map.get((int(row.team_id), target_event), 1)
        horizon_fixture_count, horizon_avg_diff, horizon_home_count = horizon_map.get(
            (int(row.team_id), target_event),
            (0, 0.0, 0),
        )
        rest_days = rest_days_map.get((int(row.team_id), int(row.fixture_id)))
        opponent_team_id = int(row.opponent_team_id)

        feature_rows.append(
            {
                "player_id": player_id,
                "player_name": str(row.player_name),
                "team_id": int(row.team_id),
                "team": str(row.team),
                "position": str(row.position),
                "fixture_id": int(row.fixture_id),
                "target_event": target_event,
                "kickoff_time": str(row.kickoff_time or ""),
                "is_home": int(row.is_home),
                "opponent_team_id": opponent_team_id,
                "opponent_team": team_name_map.get(opponent_team_id, "UNK"),
                "opponent_difficulty": int(_as_float(row.opponent_difficulty)),
                "dgw_count_in_event": int(dgw_count),
                "is_dgw": 1 if int(dgw_count) > 1 else 0,
                "rest_days": float(rest_days) if rest_days is not None else None,
                "horizon": int(max(1, horizon)),
                "horizon_fixture_count_team": int(horizon_fixture_count),
                "horizon_avg_fixture_difficulty": float(horizon_avg_diff),
                "horizon_home_count": int(horizon_home_count),
                "recent_points_avg_3": rolling_mean(prior_points, 3),
                "recent_points_avg_5": rolling_mean(prior_points, 5),
                "recent_minutes_avg_3": rolling_mean(prior_minutes, 3),
                "recent_minutes_avg_5": rolling_mean(prior_minutes, 5),
                "recent_xgi_avg_3": rolling_mean(prior_xgi, 3),
                "recent_xgi_avg_5": rolling_mean(prior_xgi, 5),
                "minutes_volatility_5": float(minutes_volatility_5),
                "points_volatility_5": float(points_volatility_5),
                "benching_probability": float(benching_probability),
                "risk_score": float(risk_score),
                "last_price": float(prior_prices[-1]) if prior_prices else float(_as_float(row.now_cost)),
                "current_status": str(row.status),
            }
        )

    feature_df = pd.DataFrame(feature_rows, columns=OUTPUT_COLUMNS)
    feature_df.sort_values(
        by=["target_event", "fixture_id", "team_id", "player_id"],
        kind="mergesort",
        inplace=True,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    feature_df.to_parquet(output_path, index=False)
    return int(len(target_rows)), int(len(feature_df))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build player x GW feature table from SQLite")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="SQLite DB path")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Parquet output path",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=3,
        help="Planning horizon in gameweeks (for horizon features)",
    )
    parser.add_argument(
        "--include-finished",
        action="store_true",
        help="Include finished fixtures in output target rows",
    )
    args = parser.parse_args()

    joined_rows, feature_rows = build_feature_table(
        db_path=args.db,
        output_path=args.output,
        horizon=max(1, args.horizon),
        include_finished=args.include_finished,
    )
    print(f"Wrote {feature_rows} rows -> {args.output}")
    print(f"Joined candidate rows: {joined_rows}")
    print(f"Horizon: {max(1, args.horizon)}")
    print(f"Include finished fixtures: {bool(args.include_finished)}")


def _entrypoint() -> int:
    try:
        main()
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"Build feature table failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(_entrypoint())
