#!/usr/bin/env python3
"""Phase 1 data pipeline for Fantasy Suggestor.

Builds a player-level feature table from FPL API with the schema:
player, team, position, price, minutes_avg, goals, assists, xg90, xa90, next_opponent
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen

FPL_BASE = "https://fantasy.premierleague.com/api"


@dataclass
class PlayerRow:
    player: str
    team: str
    position: str
    price: float
    minutes_avg: float
    goals: int
    assists: int
    xg90: float
    xa90: float
    next_opponent: str


def fetch_json(url: str, timeout: int = 30, retries: int = 2, retry_backoff_s: float = 0.5) -> Any:
    req = Request(
        url,
        headers={
            "User-Agent": "Fantasy-Suggestor-Data-Pipeline/1.0",
            "Accept": "application/json",
        },
    )
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            with urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= retries:
                break
            sleep(retry_backoff_s * (2 ** attempt))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("fetch_json failed without exception")


def parse_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def per90(value: float, minutes: float, min_minutes: float = 0.0) -> float:
    if minutes <= 0 or minutes < min_minutes:
        return 0.0
    return (value / minutes) * 90.0


def average_last_n_minutes(history: List[Dict[str, Any]], last_n: int) -> float:
    if last_n <= 0:
        return 0.0

    recent_rows = [row for row in history if row.get("minutes") is not None][-last_n:]
    if not recent_rows:
        return 0.0

    total_minutes = sum(parse_float(row.get("minutes")) for row in recent_rows)
    return total_minutes / len(recent_rows)


def normalize_text(text: str) -> str:
    return " ".join(text.strip().lower().split())


def load_understat_overrides(csv_path: Optional[Path]) -> Dict[Tuple[str, str], Tuple[float, float]]:
    if not csv_path:
        return {}

    if not csv_path.exists():
        raise FileNotFoundError(f"Understat override file not found: {csv_path}")

    overrides: Dict[Tuple[str, str], Tuple[float, float]] = {}
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"player", "team", "xg90", "xa90"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                "Understat override CSV missing required columns: "
                + ", ".join(sorted(missing))
            )

        for row in reader:
            key = (normalize_text(row.get("player", "")), normalize_text(row.get("team", "")))
            overrides[key] = (
                parse_float(row.get("xg90")),
                parse_float(row.get("xa90")),
            )
    return overrides


def fetch_player_history(
    player_id: int,
    cache_dir: Path,
    refresh_cache: bool,
    retries: int,
    retry_backoff_s: float,
) -> Tuple[List[Dict[str, Any]], str]:
    cache_path = cache_dir / f"{player_id}.json"
    if not refresh_cache and cache_path.exists():
        try:
            with cache_path.open("r", encoding="utf-8") as f:
                cached_summary = json.load(f)
            history = cached_summary.get("history", [])
            if isinstance(history, list):
                return history, "cache"
        except Exception:  # noqa: BLE001
            pass

    try:
        summary = fetch_json(
            f"{FPL_BASE}/element-summary/{player_id}/",
            retries=retries,
            retry_backoff_s=retry_backoff_s,
        )
    except Exception:  # noqa: BLE001
        return [], "error"

    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(summary, f)
    except Exception:  # noqa: BLE001
        pass

    history = summary.get("history", [])
    if not isinstance(history, list):
        return [], "error"
    return history, "api"


def build_histories_for_players(
    players: List[Dict[str, Any]],
    cache_dir: Path,
    refresh_cache: bool,
    retries: int,
    retry_backoff_s: float,
    workers: int,
) -> Tuple[Dict[int, List[Dict[str, Any]]], Dict[str, int]]:
    histories: Dict[int, List[Dict[str, Any]]] = {}
    stats = {"cache_hits": 0, "api_hits": 0, "errors": 0}

    max_workers = max(1, workers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_player_id = {
            executor.submit(
                fetch_player_history,
                int(player.get("id", 0)),
                cache_dir,
                refresh_cache,
                retries,
                retry_backoff_s,
            ): int(player.get("id", 0))
            for player in players
            if int(player.get("id", 0)) > 0
        }
        for future in as_completed(future_to_player_id):
            player_id = future_to_player_id[future]
            history, source = future.result()
            histories[player_id] = history
            if source == "cache":
                stats["cache_hits"] += 1
            elif source == "api":
                stats["api_hits"] += 1
            else:
                stats["errors"] += 1

    return histories, stats


def get_next_fixture_by_team(fixtures: List[Dict[str, Any]], team_names: Dict[int, str]) -> Dict[int, str]:
    upcoming_by_team: Dict[int, List[Dict[str, Any]]] = {}
    now = datetime.now(timezone.utc)

    for fixture in fixtures:
        if fixture.get("finished"):
            continue

        kickoff_raw = fixture.get("kickoff_time")
        kickoff_dt: Optional[datetime] = None
        if kickoff_raw:
            try:
                kickoff_dt = datetime.fromisoformat(kickoff_raw.replace("Z", "+00:00"))
            except ValueError:
                kickoff_dt = None

        if kickoff_dt and kickoff_dt < now:
            continue

        for team_key in ("team_h", "team_a"):
            team_id = fixture.get(team_key)
            if team_id is None:
                continue
            upcoming_by_team.setdefault(team_id, []).append(fixture)

    next_opponent_by_team: Dict[int, str] = {}

    def sort_key(fixture: Dict[str, Any]) -> tuple:
        kickoff_raw = fixture.get("kickoff_time")
        if not kickoff_raw:
            return (datetime.max.replace(tzinfo=timezone.utc), fixture.get("event") or 999)
        try:
            dt = datetime.fromisoformat(kickoff_raw.replace("Z", "+00:00"))
        except ValueError:
            dt = datetime.max.replace(tzinfo=timezone.utc)
        return (dt, fixture.get("event") or 999)

    for team_id, team_fixtures in upcoming_by_team.items():
        team_fixtures.sort(key=sort_key)
        fixture = team_fixtures[0]
        is_home = fixture.get("team_h") == team_id
        opponent_id = fixture.get("team_a") if is_home else fixture.get("team_h")
        opponent_name = team_names.get(opponent_id, "UNKNOWN")
        home_away = "H" if is_home else "A"
        next_opponent_by_team[team_id] = f"{opponent_name} ({home_away})"

    return next_opponent_by_team


def build_player_rows(
    last_n: int = 5,
    min_minutes_for_per90: float = 180.0,
    cache_dir: Path = Path("data/cache/element_summary"),
    refresh_cache: bool = False,
    retries: int = 2,
    retry_backoff_s: float = 0.5,
    workers: int = 16,
    understat_overrides: Optional[Dict[Tuple[str, str], Tuple[float, float]]] = None,
) -> Tuple[List[PlayerRow], Dict[str, Any]]:
    bootstrap = fetch_json(f"{FPL_BASE}/bootstrap-static/", retries=retries, retry_backoff_s=retry_backoff_s)
    fixtures = fetch_json(f"{FPL_BASE}/fixtures/?future=1", retries=retries, retry_backoff_s=retry_backoff_s)

    teams = bootstrap.get("teams", [])
    element_types = bootstrap.get("element_types", [])
    players = bootstrap.get("elements", [])

    team_names = {int(team["id"]): team["short_name"] for team in teams}
    position_names = {
        int(position["id"]): str(position.get("singular_name_short") or position.get("singular_name") or "UNK")
        for position in element_types
    }
    next_opponent_by_team = get_next_fixture_by_team(fixtures, team_names)
    overrides = understat_overrides or {}
    histories_by_player, fetch_stats = build_histories_for_players(
        players=players,
        cache_dir=cache_dir,
        refresh_cache=refresh_cache,
        retries=retries,
        retry_backoff_s=retry_backoff_s,
        workers=workers,
    )
    override_stats: Dict[str, Any] = {
        "players_total": len(players),
        "overrides_matched": 0,
        "overrides_unmatched": 0,
        "override_coverage_pct": 0.0,
    }

    rows: List[PlayerRow] = []
    for player in players:
        player_id = int(player.get("id", 0))
        team_id = int(player.get("team", 0))
        element_type_id = int(player.get("element_type", 0))
        minutes = parse_float(player.get("minutes"))

        xg = parse_float(player.get("expected_goals"))
        xa = parse_float(player.get("expected_assists"))

        history = histories_by_player.get(player_id, [])
        minutes_avg = average_last_n_minutes(history, last_n)

        player_name = f"{player.get('first_name', '').strip()} {player.get('second_name', '').strip()}".strip()
        team_name = team_names.get(team_id, "UNKNOWN")
        position_name = position_names.get(element_type_id, "UNK")
        override_key = (normalize_text(player_name), normalize_text(team_name))
        xg90 = per90(xg, minutes, min_minutes=min_minutes_for_per90)
        xa90 = per90(xa, minutes, min_minutes=min_minutes_for_per90)
        if override_key in overrides:
            xg90, xa90 = overrides[override_key]
            override_stats["overrides_matched"] += 1
        else:
            override_stats["overrides_unmatched"] += 1

        rows.append(
            PlayerRow(
                player=player_name,
                team=team_name,
                position=position_name,
                price=parse_float(player.get("now_cost")) / 10.0,
                minutes_avg=round(minutes_avg, 2),
                goals=parse_int(player.get("goals_scored")),
                assists=parse_int(player.get("assists")),
                xg90=round(xg90, 3),
                xa90=round(xa90, 3),
                next_opponent=next_opponent_by_team.get(team_id, "TBD"),
            )
        )

    rows.sort(key=lambda r: (r.team, r.position, -r.xg90, -r.xa90, r.player))
    players_total = int(override_stats["players_total"])
    if players_total > 0:
        override_stats["override_coverage_pct"] = round(
            (100.0 * int(override_stats["overrides_matched"])) / players_total,
            2,
        )

    pipeline_stats: Dict[str, Any] = {**fetch_stats, **override_stats}
    return rows, pipeline_stats


def write_csv(rows: List[PlayerRow], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
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
        )
        for row in rows:
            writer.writerow([
                row.player,
                row.team,
                row.position,
                f"{row.price:.1f}",
                f"{row.minutes_avg:.2f}",
                str(row.goals),
                str(row.assists),
                f"{row.xg90:.3f}",
                f"{row.xa90:.3f}",
                row.next_opponent,
            ])


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Phase 1 player feature table from FPL API")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/phase1_player_features.csv"),
        help="Output CSV path",
    )
    parser.add_argument(
        "--last-n",
        type=int,
        default=5,
        help="Compute minutes_avg from the most recent N matches in element-summary history",
    )
    parser.add_argument(
        "--min-minutes-per90",
        type=float,
        default=180.0,
        help="Set xg90/xa90 to 0 if season minutes are below this threshold",
    )
    parser.add_argument(
        "--understat-csv",
        type=Path,
        default=None,
        help="Optional CSV override with columns: player,team,xg90,xa90",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("data/cache/element_summary"),
        help="Directory used to cache FPL element-summary responses",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Ignore existing element-summary cache and fetch again",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Retries per HTTP request before failing",
    )
    parser.add_argument(
        "--retry-backoff",
        type=float,
        default=0.5,
        help="Base exponential backoff in seconds between retries",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Parallel workers for element-summary fetching",
    )
    args = parser.parse_args()

    overrides = load_understat_overrides(args.understat_csv)
    rows, pipeline_stats = build_player_rows(
        last_n=max(args.last_n, 1),
        min_minutes_for_per90=max(args.min_minutes_per90, 0.0),
        cache_dir=args.cache_dir,
        refresh_cache=args.refresh_cache,
        retries=max(args.retries, 0),
        retry_backoff_s=max(args.retry_backoff, 0.0),
        workers=max(args.workers, 1),
        understat_overrides=overrides,
    )
    write_csv(rows, args.output)

    print(f"Wrote {len(rows)} rows -> {args.output}")
    print(
        "element-summary fetch stats: "
        f"api={pipeline_stats['api_hits']}, cache={pipeline_stats['cache_hits']}, errors={pipeline_stats['errors']}"
    )
    print(
        "override stats: "
        f"matched={pipeline_stats['overrides_matched']}, "
        f"unmatched={pipeline_stats['overrides_unmatched']}, "
        f"coverage={float(pipeline_stats['override_coverage_pct']):.2f}%"
    )


def _entrypoint() -> int:
    try:

        main()
        return 0
    except Exception as exc:
        print(f"Pipeline failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(_entrypoint())
