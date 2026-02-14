#!/usr/bin/env python3
"""Ingest FPL data into SQLite for transfer suggestion MVP."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen

FPL_BASE = "https://fantasy.premierleague.com/api"
DEFAULT_DB = Path("data/fpl_mvp.db")
DEFAULT_CACHE_DIR = Path("data/cache/element_summary")


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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_json(
    url: str,
    timeout: int = 30,
    retries: int = 2,
    retry_backoff_s: float = 0.5,
    cookie: Optional[str] = None,
) -> Any:
    headers = {
        "User-Agent": "Fantasy-Suggestor-Ingest/1.0",
        "Accept": "application/json",
    }
    if cookie:
        headers["Cookie"] = cookie

    req = Request(url, headers=headers)
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
    if last_exc:
        raise last_exc
    raise RuntimeError("fetch_json failed without exception")


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            short_name TEXT NOT NULL,
            strength INTEGER,
            strength_overall_home INTEGER,
            strength_overall_away INTEGER,
            strength_attack_home INTEGER,
            strength_attack_away INTEGER,
            strength_defence_home INTEGER,
            strength_defence_away INTEGER,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            second_name TEXT NOT NULL,
            web_name TEXT NOT NULL,
            team_id INTEGER NOT NULL,
            position TEXT NOT NULL,
            status TEXT,
            now_cost REAL NOT NULL,
            total_points INTEGER NOT NULL,
            minutes INTEGER NOT NULL,
            goals_scored INTEGER NOT NULL,
            assists INTEGER NOT NULL,
            expected_goals REAL NOT NULL,
            expected_assists REAL NOT NULL,
            selected_by_percent REAL,
            news TEXT,
            news_added TEXT,
            chance_of_playing_next_round INTEGER,
            chance_of_playing_this_round INTEGER,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(team_id) REFERENCES teams(id)
        );

        CREATE TABLE IF NOT EXISTS fixtures (
            id INTEGER PRIMARY KEY,
            event INTEGER,
            kickoff_time TEXT,
            finished INTEGER NOT NULL,
            team_h INTEGER NOT NULL,
            team_a INTEGER NOT NULL,
            team_h_difficulty INTEGER,
            team_a_difficulty INTEGER,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(team_h) REFERENCES teams(id),
            FOREIGN KEY(team_a) REFERENCES teams(id)
        );

        CREATE TABLE IF NOT EXISTS gw_player_stats (
            player_id INTEGER NOT NULL,
            fixture_id INTEGER NOT NULL,
            round INTEGER,
            kickoff_time TEXT,
            opponent_team INTEGER,
            was_home INTEGER,
            minutes INTEGER NOT NULL,
            goals_scored INTEGER NOT NULL,
            assists INTEGER NOT NULL,
            clean_sheets INTEGER NOT NULL,
            goals_conceded INTEGER NOT NULL,
            own_goals INTEGER NOT NULL,
            penalties_saved INTEGER NOT NULL,
            penalties_missed INTEGER NOT NULL,
            yellow_cards INTEGER NOT NULL,
            red_cards INTEGER NOT NULL,
            saves INTEGER NOT NULL,
            bonus INTEGER NOT NULL,
            bps INTEGER NOT NULL,
            influence REAL,
            creativity REAL,
            threat REAL,
            ict_index REAL,
            expected_goals REAL,
            expected_assists REAL,
            expected_goal_involvements REAL,
            expected_goals_conceded REAL,
            value INTEGER,
            total_points INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(player_id, fixture_id),
            FOREIGN KEY(player_id) REFERENCES players(id),
            FOREIGN KEY(fixture_id) REFERENCES fixtures(id)
        );

        CREATE TABLE IF NOT EXISTS prices (
            player_id INTEGER NOT NULL,
            fixture_id INTEGER NOT NULL,
            round INTEGER,
            kickoff_time TEXT,
            price REAL NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(player_id, fixture_id),
            FOREIGN KEY(player_id) REFERENCES players(id),
            FOREIGN KEY(fixture_id) REFERENCES fixtures(id)
        );

        CREATE TABLE IF NOT EXISTS injuries_news (
            player_id INTEGER PRIMARY KEY,
            status TEXT,
            news TEXT,
            news_added TEXT,
            chance_of_playing_next_round INTEGER,
            chance_of_playing_this_round INTEGER,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(player_id) REFERENCES players(id)
        );

        CREATE TABLE IF NOT EXISTS user_squad_snapshot (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_id INTEGER NOT NULL,
            event INTEGER,
            bank REAL,
            squad_value REAL,
            free_transfers INTEGER,
            active_chip TEXT,
            source TEXT NOT NULL,
            captured_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS user_squad_picks (
            snapshot_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            multiplier INTEGER NOT NULL,
            is_captain INTEGER NOT NULL,
            is_vice_captain INTEGER NOT NULL,
            purchase_price REAL,
            selling_price REAL,
            PRIMARY KEY(snapshot_id, position),
            FOREIGN KEY(snapshot_id) REFERENCES user_squad_snapshot(snapshot_id),
            FOREIGN KEY(player_id) REFERENCES players(id)
        );

        CREATE INDEX IF NOT EXISTS idx_players_team_id ON players(team_id);
        CREATE INDEX IF NOT EXISTS idx_gw_stats_round ON gw_player_stats(round);
        CREATE INDEX IF NOT EXISTS idx_gw_stats_player ON gw_player_stats(player_id);
        CREATE INDEX IF NOT EXISTS idx_prices_player ON prices(player_id);
        CREATE INDEX IF NOT EXISTS idx_user_snapshot_entry ON user_squad_snapshot(entry_id, captured_at);
        """
    )
    conn.commit()


def upsert_teams(conn: sqlite3.Connection, teams: List[Dict[str, Any]]) -> int:
    now = utc_now_iso()
    sql = """
    INSERT INTO teams (
        id, name, short_name, strength, strength_overall_home, strength_overall_away,
        strength_attack_home, strength_attack_away, strength_defence_home, strength_defence_away, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
        name=excluded.name,
        short_name=excluded.short_name,
        strength=excluded.strength,
        strength_overall_home=excluded.strength_overall_home,
        strength_overall_away=excluded.strength_overall_away,
        strength_attack_home=excluded.strength_attack_home,
        strength_attack_away=excluded.strength_attack_away,
        strength_defence_home=excluded.strength_defence_home,
        strength_defence_away=excluded.strength_defence_away,
        updated_at=excluded.updated_at
    """
    rows = [
        (
            parse_int(team.get("id")),
            str(team.get("name") or ""),
            str(team.get("short_name") or ""),
            parse_int(team.get("strength")),
            parse_int(team.get("strength_overall_home")),
            parse_int(team.get("strength_overall_away")),
            parse_int(team.get("strength_attack_home")),
            parse_int(team.get("strength_attack_away")),
            parse_int(team.get("strength_defence_home")),
            parse_int(team.get("strength_defence_away")),
            now,
        )
        for team in teams
    ]
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


def upsert_players(
    conn: sqlite3.Connection,
    players: List[Dict[str, Any]],
    position_names: Dict[int, str],
) -> int:
    now = utc_now_iso()
    sql = """
    INSERT INTO players (
        id, first_name, second_name, web_name, team_id, position, status, now_cost,
        total_points, minutes, goals_scored, assists, expected_goals, expected_assists,
        selected_by_percent, news, news_added, chance_of_playing_next_round,
        chance_of_playing_this_round, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
        first_name=excluded.first_name,
        second_name=excluded.second_name,
        web_name=excluded.web_name,
        team_id=excluded.team_id,
        position=excluded.position,
        status=excluded.status,
        now_cost=excluded.now_cost,
        total_points=excluded.total_points,
        minutes=excluded.minutes,
        goals_scored=excluded.goals_scored,
        assists=excluded.assists,
        expected_goals=excluded.expected_goals,
        expected_assists=excluded.expected_assists,
        selected_by_percent=excluded.selected_by_percent,
        news=excluded.news,
        news_added=excluded.news_added,
        chance_of_playing_next_round=excluded.chance_of_playing_next_round,
        chance_of_playing_this_round=excluded.chance_of_playing_this_round,
        updated_at=excluded.updated_at
    """
    rows = []
    for player in players:
        element_type = parse_int(player.get("element_type"))
        rows.append(
            (
                parse_int(player.get("id")),
                str(player.get("first_name") or ""),
                str(player.get("second_name") or ""),
                str(player.get("web_name") or ""),
                parse_int(player.get("team")),
                position_names.get(element_type, "UNK"),
                str(player.get("status") or ""),
                parse_float(player.get("now_cost")) / 10.0,
                parse_int(player.get("total_points")),
                parse_int(player.get("minutes")),
                parse_int(player.get("goals_scored")),
                parse_int(player.get("assists")),
                parse_float(player.get("expected_goals")),
                parse_float(player.get("expected_assists")),
                parse_float(player.get("selected_by_percent")),
                str(player.get("news") or ""),
                str(player.get("news_added") or ""),
                parse_int(player.get("chance_of_playing_next_round")),
                parse_int(player.get("chance_of_playing_this_round")),
                now,
            )
        )
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


def upsert_injuries_news(conn: sqlite3.Connection, players: List[Dict[str, Any]]) -> int:
    now = utc_now_iso()
    sql = """
    INSERT INTO injuries_news (
        player_id, status, news, news_added, chance_of_playing_next_round,
        chance_of_playing_this_round, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(player_id) DO UPDATE SET
        status=excluded.status,
        news=excluded.news,
        news_added=excluded.news_added,
        chance_of_playing_next_round=excluded.chance_of_playing_next_round,
        chance_of_playing_this_round=excluded.chance_of_playing_this_round,
        updated_at=excluded.updated_at
    """
    rows = [
        (
            parse_int(player.get("id")),
            str(player.get("status") or ""),
            str(player.get("news") or ""),
            str(player.get("news_added") or ""),
            parse_int(player.get("chance_of_playing_next_round")),
            parse_int(player.get("chance_of_playing_this_round")),
            now,
        )
        for player in players
    ]
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


def upsert_fixtures(conn: sqlite3.Connection, fixtures: List[Dict[str, Any]]) -> int:
    now = utc_now_iso()
    sql = """
    INSERT INTO fixtures (
        id, event, kickoff_time, finished, team_h, team_a, team_h_difficulty, team_a_difficulty, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
        event=excluded.event,
        kickoff_time=excluded.kickoff_time,
        finished=excluded.finished,
        team_h=excluded.team_h,
        team_a=excluded.team_a,
        team_h_difficulty=excluded.team_h_difficulty,
        team_a_difficulty=excluded.team_a_difficulty,
        updated_at=excluded.updated_at
    """
    rows = [
        (
            parse_int(fixture.get("id")),
            parse_int(fixture.get("event")),
            str(fixture.get("kickoff_time") or ""),
            1 if fixture.get("finished") else 0,
            parse_int(fixture.get("team_h")),
            parse_int(fixture.get("team_a")),
            parse_int(fixture.get("team_h_difficulty")),
            parse_int(fixture.get("team_a_difficulty")),
            now,
        )
        for fixture in fixtures
        if parse_int(fixture.get("id")) > 0
    ]
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


def fetch_player_history(
    player_id: int,
    cache_dir: Path,
    refresh_cache: bool,
    retries: int,
    retry_backoff_s: float,
) -> Tuple[int, List[Dict[str, Any]], str]:
    cache_path = cache_dir / f"{player_id}.json"

    if not refresh_cache and cache_path.exists():
        try:
            with cache_path.open("r", encoding="utf-8") as f:
                summary = json.load(f)
            history = summary.get("history", [])
            if isinstance(history, list):
                return player_id, history, "cache"
        except Exception:  # noqa: BLE001
            pass

    try:
        summary = fetch_json(
            f"{FPL_BASE}/element-summary/{player_id}/",
            retries=retries,
            retry_backoff_s=retry_backoff_s,
        )
    except Exception:  # noqa: BLE001
        return player_id, [], "error"

    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(summary, f)
    except Exception:  # noqa: BLE001
        pass

    history = summary.get("history", [])
    if not isinstance(history, list):
        return player_id, [], "error"
    return player_id, history, "api"


def fetch_histories(
    player_ids: List[int],
    cache_dir: Path,
    refresh_cache: bool,
    retries: int,
    retry_backoff_s: float,
    workers: int,
) -> Tuple[Dict[int, List[Dict[str, Any]]], Dict[str, int]]:
    stats = {"api_hits": 0, "cache_hits": 0, "errors": 0}
    histories: Dict[int, List[Dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(
                fetch_player_history,
                player_id,
                cache_dir,
                refresh_cache,
                retries,
                retry_backoff_s,
            ): player_id
            for player_id in player_ids
        }
        for future in as_completed(futures):
            player_id, history, source = future.result()
            histories[player_id] = history
            if source == "api":
                stats["api_hits"] += 1
            elif source == "cache":
                stats["cache_hits"] += 1
            else:
                stats["errors"] += 1
    return histories, stats


def upsert_gw_stats_and_prices(
    conn: sqlite3.Connection,
    histories: Dict[int, List[Dict[str, Any]]],
) -> Tuple[int, int]:
    now = utc_now_iso()
    stat_sql = """
    INSERT INTO gw_player_stats (
        player_id, fixture_id, round, kickoff_time, opponent_team, was_home, minutes,
        goals_scored, assists, clean_sheets, goals_conceded, own_goals, penalties_saved,
        penalties_missed, yellow_cards, red_cards, saves, bonus, bps, influence,
        creativity, threat, ict_index, expected_goals, expected_assists,
        expected_goal_involvements, expected_goals_conceded, value, total_points, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(player_id, fixture_id) DO UPDATE SET
        round=excluded.round,
        kickoff_time=excluded.kickoff_time,
        opponent_team=excluded.opponent_team,
        was_home=excluded.was_home,
        minutes=excluded.minutes,
        goals_scored=excluded.goals_scored,
        assists=excluded.assists,
        clean_sheets=excluded.clean_sheets,
        goals_conceded=excluded.goals_conceded,
        own_goals=excluded.own_goals,
        penalties_saved=excluded.penalties_saved,
        penalties_missed=excluded.penalties_missed,
        yellow_cards=excluded.yellow_cards,
        red_cards=excluded.red_cards,
        saves=excluded.saves,
        bonus=excluded.bonus,
        bps=excluded.bps,
        influence=excluded.influence,
        creativity=excluded.creativity,
        threat=excluded.threat,
        ict_index=excluded.ict_index,
        expected_goals=excluded.expected_goals,
        expected_assists=excluded.expected_assists,
        expected_goal_involvements=excluded.expected_goal_involvements,
        expected_goals_conceded=excluded.expected_goals_conceded,
        value=excluded.value,
        total_points=excluded.total_points,
        updated_at=excluded.updated_at
    """
    price_sql = """
    INSERT INTO prices (
        player_id, fixture_id, round, kickoff_time, price, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(player_id, fixture_id) DO UPDATE SET
        round=excluded.round,
        kickoff_time=excluded.kickoff_time,
        price=excluded.price,
        updated_at=excluded.updated_at
    """

    stat_rows: List[Tuple[Any, ...]] = []
    price_rows: List[Tuple[Any, ...]] = []
    for player_id, history in histories.items():
        for row in history:
            fixture_id = parse_int(row.get("fixture"))
            if fixture_id <= 0:
                continue
            round_no = parse_int(row.get("round"))
            kickoff_time = str(row.get("kickoff_time") or "")
            value = parse_int(row.get("value"))
            stat_rows.append(
                (
                    player_id,
                    fixture_id,
                    round_no,
                    kickoff_time,
                    parse_int(row.get("opponent_team")),
                    1 if row.get("was_home") else 0,
                    parse_int(row.get("minutes")),
                    parse_int(row.get("goals_scored")),
                    parse_int(row.get("assists")),
                    parse_int(row.get("clean_sheets")),
                    parse_int(row.get("goals_conceded")),
                    parse_int(row.get("own_goals")),
                    parse_int(row.get("penalties_saved")),
                    parse_int(row.get("penalties_missed")),
                    parse_int(row.get("yellow_cards")),
                    parse_int(row.get("red_cards")),
                    parse_int(row.get("saves")),
                    parse_int(row.get("bonus")),
                    parse_int(row.get("bps")),
                    parse_float(row.get("influence")),
                    parse_float(row.get("creativity")),
                    parse_float(row.get("threat")),
                    parse_float(row.get("ict_index")),
                    parse_float(row.get("expected_goals")),
                    parse_float(row.get("expected_assists")),
                    parse_float(row.get("expected_goal_involvements")),
                    parse_float(row.get("expected_goals_conceded")),
                    value,
                    parse_int(row.get("total_points")),
                    now,
                )
            )
            if value > 0:
                price_rows.append(
                    (
                        player_id,
                        fixture_id,
                        round_no,
                        kickoff_time,
                        value / 10.0,
                        now,
                    )
                )

    if stat_rows:
        conn.executemany(stat_sql, stat_rows)
    if price_rows:
        conn.executemany(price_sql, price_rows)
    conn.commit()
    return len(stat_rows), len(price_rows)


def get_current_event_id(events: List[Dict[str, Any]]) -> int:
    for event in events:
        if event.get("is_current"):
            return parse_int(event.get("id"))
    for event in events:
        if event.get("is_next"):
            return parse_int(event.get("id"))
    ids = [parse_int(event.get("id")) for event in events if parse_int(event.get("id")) > 0]
    return max(ids) if ids else 0


def fetch_user_squad(
    entry_id: int,
    current_event: int,
    retries: int,
    retry_backoff_s: float,
    cookie: Optional[str],
) -> Dict[str, Any]:
    if cookie:
        my_team = fetch_json(
            f"{FPL_BASE}/my-team/{entry_id}/",
            retries=retries,
            retry_backoff_s=retry_backoff_s,
            cookie=cookie,
        )
        transfers = my_team.get("transfers", {}) if isinstance(my_team, dict) else {}
        return {
            "entry_id": entry_id,
            "event": current_event if current_event > 0 else None,
            "bank": parse_float(transfers.get("bank")) / 10.0,
            "squad_value": parse_float(transfers.get("value")) / 10.0,
            "free_transfers": parse_int(transfers.get("limit")),
            "active_chip": str(my_team.get("active_chip") or ""),
            "source": "my-team",
            "picks": my_team.get("picks", []),
        }

    if current_event <= 0:
        raise ValueError("Cannot fetch public picks without a valid current event id")
    public_picks = fetch_json(
        f"{FPL_BASE}/entry/{entry_id}/event/{current_event}/picks/",
        retries=retries,
        retry_backoff_s=retry_backoff_s,
    )
    entry_history = public_picks.get("entry_history", {}) if isinstance(public_picks, dict) else {}
    return {
        "entry_id": entry_id,
        "event": current_event,
        "bank": parse_float(entry_history.get("bank")) / 10.0,
        "squad_value": parse_float(entry_history.get("value")) / 10.0,
        "free_transfers": None,
        "active_chip": "",
        "source": "entry-picks",
        "picks": public_picks.get("picks", []),
    }


def insert_user_snapshot(conn: sqlite3.Connection, squad: Dict[str, Any]) -> Tuple[int, int]:
    insert_snapshot = """
    INSERT INTO user_squad_snapshot (
        entry_id, event, bank, squad_value, free_transfers, active_chip, source, captured_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    insert_pick = """
    INSERT INTO user_squad_picks (
        snapshot_id, player_id, position, multiplier, is_captain, is_vice_captain, purchase_price, selling_price
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    now = utc_now_iso()
    cursor = conn.execute(
        insert_snapshot,
        (
            parse_int(squad.get("entry_id")),
            parse_int(squad.get("event")),
            parse_float(squad.get("bank")),
            parse_float(squad.get("squad_value")),
            squad.get("free_transfers"),
            str(squad.get("active_chip") or ""),
            str(squad.get("source") or ""),
            now,
        ),
    )
    snapshot_id = int(cursor.lastrowid)
    pick_rows = []
    for pick in squad.get("picks", []):
        pick_rows.append(
            (
                snapshot_id,
                parse_int(pick.get("element")),
                parse_int(pick.get("position")),
                parse_int(pick.get("multiplier")),
                1 if pick.get("is_captain") else 0,
                1 if pick.get("is_vice_captain") else 0,
                parse_float(pick.get("purchase_price")) / 10.0 if pick.get("purchase_price") is not None else None,
                parse_float(pick.get("selling_price")) / 10.0 if pick.get("selling_price") is not None else None,
            )
        )
    if pick_rows:
        conn.executemany(insert_pick, pick_rows)
    conn.commit()
    return snapshot_id, len(pick_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest FPL API data into SQLite")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="SQLite database output path")
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help="Directory for caching element-summary responses",
    )
    parser.add_argument("--refresh-cache", action="store_true", help="Ignore cache and re-fetch element-summary")
    parser.add_argument("--workers", type=int, default=16, help="Parallel workers for element-summary fetch")
    parser.add_argument("--retries", type=int, default=2, help="HTTP retry count")
    parser.add_argument("--retry-backoff", type=float, default=0.5, help="Retry backoff base in seconds")
    parser.add_argument("--entry-id", type=int, default=0, help="Optional FPL entry id for squad snapshot")
    parser.add_argument(
        "--fpl-cookie",
        type=str,
        default="",
        help="Optional FPL Cookie header value for /my-team endpoint (or use FPL_COOKIE env)",
    )
    args = parser.parse_args()

    db_path = args.db
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    ensure_schema(conn)

    bootstrap = fetch_json(
        f"{FPL_BASE}/bootstrap-static/",
        retries=max(0, args.retries),
        retry_backoff_s=max(0.0, args.retry_backoff),
    )
    fixtures = fetch_json(
        f"{FPL_BASE}/fixtures/",
        retries=max(0, args.retries),
        retry_backoff_s=max(0.0, args.retry_backoff),
    )

    teams = bootstrap.get("teams", [])
    players = bootstrap.get("elements", [])
    events = bootstrap.get("events", [])
    element_types = bootstrap.get("element_types", [])
    position_names = {
        parse_int(position.get("id")): str(
            position.get("singular_name_short") or position.get("singular_name") or "UNK"
        )
        for position in element_types
    }

    team_count = upsert_teams(conn, teams)
    player_count = upsert_players(conn, players, position_names)
    injury_count = upsert_injuries_news(conn, players)
    fixture_count = upsert_fixtures(conn, fixtures if isinstance(fixtures, list) else [])

    player_ids = [parse_int(player.get("id")) for player in players if parse_int(player.get("id")) > 0]
    histories, history_stats = fetch_histories(
        player_ids=player_ids,
        cache_dir=args.cache_dir,
        refresh_cache=args.refresh_cache,
        retries=max(0, args.retries),
        retry_backoff_s=max(0.0, args.retry_backoff),
        workers=max(1, args.workers),
    )
    stat_rows, price_rows = upsert_gw_stats_and_prices(conn, histories)

    snapshot_msg = "user_squad_snapshot skipped"
    if args.entry_id > 0:
        cookie = args.fpl_cookie.strip() or os.environ.get("FPL_COOKIE", "").strip() or None
        try:
            squad = fetch_user_squad(
                entry_id=args.entry_id,
                current_event=get_current_event_id(events if isinstance(events, list) else []),
                retries=max(0, args.retries),
                retry_backoff_s=max(0.0, args.retry_backoff),
                cookie=cookie,
            )
            snapshot_id, picks_count = insert_user_snapshot(conn, squad)
            snapshot_msg = (
                f"user_squad_snapshot inserted: snapshot_id={snapshot_id}, "
                f"entry_id={args.entry_id}, picks={picks_count}, source={squad.get('source')}"
            )
        except Exception as exc:  # noqa: BLE001
            snapshot_msg = f"user_squad_snapshot failed for entry_id={args.entry_id}: {exc}"

    conn.close()

    print(f"DB updated: {db_path}")
    print(f"teams upserted: {team_count}")
    print(f"players upserted: {player_count}")
    print(f"injuries_news upserted: {injury_count}")
    print(f"fixtures upserted: {fixture_count}")
    print(f"gw_player_stats upserted: {stat_rows}")
    print(f"prices upserted: {price_rows}")
    print(
        "element-summary fetch stats: "
        f"api={history_stats['api_hits']}, cache={history_stats['cache_hits']}, errors={history_stats['errors']}"
    )
    print(snapshot_msg)


def _entrypoint() -> int:
    try:
        main()
        return 0
    except Exception as exc:
        print(f"Ingest failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(_entrypoint())
