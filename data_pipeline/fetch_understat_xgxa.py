#!/usr/bin/env python3
"""Fetch Understat EPL player xG/xA and export CSV for phase1 overrides.

Output columns include the required phase1 override schema:
player,team,xg90,xa90
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from urllib.request import Request, urlopen

UNDERSTAT_BASE = "https://understat.com"
FPL_BASE = "https://fantasy.premierleague.com/api"

TEAM_ALIASES = {
    "man city": "manchester city",
    "man utd": "manchester united",
    "manchester city": "man city",
    "manchester united": "man utd",
    "spurs": "tottenham hotspur",
    "tottenham": "spurs",
    "tottenham hotspur": "spurs",
    "wolves": "wolverhampton wanderers",
    "wolverhampton wanderers": "wolves",
    "nott'm forest": "nottingham forest",
    "nottingham forest": "nott'm forest",
    "newcastle": "newcastle united",
    "newcastle united": "newcastle",
    "brighton": "brighton and hove albion",
}


def normalize_text(text: str) -> str:
    return " ".join(text.strip().lower().split())


def parse_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def fetch_text(url: str, timeout: int = 30, xhr: bool = False) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "Fantasy-Suggestor-Understat-Fetch/1.0",
            "Accept": "text/html,application/json",
            **({"X-Requested-With": "XMLHttpRequest"} if xhr else {}),
        },
    )
    with urlopen(req, timeout=timeout) as response:
        raw = response.read()
        content_encoding = str(response.headers.get("Content-Encoding") or "").lower()
        if content_encoding == "gzip" or raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return raw.decode("utf-8")


def fetch_json(url: str, timeout: int = 30, xhr: bool = False) -> Any:
    return json.loads(fetch_text(url, timeout=timeout, xhr=xhr))


def infer_default_season_start_year() -> int:
    now = datetime.now(timezone.utc)
    return now.year if now.month >= 8 else now.year - 1


def load_understat_players(league: str, season: int) -> List[Dict[str, Any]]:
    data = fetch_json(f"{UNDERSTAT_BASE}/getLeagueData/{league}/{season}", xhr=True)
    if not isinstance(data, dict):
        raise ValueError("Understat response is not an object")
    players = data.get("players", [])
    if not isinstance(players, list):
        raise ValueError("Understat players payload is not a list")
    return players


def load_fpl_team_short_names() -> Dict[str, str]:
    bootstrap = fetch_json(f"{FPL_BASE}/bootstrap-static/")
    teams = bootstrap.get("teams", [])
    by_name: Dict[str, str] = {}
    for team in teams:
        short_name = str(team.get("short_name") or "").strip()
        full_name = str(team.get("name") or "").strip()
        if short_name:
            by_name[normalize_text(short_name)] = short_name
        if full_name and short_name:
            by_name[normalize_text(full_name)] = short_name
    return by_name


def map_understat_team_to_fpl_short(team_name: str, fpl_team_map: Dict[str, str]) -> str:
    norm = normalize_text(team_name)
    if norm in fpl_team_map:
        return fpl_team_map[norm]

    alias_norm = TEAM_ALIASES.get(norm)
    if alias_norm and alias_norm in fpl_team_map:
        return fpl_team_map[alias_norm]

    return team_name.strip()


def choose_team(understat_team_title: str) -> str:
    if not understat_team_title:
        return ""
    teams = [part.strip() for part in understat_team_title.split(",") if part.strip()]
    if not teams:
        return ""
    return teams[-1]


def build_rows(players: List[Dict[str, Any]], fpl_team_map: Dict[str, str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for player in players:
        player_name = str(player.get("player_name") or "").strip()
        team_understat = choose_team(str(player.get("team_title") or "").strip())
        team_fpl = map_understat_team_to_fpl_short(team_understat, fpl_team_map)

        minutes = parse_float(player.get("time"))
        xg = parse_float(player.get("xG"))
        xa = parse_float(player.get("xA"))
        xg90 = (xg / minutes * 90.0) if minutes > 0 else 0.0
        xa90 = (xa / minutes * 90.0) if minutes > 0 else 0.0

        rows.append(
            {
                "player": player_name,
                "team": team_fpl,
                "xg90": round(xg90, 3),
                "xa90": round(xa90, 3),
                "minutes_understat": int(minutes),
                "xg_total": round(xg, 3),
                "xa_total": round(xa, 3),
                "team_understat": team_understat,
            }
        )

    rows.sort(key=lambda row: (row["team"], row["player"]))
    return rows


def write_csv(rows: List[Dict[str, Any]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "player",
        "team",
        "xg90",
        "xa90",
        "minutes_understat",
        "xg_total",
        "xa_total",
        "team_understat",
    ]

    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Understat EPL xG/xA per90 and export CSV")
    parser.add_argument(
        "--season",
        type=int,
        default=infer_default_season_start_year(),
        help="Season start year for Understat EPL URL (e.g. 2025 for 2025/26)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/understat_xgxa.csv"),
        help="Output CSV path",
    )
    args = parser.parse_args()

    players = load_understat_players(league="EPL", season=args.season)
    fpl_team_map = load_fpl_team_short_names()
    rows = build_rows(players, fpl_team_map)
    write_csv(rows, args.output)

    unresolved_team_rows = sum(1 for row in rows if len(str(row["team"])) > 3)
    print(f"Wrote {len(rows)} rows -> {args.output}")
    print(f"Rows with unresolved team mapping: {unresolved_team_rows}")


if __name__ == "__main__":
    main()
