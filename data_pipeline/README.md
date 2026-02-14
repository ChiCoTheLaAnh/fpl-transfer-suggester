# Phase 1 - Data Pipeline (Foundation)

## Planning docs
- Phase 0 scope one-pager (English): `data_pipeline/PHASE0_SCOPE.md`

## Phase 1 Ingest + Storage (SQLite MVP)
Muc tieu bo sung cho Phase 1:
- ingest du lieu FPL vao DB thay vi chi CSV snapshot
- dat nen cho transfer optimization (players, fixtures, GW stats, prices, user squad snapshot)

Script ingest:
```bash
python3 data_pipeline/ingest.py
```

Tu chon DB path:
```bash
python3 data_pipeline/ingest.py --db data/fpl_mvp.db
```

Refresh lai element-summary cache:
```bash
python3 data_pipeline/ingest.py --refresh-cache --workers 16 --retries 3 --retry-backoff 0.5
```

Ingest user squad snapshot (entry id):
```bash
python3 data_pipeline/ingest.py --entry-id 123456
```

Neu can endpoint `/my-team/{entry_id}`, truyen cookie:
```bash
python3 data_pipeline/ingest.py --entry-id 123456 --fpl-cookie "pl_profile=...; sessionid=..."
```
hoac set env var:
```bash
export FPL_COOKIE="pl_profile=...; sessionid=..."
python3 data_pipeline/ingest.py --entry-id 123456
```

SQLite tables duoc tao:
- `teams`
- `players`
- `fixtures`
- `gw_player_stats`
- `prices`
- `injuries_news`
- `user_squad_snapshot`
- `user_squad_picks`

Scheduler:
- GitHub Actions workflow: `.github/workflows/phase1-ingest-schedule.yml`
- Chay daily luc `03:00 UTC` + manual `workflow_dispatch`
- Neu setup secrets `FPL_ENTRY_ID` va `FPL_COOKIE`, workflow se ingest them user squad snapshot

## Muc tieu
Tao bang player feature voi schema:

| player | team | position | price | minutes_avg | goals | assists | xg90 | xa90 | next_opponent |

## Nguon du lieu
- FPL API `bootstrap-static` (player info, price, minutes, expected goals/assists)
- FPL API `fixtures` (upcoming fixture de lay `next_opponent`)
- FPL API `element-summary/{player_id}` (history de tinh `minutes_avg` last N)
- Understat EPL page (optional, de override `xg90/xa90` qua CSV)

## Chay pipeline
```bash
python3 data_pipeline/phase1_pipeline.py
```

Tu chon output path:
```bash
python3 data_pipeline/phase1_pipeline.py --output data/phase1_player_features.csv
```

Minutes trung binh gan day (mac dinh 5 tran):
```bash
python3 data_pipeline/phase1_pipeline.py --last-n 5
```

Chan nhieu xg90/xa90 voi player minutes thap:
```bash
python3 data_pipeline/phase1_pipeline.py --min-minutes-per90 180
```

Override xg90/xa90 tu file Understat local:
```bash
python3 data_pipeline/phase1_pipeline.py --understat-csv data/understat_xgxa.csv
```

Tao file Understat override:
```bash
python3 data_pipeline/fetch_understat_xgxa.py --season 2025 --output data/understat_xgxa.csv
```

Toc do va do on dinh khi fetch `element-summary`:
```bash
python3 data_pipeline/phase1_pipeline.py --workers 16 --retries 3 --retry-backoff 0.5
```

Dung cache (mac dinh: `data/cache/element_summary`), hoac refresh cache:
```bash
python3 data_pipeline/phase1_pipeline.py --cache-dir data/cache/element_summary
python3 data_pipeline/phase1_pipeline.py --refresh-cache
```

## Output
Mac dinh ghi ra file:
- `data/phase1_player_features.csv`

## Definition of Done (Phase 1)
Phase 1 duoc xem la "Done" khi dat du tat ca tieu chi sau:

1. Functional
- Lenh chay thanh cong (exit code = 0):
```bash
python3 data_pipeline/phase1_pipeline.py --understat-csv data/understat_xgxa.csv
```
- Co log thong ke cuoi pipeline:
  - `element-summary fetch stats: ...`
  - `override stats: ...`

2. Data contract (bat buoc)
- Tao duoc file `data/phase1_player_features.csv`
- Schema trung khop 100% (dung thu tu cot):
  - `player,team,position,price,minutes_avg,goals,assists,xg90,xa90,next_opponent`
- So dong output = so player trong FPL `bootstrap-static` (khong thieu player)
- Khong co gia tri rong o cac cot tren
- `next_opponent` khong co `TBD` trong run binh thuong

3. Data quality gates
- `element-summary` fetch errors = 0 trong run binh thuong
- Khi dung `--understat-csv`, `override_coverage_pct >= 40%`
- File `understat_xgxa.csv` co team mapping hop le (khong co unresolved team code > 3 ky tu)

4. Operational readiness
- Ho tro run nhanh bang cache (mac dinh `data/cache/element_summary`)
- Ho tro full refresh bang `--refresh-cache`
- Co tai lieu huong dan day du trong README de 1 nguoi moi co the tu chay duoc

## Refresh cache schedule (de xuat)
1. Hang ngay (nhanh, dung cache):
```bash
python3 data_pipeline/phase1_pipeline.py --understat-csv data/understat_xgxa.csv
```

2. Hang tuan (full refresh, khuyen nghi Thu Hai 03:00 local time):
```bash
python3 data_pipeline/fetch_understat_xgxa.py --season 2025 --output data/understat_xgxa.csv
python3 data_pipeline/phase1_pipeline.py --refresh-cache --workers 16 --retries 3 --retry-backoff 0.5 --understat-csv data/understat_xgxa.csv
```

3. Truoc han chot GW (neu can):
- Chay lai full refresh neu co bien dong lon ve fixture/chuyen nhuong/injury duoc cap nhat vao FPL API.

## Ghi chu
- `minutes_avg` = trung binh minutes cua `last N` tran gan nhat tu `element-summary/{player_id}` history
- `position` = map `elements.element_type` voi `element_types` (VD: GKP/DEF/MID/FWD)
- `goals`, `assists` = tong mua tu `bootstrap-static`
- `xg90` = `expected_goals / minutes * 90`, nhung se set `0` neu `minutes < min-minutes-per90`
- `xa90` = `expected_assists / minutes * 90`, nhung se set `0` neu `minutes < min-minutes-per90`
- `next_opponent` co dang `TEAM (H|A)`
- Neu truyen `--understat-csv`, cot `xg90/xa90` se duoc override theo cap `(player, team)` voi schema `player,team,xg90,xa90`

---

# Phase 2 - Ranking MVP

## Muc tieu
Xep hang cau thu outfield (DEF/MID/FWD) tu output Phase 1 bang weighted heuristic.

## Scope
- Co: ranking MVP, CLI, CSV + JSON metadata, deterministic output
- Khong: transfer optimization theo squad ca nhan, API service, backtest history
- GKP duoc loai khoi ranking Phase 2

## Chay ranking
Lenh mac dinh:
```bash
python3 data_pipeline/phase2_rank_players.py
```

Tu chon input/output:
```bash
python3 data_pipeline/phase2_rank_players.py \
  --input data/phase1_player_features.csv \
  --output-csv data/phase2_ranked_players.csv \
  --output-json data/phase2_ranked_players.meta.json
```

Tune ranking:
```bash
python3 data_pipeline/phase2_rank_players.py \
  --top-n-per-position 20 \
  --min-minutes-avg 30 \
  --positions DEF,MID,FWD \
  --w-attack 0.50 \
  --w-minutes 0.25 \
  --w-value 0.20 \
  --w-fixture 0.05
```

## Cong thuc scoring
Eligibility:
- `position` thuoc `DEF|MID|FWD`
- `minutes_avg >= min_minutes_avg`
- `xg90 + xa90 > 0`

Feature:
- `attack_raw = xg90 + 0.7 * xa90`
- `minutes_raw = clamp(minutes_avg / 90, 0, 1)`
- `value_raw = attack_raw / price` (neu `price <= 0` thi = 0)
- `fixture_home = 1` neu `next_opponent` ket thuc bang `(H)`, nguoc lai = 0

Normalize theo tung position:
- Min-max cho `attack_raw`, `minutes_raw`, `value_raw`
- Neu `max == min` thi set norm = `0.5`
- `fixture_norm = fixture_home`

Score:
- `score = w_attack*attack_norm + w_minutes*minutes_norm + w_value*value_norm + w_fixture*fixture_norm`
- `score` duoc round `6` chu so thap phan

Sort/tie-break trong tung position:
- `score DESC`
- `value_raw DESC`
- `price ASC`
- `player ASC`

## Output Phase 2
CSV mac dinh:
- `data/phase2_ranked_players.csv`
- Schema co dinh:
  - `player,team,position,price,next_opponent,minutes_avg,xg90,xa90,attack_raw,value_raw,fixture_home,score,rank_position`

JSON mac dinh:
- `data/phase2_ranked_players.meta.json`
- Top-level keys co dinh:
  - `schema_version`
  - `input_file`
  - `filters`
  - `weights`
  - `counts`

## Definition of Done (Phase 2)
1. Functional
- Lenh chay thanh cong:
```bash
python3 data_pipeline/phase2_rank_players.py
python3 data_pipeline/check_phase2_dod.py
```

2. Artifact contract
- Tao duoc ca 2 file: CSV + JSON metadata
- CSV schema exact-match
- JSON schema exact-match
- Khong co GKP trong output
- Moi position toi da `top-n-per-position` dong
- `rank_position` lien tuc tu 1 theo tung position

3. Deterministic
- Chay `phase2_rank_players.py` 2 lan lien tiep cung input
- SHA256 cua CSV va JSON trung nhau

4. CI
- Workflow: `.github/workflows/phase2-dod.yml`
- Chay `python3 data_pipeline/check_phase2_dod.py` moi push

---

# Phase 2 - Feature Engineering (player x GW)

## Muc tieu
Tao feature table theo tung `player x upcoming fixture (GW)` de phuc vu expected-points model va transfer optimization.

## Input
- SQLite DB tu ingest:
  - `data/fpl_mvp.db`
- Bang su dung:
  - `players`, `teams`, `fixtures`, `gw_player_stats`, `prices`

## Chay feature builder
Lenh mac dinh:
```bash
python3 data_pipeline/build_feature_table.py
```

Tuy chinh output va horizon:
```bash
python3 data_pipeline/build_feature_table.py \
  --db data/fpl_mvp.db \
  --output data/player_gw_features.parquet \
  --horizon 3
```

Neu can include ca fixture da ket thuc:
```bash
python3 data_pipeline/build_feature_table.py --include-finished
```

## Feature groups da co
1. Rolling form (truoc target fixture):
- `recent_points_avg_3`, `recent_points_avg_5`
- `recent_minutes_avg_3`, `recent_minutes_avg_5`
- `recent_xgi_avg_3`, `recent_xgi_avg_5`

2. Fixture context:
- `is_home`, `opponent_difficulty`
- `dgw_count_in_event`, `is_dgw`
- `rest_days`

3. Risk features:
- `minutes_volatility_5`
- `points_volatility_5`
- `benching_probability` (heuristic tu minutes gan day)
- `risk_score`

4. Multi-GW planning features:
- `horizon`
- `horizon_fixture_count_team`
- `horizon_avg_fixture_difficulty`
- `horizon_home_count`

## Output
Parquet mac dinh:
- `data/player_gw_features.parquet`

Schema co dinh:
- `player_id,player_name,team_id,team,position,fixture_id,target_event,kickoff_time,is_home,opponent_team_id,opponent_team,opponent_difficulty,dgw_count_in_event,is_dgw,rest_days,horizon,horizon_fixture_count_team,horizon_avg_fixture_difficulty,horizon_home_count,recent_points_avg_3,recent_points_avg_5,recent_minutes_avg_3,recent_minutes_avg_5,recent_xgi_avg_3,recent_xgi_avg_5,minutes_volatility_5,points_volatility_5,benching_probability,risk_score,last_price,current_status`

## Definition of Done (Phase 2 FE)
1. Functional
```bash
python3 data_pipeline/build_feature_table.py
python3 data_pipeline/check_phase2_features_dod.py
```

2. Data contract
- Output parquet ton tai va non-empty
- Schema exact-match
- Khong duplicate tren `(player_id, fixture_id)`
- `is_dgw` khop voi `dgw_count_in_event`
- `benching_probability` va `risk_score` trong [0, 1]

3. Deterministic
- Chay build 2 lan voi cung input -> SHA256 parquet khong doi

4. CI
- Workflow: `.github/workflows/phase2-features-dod.yml`
- CI chay ingest + check feature DoD moi push
