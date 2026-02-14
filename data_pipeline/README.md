# Phase 1 - Data Pipeline (Foundation)

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
