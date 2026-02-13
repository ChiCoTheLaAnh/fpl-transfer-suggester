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

## Ghi chu
- `minutes_avg` = trung binh minutes cua `last N` tran gan nhat tu `element-summary/{player_id}` history
- `position` = map `elements.element_type` voi `element_types` (VD: GKP/DEF/MID/FWD)
- `goals`, `assists` = tong mua tu `bootstrap-static`
- `xg90` = `expected_goals / minutes * 90`, nhung se set `0` neu `minutes < min-minutes-per90`
- `xa90` = `expected_assists / minutes * 90`, nhung se set `0` neu `minutes < min-minutes-per90`
- `next_opponent` co dang `TEAM (H|A)`
- Neu truyen `--understat-csv`, cot `xg90/xa90` se duoc override theo cap `(player, team)` voi schema `player,team,xg90,xa90`
