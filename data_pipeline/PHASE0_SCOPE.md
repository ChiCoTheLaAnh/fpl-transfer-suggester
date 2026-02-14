# Phase 0 One-Pager: Product Scope and Correctness Criteria

## 1) Goal
Build an FPL transfer suggestion MVP that recommends actionable transfers for the **next gameweek**, while evaluating impact over a **3-gameweek horizon**.

Primary objective:
- maximize expected points gain versus a no-transfer baseline

Secondary objective:
- avoid low-quality hit-heavy plans unless expected upside is clearly positive

## 2) Decision-Complete Scope

### 2.1 Planning horizon
- Decision: optimize for **GW+1 only**, but score plans using **GW+1..GW+3 cumulative expected points**.
- Configurable later: horizon `k` in `[1..6]`.
- Baseline for comparison: "no transfer" plan for the same horizon.

### 2.2 Hits and chips
- Decision for MVP:
- include transfer hits in optimization and scoring (`-4` points per extra transfer above free transfers).
- allow plans up to **-8** max hit.
- chips (`WC`, `BB`, `TC`, `FH`) are **out of scope** for recommendation logic in MVP.
- if an active chip is detected for target GW, return "chip week not supported by MVP" status.

### 2.3 Output format
- Decision: return **Top 3 transfer plans**, not a full team optimizer UI output.
- Each plan must include:
- transfers in/out
- cost, remaining bank, hit cost
- projected points (baseline vs plan) for GW+1 and GW+1..GW+3
- net expected gain (`delta_after_hit`)
- risk summary
- short explanation (2-4 bullets)

## 3) Hard Constraints (must never be violated)
- valid FPL squad structure after transfers:
- 15 players total
- 2 GKP, 5 DEF, 5 MID, 3 FWD
- max 3 players per real-world team
- budget cannot go below zero (respect bank + sale values)
- transfer count <= allowed cap (for MVP run: max 3 transfers considered in search)
- hit cap: no plan below `-8`

## 4) Risk and Quality Definition

### 4.1 Risk score (MVP)
Risk is a normalized `0..1` value combining:
- minutes uncertainty proxy (recent minutes volatility)
- injury/news uncertainty flag
- rotation risk proxy

Higher score = higher risk.

### 4.2 Recommendation quality policy
- prefer lower risk when expected gains are similar:
- if two plans differ by < 1.0 expected points over horizon, choose lower-risk plan first

## 5) KPI / Backtest Targets (chosen)
Use these two KPIs as primary success criteria:

1. **Expected Points Lift vs Baseline**
- Metric: average `delta_after_hit` over evaluation windows
- Target for MVP acceptance: positive average lift (`> 0`)

2. **Hit Efficiency**
- Metric: proportion of hit plans where `delta_after_hit > 0`
- Report split:
- no-hit plans
- hit plans
- Target for MVP acceptance: hit plans should not dominate top-3 unless they beat no-hit alternatives by clear margin

Non-gating diagnostic metric:
- recommendation stability/risk distribution (mean and P75 risk score across top plans)

## 6) MVP Output Contract (for downstream consumers)
For each target GW and user squad snapshot, return:
- `plans[0..2]` sorted by quality score
- each plan fields:
- `transfers`: list of `{out_player_id, in_player_id}`
- `num_transfers`
- `hit_points`
- `bank_after`
- `ep_gw1`
- `ep_horizon`
- `delta_vs_baseline_gw1`
- `delta_vs_baseline_horizon`
- `delta_after_hit`
- `risk_score`
- `reasons[]`
- plus:
- `baseline_ep_gw1`
- `baseline_ep_horizon`
- `model_version`
- `data_timestamp_utc`

## 7) Out of Scope (explicit)
- chip strategy optimization (`WC/BB/TC/FH`)
- long-term strategic planning beyond 6 GW
- personalized manager behavior modeling
- advanced Monte Carlo simulation as release blocker

## 8) Acceptance Criteria for Phase 0 Sign-off
Phase 0 is complete when:
- this document is approved and treated as the single source of truth
- all decisions above are reflected in implementation tickets (data, modeling, optimization)
- every future PR for transfer logic references this scope and KPIs

## 9) Defaults to Implement Next
- target horizon: `3`
- max candidate output plans: `3`
- max hit allowed: `-8`
- tie-break tolerance for "similar gain": `1.0` expected points
- chips handling: "not supported in MVP" status path
