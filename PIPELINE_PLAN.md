# Scraper Pipeline: Audit Findings and Redesign Plan

_Written 2026-10-05. Everything here comes from read-only checks of S3, CloudWatch Logs and the code; nothing has been changed yet._

## TL;DR

- **About 20% of tournaments are missing from the prod S3 data.** 66 of 182 months are missing whole chunks: ~65k of 326k tournaments. Every month still has a `validation_report.json`, so the orchestrator treated them as complete.
- **2022–2024 is the worst-hit period** (71%, 62% and 57% coverage). That is exactly the evaluation window in FINDINGS.md, so those Glicko-2 results are **provisional**.
- **The data we do have is accurate.** Re-parsing stored raw HTML reproduces the stored games exactly, so only the gaps need scraping.
- **FIDE blocks per IP, and our own bursty traffic and retries caused the blocks.** The fix is a slow, steady, single-worker pipeline that stops at the first block, not more retries or more parallelism.
- **Remaining work:** ~128.5k requests. At ~0.2 req/s that is about 7.4 days of continuous scraping and roughly $5 of Lambda time.

---

## 1. Coverage audit (S3)

Method: for each month, compare `data/tournament_ids.txt` with every details and reports chunk file in S3, whatever `_of_N` chunk count it was written under.

| | Tournaments |
|---|---|
| In ID lists (182 months) | 326,369 |
| In merged `tournament_details.parquet` | 261,474 (80.1%) |
| Reports done using only the merged files | 260,314 |
| Reports done using the **union of all chunk sets** | 269,312 (~9k recovered with no scraping) |
| Still needed: details | 53,888 |
| Still needed: reports | 57,057 |
| Months fully complete | 115 / 182 |

Coverage by year, from merged reports:

```
2006:72% 2007:89% 2008:60% 2009:96% 2010:96% 2011:92% 2012:97% 2013:92% 2014:89%
2015:100% 2016:97% 2017:80% 2018:89% 2019:80% 2020:97% 2021:83% 2022:71% 2023:62% 2024:57%
```

The worst months are 2024-01 (168/3692), 2023-11 (513/3585), 2023-12, 2024-06 and 2022-07. 2006-10 and 2008-10 have no merged output at all.

**Expected gaps, not bugs:** 2006-01/04/07 have zero games because FIDE serves `report_updated_or_replaced` (no cross table) for those tournaments.

### How the chunks were lost (silent-failure chain)

1. A DetailsChunk Lambda times out or aborts. The `Catch` sends it to `ChunkFailed`, a Pass state that ends normally, so that chunk's **ReportsChunk never runs**.
2. Reports Lambdas on a blocked IP spend 15s on every tournament (`Skipping tournament ... timeout`). They reach the 900s timeout having written nothing.
3. `merge_chunks.py` merges **whatever chunk files exist** and never checks against `chunk_count`.
4. Merge, validate, federations and player_list handlers **return `success: false` instead of raising**, which Step Functions treats as success.
5. `validation_report.json` is written anyway, and the orchestrator marks the month done for good.

### Accuracy checks on the data we have

- **Parser:** stored raw HTML for 10 random chunks (2010–2023, 338,579 games) was re-parsed with the current `parse_report_html`. The output matches the stored games parquet exactly: same scores, dates and forfeits, nothing missing or extra, no duplicate keys.
- **Null `round_date` (10.4% of games, all before 2016):** FIDE's source pages have no dates for these tournaments; for example, 100 of 247 tournaments in one 2010 chunk have none. `build_glicko_input` fills them with the period month, which is correct for monthly periods.
- **Small cleanups for the rebuild:**
  - duplicate details rows (15 months), left by retry passes appending both a failed and a successful row
  - 2 games where a player plays themselves
  - 14 tournaments skipped on timeouts that have no data
- **Not yet verified: whether the ID lists are complete.** The tournaments step can write a partial list on failure; this was reproduced locally. Refetching every month's list (17.5k requests) settles it and also picks up tournaments FIDE registered late.

---

## 2. What FIDE's throttling looks like (CloudWatch, Mar–May 2026)

| Signal | Count |
|---|---|
| Connect timeouts (packets silently dropped) | 12,259 |
| Connection refused (Errno 111) | 438 |
| Read timeouts | 278 |
| HTTP 502 | 215 |
| HTTP 429 | 0 |

- **It's a per-IP firewall block, not an app-level rate limit.**
- **96% of blocked containers failed on their very first request** (211/223 details, 362/369 reports). The IP was already blocked when the container started.
- **A blocked container never recovers** (6 of 558), while fresh containers kept working during the same episodes. In the 2026-05-24 episode, 45 containers were blocked while 211 others saved data, with no overlap.
- **The blocked share climbed throughout the backfill and didn't clear after a 3-week pause:**
  ```
  W11 2.9% | W12 15.9% | W13 13.3% | W14 22.0% | (W15–17 idle) | W18 28.5% | W19 41.2% | W20 47.4% | W21 44.1% | W22 33.0%
  ```
  This is the signature of **our own traffic building up blocks that last weeks** across the shared Lambda IP pool. 1,953 of 5,988 chunk runs (33%) were wasted on IPs that were already blocked.
- **Why:** bursts. Each run fanned out 5 chunks in parallel × 2 endpoints, plus Map retries, plus blocked containers retrying every 15s for 15 minutes. Spacing runs out (3 a day) didn't help, because the blocks outlast the gap.
- **A local single-IP test** (details + reports interleaved, 0.66 req/s total, ~9 min) saw no throttling. A full missing 2024-04 chunk re-scraped cleanly: 173/173 tournaments, 14,444 games.
- **There is no official FIDE API or bulk export** for tournament reports or crosstables; only rating lists are downloadable. Asking FIDE for allowlisting or an export is worth trying. Rotating IPs to get around the blocks is **not** the plan.

Lambda memory, for sizing: details peaked at 279 MB (512 allocated). Reports averaged 328 MB but **hit its 1024 MB limit at least once**, because raw HTML for the whole chunk is held in memory.

---

## 3. Fixes, ranked by impact

| # | Fix | Impact |
|---|---|---|
| 1 | **Fail loudly instead of dropping chunks.** Remove the `ChunkFailed` Catch; merge requires every chunk/ID; handlers raise; validation report written only when the month is complete. | Stops the silent loss behind all 66 months. |
| 2 | **Stop on the first block, then wait using Step Functions.** On a connect timeout or refused connection: wait 30s and retry once, then save progress and raise `FideBlocked`. Step Functions retries after 30 min → 1h → 2h, up to ~12h. No 15s-per-tournament grinding. | Removes the main cause of lost chunks and wasted Lambda time; stops making the blocks worse. |
| 3 | **Resumable, time-budgeted workers.** Append part files as work progresses; save and exit with ~60s left. | Lambda timeouts can no longer lose data; retries never redo finished work. |
| 4 | **Offline rebuild, then rescrape the gaps, then rerun Glicko.** | Recovers ~9k tournaments for free, then ~57k by scraping. |
| 5 | **Stop swallowing write errors** (`save_*_parquet` log and continue). | No chunk marked done while its output is missing. |
| 6 | **Tournaments step: no partial `tournament_ids.txt`.** Write it only when every federation succeeded; track per-federation progress. | Prevents a month starting from a partial ID list. |
| 7 | **Never skip a tournament because of a network error.** Only page-structure cases (`updated_or_replaced`, no cross table) count as skips. Connection refused means blocked, not permanent. | Fixes the skipped and unretried tournaments inside chunks that did complete. |
| 8 | **Data cleanups:** remove duplicate details rows; stop counting unknown or empty time controls as `"S"`; fix `rstrip(".parquet")`; code default `chunk_size` 300 → 175 (300 exceeds 900s at 0.33 req/s). | Correctness of details data and the S-only Glicko filter. |
| 9 | **Cost and speed:** honest User-Agent; worker memory around 512 MB; `validate_pipeline` per-tournament filter → `groupby`; upload the player list zip as-is instead of re-gzipping ~700 MB of XML; cache the boto3 client. | Cheaper and faster; no correctness effect. |

---

## 4. Redesign: one gentle worker instead of a fan-out

Goal: a slow, steady trickle from one Lambda at a time. It's fine to be 2–3× slower than the aggressive estimate.

```
EnsureRunName → RefreshTournamentLists → BuildQueue
   → Worker ⟲ (Choice: remaining > 0 → Worker)
   → Merge → Validate
```

1. **One worker, no Map fan-out.** Each Worker call runs ~13 min, saves progress, and returns `{"remaining": N}`. A Choice loops back straight away. Container reuse keeps the outbound IP stable.
2. **One global rate limit shared by the endpoints:** ~0.2 req/s total, taking turns between details and reports (tested safe locally at 0.66). Set in SSM so it can be tuned without a redeploy.
3. **The queue is built from S3:** per month, the IDs in the list minus every tournament already stored in any chunk or part file. The worker appends `parts/{details,reports}_<ts>.parquet`. Idempotent: restarts never redo work and there's no `chunk_count` to get wrong. Tournament list refetches go through the same rate-limited worker.
4. **Stop on the first block** (fix #2). `FideBlocked` is retried by Step Functions with long intervals; waiting costs nothing. If blocking continues past ~12h, the execution fails visibly.
5. **"Done" means complete.** Merge runs only when a month's queue is empty: the union of all parts and chunks, duplicates removed. Validate checks coverage against the ID list before writing `validation_report.json`.
6. **Run one execution at a time.** The orchestrator already enforces this.
7. **If blocks still happen at ~0.2 req/s:** put the worker behind a NAT gateway with an Elastic IP (one stable, owned IP; ~$1/day while running) and/or lower the rate further. Moving scraping to a small EC2 instance with an Elastic IP is the alternative; it also removes the 15-minute limit.

### Estimates at ~0.2 req/s

| | |
|---|---|
| Tournament lists (all 182 months) | 17.5k requests |
| Details + reports for missing tournaments | 111k requests |
| **Total** | **128.5k requests ≈ 7.4 days** of continuous running |
| Lambda cost | ~180 h × 0.5 GB ≈ $5 (mostly time spent sleeping between requests) |
| Step Functions | ~850 Worker loops, a few cents |
| Monthly run afterwards (~4k tournaments) | ~11 h |

---

## 5. Order of work

1. **Pipeline correctness + gentle worker** (fixes 1–3, 5–8, the redesign) on a separate branch, e.g. `fix/gentle-pipeline`.
2. **Offline rebuild, no FIDE requests:** per month, merge the union of all chunk sets, remove duplicates, rewrite merged files and validation reports. Recovers ~9k tournaments.
3. **Refetch tournament lists, then scrape the ~57k missing tournaments** with the gentle worker (~7 days).
4. **Rerun `build_glicko_input.py` and the Rust evaluation/tuning; update FINDINGS.md.** The tuned `vol=0.09` result in particular may partly reflect missing games, which look like inactivity to Glicko-2.

Optional: email FIDE's ratings administration to describe the project and ask about allowlisting or a bulk export.

---

## Run log

- **2026-11-09, first deployed test (branch `fix/gentle-pipeline`, commit 89ebda3):** prod 2024-04, execution `manual-2024-04-20261109053021`, existing ID list (no refresh), 0.3 req/s, one chunk at a time. Waiter job `runs` name `sfn-2024-04`. Check: execution SUCCEEDED; `prod/2024-04/reports/coverage_complete.json` exists; dashboard `fide-glicko-scrape` shows no blocks; coverage summary in validation_report.json (missing 0, given-up count). Before: 876 missing details, 2,047 missing reports.
- **2026-11-09, AWS blocked:** both Lambda attempts at 2024-04 hit connect timeouts on the first FIDE request (federations; then reports, again after the 30-minute retry). Execution aborted. Switched to `scripts/run_month_local.py` from the home IP.
- **2026-11-09, local run of prod 2024-04:** `run-exp local-2024-04 uv run scripts/run_month_local.py --month 2024-04`, log `runs/local-2024-04.log`. Check: final JSON shows missing_details/missing_reports 0, `prod/2024-04/reports/coverage_complete.json` exists, no "blocked" lines in the log.
- 2026-11-09 local-2024-04 (home IP, 0.3 req/s, 203 min): all details done, 3804/3805 reports. One block (chunk 14 reports; recovered after the 30-min wait) and one block signal recovered by the 30 s retry. Merge refused: the weak skip 365437 ("no data found") was never confirmed because run_chunk counted it as done. Fixed in cca6e1a. Rerun: `run-exp local-2024-04b uv run scripts/run_month_local.py --month 2024-04`; check merge+validate succeed and coverage_complete.json exists.
- 2026-11-09 local-2024-04b (10.7 min): **2024-04 complete.** 3805/3805 details and reports; 365437 returned data on the retry (the first "no data found" was transient, which justifies the 2-confirmation rule). Merged rows: 3,805 details, 187,951 players, 422,540 games. `coverage_complete.json` written. validation has_issues=True only from informational checks: 2,729 player_count_mismatches (known quirk), 80 players not in the player list, 47 date issues.
