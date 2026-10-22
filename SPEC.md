# SPEC — FinStack overhaul (Phase 0 → 7)

Owner: Niloufar. Executor: Claude Code, unattended overnight. Review: next morning, via one PR.

## Goal

Turn `finstack-analytics` into a small, correct, fully working dbt + BigQuery portfolio project.
Both GitHub Actions workflows currently fail. When you are done:

- every workflow passes,
- every model is correct,
- every remaining feature earns its place on a CV,
- everything else is deleted.

**Smaller is better.** When in doubt, cut it. Do not add features that are not in this spec.

## How to work

- Read `CLAUDE.md` first. Its safety rules override anything in this spec.
- Work on branch `fix/finstack-overhaul`. Make **one commit per phase**, with message `phase N: <summary>`, so Niloufar can review phase by phase.
- Do not stop to ask questions. When something is ambiguous:
  - pick the simplest option consistent with this spec,
  - record it under "Decisions I made" in the PR description,
  - keep going.
- Stop only if continuing would break a safety rule in `CLAUDE.md`.
- If a step cannot be verified (Yahoo blocks you, a permission is missing):
  - finish the code anyway,
  - mark the step **UNVERIFIED** in the PR description, with the exact error,
  - move on.
- Do not commit `SPEC.md`.

## Decisions already made (do not revisit)

| Topic | Decision |
|---|---|
| dbt version | Upgrade to dbt-core **1.11.x** + the latest compatible `dbt-bigquery`. Exception: if `dbt-metricflow` cannot install alongside 1.11, use the highest dbt-core ≥1.10 it supports, and record why. |
| Python | 3.12 |
| Keep | staging → intermediate → marts, model contract on `fct_prices_daily`, SCD-2 snapshot, quarterly financials pipeline, MetricFlow semantic layer, Slim CI |
| Delete | seeds, groups/access, exposures, `static_analysis`, custom `positive_value` test, `dbt_expectations` (and its `dbt_date` dependency), unused metrics/measures, empty log file, nested README stub |
| Data loading | **Manual only**. No cron schedule. |
| `fct_prices_daily` | Plain `table`, not incremental (~38k rows). |
| Datasets | Raw stays in `finstack_raw`. Prod models go to `finstack_analytics`, dev to `finstack_dev`, CI to `finstack_ci`. Snapshots go to `<target dataset>_snapshots`. |
| Profiles | One committed `finstack_dbt/profiles.yml` using `env_var()` only. It must contain no secrets. |

---

## Phase 0 — Diagnose (read-only; nothing is written to BigQuery)

1. Read the failures:
   - `gh run list --limit 10`
   - for the most recent failure of **each** workflow: `gh run view <id> --log-failed`
   - Note the failing step and the root-cause error.
2. Check the secrets: `gh secret list`. Confirm `GCP_SA_JSON` exists. Never print its value.
3. Inspect BigQuery with `bq`, read-only:
   - Datasets that exist, and their **locations**: `finstack_raw` plus anything the old setup created, e.g. `finstack_raw_seeds`, `finstack_raw_ci`.
   - Row count and distinct tickers per raw table. The expected ticker count is 30.
   - Does any raw table look partially loaded (fewer than 30 tickers)?
   - NVDA rows around 2024-06-07 to 2024-06-11. Is the close price continuous (adjusted) or does it drop ~90% (unadjusted)?
4. Run the current project locally on the current pinned versions, against `--target dev`, and capture the errors.
5. Write the findings into the PR description, section **Phase 0 findings**: failing step and root cause per workflow, raw data health, dataset locations.

Gate: if the raw tables are partially loaded or broken, every later phase builds against
`finstack_raw_dev`, loaded in Phase 2 with `--vars '{raw_dataset: finstack_raw_dev}'`.
Never fix prod raw data.

## Phase 1 — Foundation and deletions

1. **`.gitignore`** (repo root): replace the bloated Python template with a short file. It must cover:
   - `.venv/`, `venv/`, `__pycache__/`, `*.pyc`
   - `credentials/`, `*.json` keys under `credentials/`, `.env`
   - `finstack_dbt/target/`, `finstack_dbt/dbt_packages/`, `finstack_dbt/logs/`, `logs/`
   - `.DS_Store`
   - **Remove the `*.txt` rule.**

   Delete `finstack_dbt/.gitignore` if the root file covers it.
2. **`requirements.txt`**: exact pins (`==`) for **top-level** packages only:
   - `dbt-core`, `dbt-bigquery`, `dbt-metricflow[bigquery]`
   - `yfinance`, `google-cloud-bigquery`, `pandas`, `db-dtypes`, `pyarrow`

   Install it in a fresh Python 3.12 venv and confirm `dbt --version` works. Remove the `protobuf<5` hack everywhere.
3. **`finstack_dbt/profiles.yml`**, committed:
   - top-level `target: "{{ env_var('DBT_TARGET', 'dev') }}"`
   - outputs `dev`, `ci`, `prod`: `type: bigquery`, `method: service-account`
   - `project: "{{ env_var('GCP_PROJECT_ID', 'finstack-analytics') }}"`
   - `keyfile: "{{ env_var('DBT_KEYFILE', '../credentials/service_account.json') }}"`
   - location: the location found in Phase 0 (expected `EU`)
   - `threads: 4`
   - datasets: `finstack_dev`, `finstack_ci` and `finstack_analytics` respectively
4. **`dbt_project.yml`**:
   - remove `+static_analysis`, all `+access`, and the `seeds:` block
   - keep the materializations: staging and intermediate as `view`, marts as `table`
5. **`packages.yml`**:
   - keep only `dbt-labs/dbt_utils`, with a bounded range (e.g. `[">=1.3.0", "<2.0.0"]`)
   - remove `dbt_expectations`
   - regenerate `package-lock.yml`
6. **Delete** these files:
   - `finstack_dbt/seeds/` (both CSVs)
   - `finstack_dbt/models/groups.yml`
   - `finstack_dbt/models/exposures.yml`
   - `finstack_dbt/macros/test_positive_value.sql`
   - `logs/query_log.sql`
   - `finstack_dbt/README.md`

   Also remove every `group:` / `access` / `contract`-unrelated governance config in the YAML files.
7. **dbt 1.11 syntax**: run `dbt parse` and fix **every** deprecation warning (e.g. `tests:` → `data_tests:`, generic test arguments). Check the exact syntax against docs.getdbt.com for the installed version; do not guess.

Acceptance: `dbt deps && dbt parse` succeed with zero deprecation warnings.

## Phase 2 — Loader (`scripts/load_yfinance.py`)

Keep it one readable script. No new framework and no class hierarchy.

1. **Config from the environment**:
   - `GCP_PROJECT_ID` (default `finstack-analytics`)
   - `RAW_DATASET` (default `finstack_raw`)
   - `GOOGLE_APPLICATION_CREDENTIALS` via `os.environ.setdefault(...)`, keeping the current `credentials/service_account.json` path as the default. Do not overwrite a value that is already set.
2. **CLI flags** (argparse):
   - `--dry-run`: fetch and validate, print a summary, write nothing.
   - `--only {prices,company,financials}`: optional.
3. **Rate-limit hardening**:
   - retry each Yahoo call up to 4 times with exponential backoff (e.g. 5s, 15s, 45s, 120s)
   - sleep ~1.5s between per-ticker `.info` and `quarterly_income_stmt` calls
   - use `logging`, not `print`
4. **Validate before writing.** Abort the whole table write with a non-zero exit code if:
   - fewer than 30 tickers came back for prices or company info, or
   - required columns are missing.

   A partial fetch must **never** truncate a good table.
5. **Prices**:
   - pass `auto_adjust=True` explicitly to `yf.download`
   - keep the 5-year period with `WRITE_TRUNCATE`
   - drop the dead `dividends` / `stock_splits` handling and the no-op rename
6. **Company info**: `WRITE_TRUNCATE`, but only after validation passes.
7. **Financials** must **accumulate history**:
   - load with `WRITE_APPEND`
   - every row carries `_loaded_at`
   - set an explicit BigQuery schema in `LoadJobConfig` so all-null columns cannot break the load

   Deduplication happens in staging (Phase 3).
8. Test it:
   - run `--dry-run`
   - then run one real load with `RAW_DATASET=finstack_raw_dev` (create that dataset in the same location if needed)
   - **Never run a real load against `finstack_raw`.**

Acceptance: the dry run passes, or is marked UNVERIFIED with the Yahoo error. The dev load writes all 3 tables with 30 tickers.

## Phase 3 — Models, snapshot, tests

Follow the SQL conventions in `CLAUDE.md`.

1. **Sources** (`_stg__sources.yml`):
   - `database: "{{ env_var('GCP_PROJECT_ID', 'finstack-analytics') }}"`
   - `schema: "{{ var('raw_dataset', 'finstack_raw') }}"`
   - freshness and `loaded_at_field` in the dbt 1.11 location (under `config:`)
   - `warn_after: 7 days`, **no** `error_after`, because loading is manual
   - remove the documented columns that no longer exist (`dividends`, `stock_splits`)
2. **Macro `pct_change`**:
   - add a `precision` argument (default 2)
   - use it in `int_prices_with_returns` (precision 4) and `int_financials_quarterly`
   - compute each `lag()` **once** in a CTE instead of repeating it
3. **`stg_financials`**: deduplicate to one row per `(ticker, quarter_date)`, keeping the latest `_loaded_at` (`qualify row_number() ...`).
4. **`int_financials_quarterly`**:
   - compute QoQ revenue growth only when the previous quarter is 80–100 days earlier; otherwise null
   - note in the YAML that gross margin is null for banks, because yfinance has no gross profit for them
5. **`int_prices_moving_averages`**: `sma_20` / `sma_50` must be null until the ticker has 20 / 50 rows of history.
6. **`fct_prices_daily`**:
   - `materialized='table'`
   - remove the incremental config
   - add `price_id = dbt_utils.generate_surrogate_key(['ticker','date'])`
   - keep the **enforced contract** and add `price_id` to it
   - reference the doc blocks `close_price_description` and `daily_return_description`
   - delete any doc block that is still unused afterwards
7. **`fct_returns`**:
   - grain is one row per ticker per month
   - columns: `ticker`, `month`, `trading_days`, `month_low` (min `low_price`), `month_high` (max `high_price`), `total_volume`, `monthly_return_pct`
   - `monthly_return_pct` = (last close of the month ÷ last close of the previous month − 1) × 100, rounded to 2 decimals; null for each ticker's first month
   - remove `avg_daily_return_pct`
8. **New mart `fct_financials_quarterly`**:
   - built from `int_financials_quarterly`
   - add `financials_id` (surrogate key of ticker + quarter_date)
   - document it in `_marts__models.yml`
9. **`dim_companies`** and **`dim_sectors`**: keep as they are, minus the `group` config. Sectors come from yfinance (7 values, including Communication Services); the docs must say that.
10. **Snapshot `snp_company_info`**:
    - use dbt 1.9+ snapshot config so the snapshot lands in `<target dataset>_snapshots` in every target. Verify this in dev; it must never be hardcoded to `finstack_raw`.
    - `check_cols`: `sector`, `industry`, `recommendation_key` only (`market_cap` changes daily and would turn SCD-2 into a price log)
    - add a YAML entry with descriptions
11. **Tests** (`data_tests`). Required set; do not add more than this:

    | Model | Tests |
    |---|---|
    | `stg_prices` | `dbt_utils.unique_combination_of_columns` on `(ticker, date)` |
    | `stg_financials` | unique on `(ticker, quarter_date)` |
    | `fct_prices_daily` | `price_id` unique + not_null; `close_price`, `volume` via `dbt_utils.accepted_range` (min 0); `ticker` relationships → `dim_companies` |
    | `fct_returns` | unique on `(ticker, month)` |
    | `fct_financials_quarterly` | `financials_id` unique + not_null; `ticker` relationships → `dim_companies` |
    | `dim_companies` | existing tests; `market_cap` via `dbt_utils.accepted_range` (min 0) |
    | `snp_company_info` | unique on `(ticker, dbt_valid_from)` |
    | singular test `assert_ohlc_consistency` | extend it: `low <= open <= high` and `low <= close <= high`, with a 1e-6 tolerance |

    If the OHLC test fails on real Yahoo data, set it to `severity: warn` and report the row count in the PR. Do not "fix" the data.
12. Update `_stg__models.yml`, `_int__models.yml`, `_marts__models.yml` and `docs/_overview.md` so every column and claim matches the SQL. Remove all mentions of seeds, incremental, groups and exposures.

Acceptance: `dbt build --target dev` passes (the warn-severity OHLC test is allowed), and the snapshot lands in `finstack_dev_snapshots`.

## Phase 4 — Semantic layer

Keep it small and correct. Use one folder, `models/semantic/`; merge the current `semantic_models/` and `metrics/` folders into it.

1. **Time spine**: configure `metricflow_time_spine` the dbt 1.9+ way (YAML `time_spine:` config with `standard_granularity_column: date_day`). Verify the syntax against the docs.
2. **Semantic models**. Each one gets a real primary entity:
   - `companies` on `dim_companies`:
     - primary entity `company` (expr `ticker`)
     - dimensions `sector`, `industry`
   - `prices` on `fct_prices_daily`:
     - primary entity `price` (expr `price_id`)
     - foreign entity `company` (expr `ticker`)
     - time dimension `date`
     - measures `total_volume` (sum), `avg_daily_return` (average of `daily_return_pct`)
   - `financials` on `fct_financials_quarterly`:
     - primary entity `financial_quarter` (expr `financials_id`)
     - foreign entity `company`
     - time dimension `quarter_date`
     - measures `total_revenue`, `gross_profit`, `net_income` (all sum)
3. **Metrics**. Exactly these five:
   - `trading_volume` (simple)
   - `avg_daily_return_pct` (simple)
   - `revenue` (simple)
   - `gross_margin_pct` (**ratio**: gross_profit / total_revenue)
   - `net_margin_pct` (**ratio**: net_income / total_revenue)

   Delete every other metric and measure.
4. Verify:
   - `dbt parse` is clean
   - `mf validate-configs` passes
   - run against dev:
     - `mf query --metrics revenue,gross_margin_pct --group-by metric_time__quarter,company__sector`
     - one prices query grouped by `company__sector`

   Paste the real outputs (truncated) into the PR description.

Acceptance: both `mf query` commands return rows. If `dbt-metricflow` cannot work with the chosen dbt version, mark this UNVERIFIED and explain why.

## Phase 5 — CI/CD

1. **Delete** `.github/workflows/dbt_daily.yml`.
2. **Create** `.github/workflows/prod_refresh.yml`:
   - trigger: `workflow_dispatch` only, with a boolean input `load_data` (default `true`)
   - steps, in order:
     1. checkout
     2. Python 3.12
     3. `pip install -r requirements.txt`
     4. write `secrets.GCP_SA_JSON` to `credentials/service_account.json`
     5. if `load_data`: run the loader
     6. `dbt deps`
     7. `dbt source freshness` (warn-only, no `|| true`)
     8. `dbt build --target prod`
     9. upload `target/manifest.json` as artifact `prod-manifest` with `retention-days: 90`
   - env: `DBT_TARGET=prod`, `DBT_KEYFILE`, `GCP_PROJECT_ID`
   - the `load_data=false` path exists because Yahoo often blocks GitHub runner IPs. Niloufar can then load locally and run the build only.
3. **Rewrite** `.github/workflows/dbt_slim_ci.yml`:
   - trigger on `pull_request` to `main`
   - Python 3.12, `pip install -r requirements.txt`, write the SA key, `DBT_TARGET=ci`, `dbt deps`
   - download `prod-manifest` from `prod_refresh.yml` on `main`; keep the existing missing-artifact fallback behaviour
   - if the manifest exists: `dbt build --select state:modified+ --defer --state ./prod-state --fail-fast`
   - otherwise: a full `dbt build --fail-fast`
   - remove the generated `profiles.yml` heredoc, since the committed profile is used
   - keep the explanatory comment about not setting `DBT_PROJECT_DIR`
4. Push the branch and open the PR. **CI must pass on the PR.** This first run is a full build into `finstack_ci`, because no prod manifest exists yet. Iterate until it is green.

Acceptance: the Slim CI run on the PR is green. Never trigger `prod_refresh.yml`; that writes prod data.

## Phase 6 — README

Rewrite the root `README.md`, for a hiring manager who spends 2 minutes on it. Model its tone on Niloufar's other repos (UpCloud billing, Wolt snack): plain, direct, and honest about trade-offs.

Sections:
1. **What it is**: 2–3 sentences. 30 S&P 500 tickers, 5 years of daily prices, quarterly fundamentals, dbt + BigQuery.
2. **Architecture**: a Mermaid diagram covering yfinance → loader → `finstack_raw` → staging → intermediate → marts/snapshot → semantic layer, plus the CI/CD flow.
3. **Data model**: one short line per mart, giving its grain.
4. **Design decisions** (short paragraphs):
   - adjusted prices, and why `fct_prices_daily` is a table rather than incremental (restated history; what would change at scale)
   - financials accumulate via append + dedupe, because the source only exposes recent quarters
   - SCD-2 snapshot and why `market_cap` is excluded
   - ratio metrics instead of averaged percentages
   - the contract on the main fact
   - vendor sector classification
5. **Data quality**: what is tested and why, in a few lines.
6. **CI/CD**: Slim CI with state deferral; the manual prod refresh and why it is manual (Yahoo rate limits).
7. **Run it locally**: venv, `pip install -r requirements.txt`, env vars, loader `--dry-run`, `dbt build`, one `mf query`.
8. **Limitations / at scale**: 3–5 honest bullets.

**Every number in the README must come from an actual run.** Do not invent results, row counts or performance claims.

## Phase 7 — Final check and PR description

1. In a fresh venv:
   - `pip install -r requirements.txt`
   - `dbt deps && dbt build --target dev`
   - `mf validate-configs`
2. `grep` the repo for leftover references to deleted things: `seeds`, `ticker_universe`, `exposure`, `group:`, `dbt_expectations`, `static_analysis`, `dbt_daily`, `finstack_raw_ci`. Fix any that remain.
3. Write the PR description with these sections:
   - **Summary**: 5 lines max.
   - **Phase 0 findings**.
   - **Per-phase changes**: bullets that map to the commits.
   - **Deleted**: the full list of removed files and features.
   - **Decisions I made**: anything not dictated by this spec.
   - **Verification**: the commands run and their results; anything UNVERIFIED, with its error.
   - **Manual steps for Niloufar after merge**:
     1. Run `prod_refresh` with `load_data=true`. If Yahoo blocks the runner, run the loader locally and then run `prod_refresh` with `load_data=false`.
     2. List the leftover BigQuery objects from the old layout (models inside `finstack_raw`, the old snapshot table, `finstack_raw_seeds`, `finstack_raw_ci`, `finstack_raw_dev` if created) for her to delete manually.
     3. Later, optionally: build a Looker Studio dashboard on the marts and add one exposure for it.

## Non-goals (do not do these)

Looker Studio, Workload Identity Federation, Airflow, Docker, dbt Cloud, a custom `generate_schema_name` macro, a Python test framework, new dbt packages, extra marts or metrics, per-PR CI datasets, refactoring outside this spec.

## Definition of done

- [ ] Slim CI is green on the PR.
- [ ] `dbt build --target dev` and `mf validate-configs` pass locally from a fresh venv.
- [ ] Zero dbt deprecation warnings.
- [ ] Nothing writes to `finstack_raw` or `finstack_analytics` during this work.
- [ ] Every deletion in Phase 1 is done, and grep finds no leftovers.
- [ ] The README is rewritten, and every number in it is real.
- [ ] The PR description is complete, including UNVERIFIED items and manual steps.
