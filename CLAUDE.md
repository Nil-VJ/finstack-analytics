# CLAUDE.md — FinStack Analytics

Portfolio project by Niloufar Valinejad (Analytics Engineer). Stock market data warehouse:
Yahoo Finance → Python loader → BigQuery → dbt (staging → intermediate → marts) → MetricFlow semantic layer, with GitHub Actions Slim CI.

The goal is a **small, correct, honest** project a hiring manager can understand in 2 minutes. Prefer deleting over adding.

## Layout

```
scripts/load_yfinance.py        # Python loader: yfinance → BigQuery raw dataset
finstack_dbt/
  profiles.yml                  # committed; env_var() only, no secrets
  models/staging/               # stg_* views: rename, cast, dedupe. 1:1 with sources
  models/intermediate/          # int_* views: window functions, business logic
  models/marts/                 # fct_* / dim_* tables: what consumers query
  models/semantic/              # semantic models + metrics (MetricFlow)
  snapshots/                    # SCD-2 of company info
  macros/                       # pct_change only
  tests/                        # singular tests
  docs/                         # doc blocks + project overview
.github/workflows/
  dbt_slim_ci.yml               # PRs: state:modified+ with --defer against the prod manifest
  prod_refresh.yml              # manual only: optional load + prod build + manifest upload
```

## Environment

- Python 3.12, using the venv at `.venv`. Install with `pip install -r requirements.txt`, which pins exact versions.
- The GCP service account key lives at `credentials/service_account.json`. It is gitignored. Never print or commit it.
- Environment variables:
  - `DBT_TARGET`: `dev`, `ci` or `prod`; default `dev`
  - `DBT_KEYFILE`
  - `GCP_PROJECT_ID`: default `finstack-analytics`
  - `RAW_DATASET`: used by the loader; default `finstack_raw`

## Commands

Run dbt from inside `finstack_dbt/`:

```bash
dbt deps
dbt parse                                  # must show zero deprecation warnings
dbt build --target dev                     # models + snapshot + tests into finstack_dev
dbt build --target dev --vars '{raw_dataset: finstack_raw_dev}'   # build against the dev raw copy
mf validate-configs
mf query --metrics revenue,gross_margin_pct --group-by metric_time__quarter,company__sector
```

Loader, run from the repo root:

```bash
python scripts/load_yfinance.py --dry-run                       # fetch + validate, no writes
RAW_DATASET=finstack_raw_dev python scripts/load_yfinance.py    # real load into the DEV raw dataset
```

GitHub:

```bash
gh run list --limit 10
gh run view <id> --log-failed
gh pr create / gh pr view / gh pr checks
```

## SAFETY RULES (override everything else)

BigQuery datasets:

| Dataset | Allowed |
|---|---|
| `finstack_raw` | **read only** |
| `finstack_analytics`, `finstack_analytics_snapshots` | **read only** (prod) |
| `finstack_dev`, `finstack_dev_snapshots`, `finstack_raw_dev` | read/write (create if missing, same location as `finstack_raw`) |
| `finstack_ci*` | written **only** by the CI workflow, never locally |

- Never run `dbt` with `--target prod`.
- Never run the loader without `--dry-run` unless `RAW_DATASET=finstack_raw_dev`.
- Never delete or drop any BigQuery dataset, table or view, including old leftovers. List them for Niloufar instead.
- Never trigger `prod_refresh.yml`, which writes prod data.
- Git:
  - never push to `main`, never force-push, never merge PRs
  - never change repo settings or secrets
  - reading secret *names* with `gh secret list` is fine
- Never commit `credentials/`, keys, `.env`, or `SPEC.md`.

## dbt conventions

- dbt-core 1.11. **Check config and YAML syntax against docs.getdbt.com for the installed version before writing it. Do not rely on memory.** Older syntax (`tests:`, `target_schema` on snapshots, freshness at the top level) is wrong here.
- Layer rules:
  - staging only selects from `source()`
  - intermediate and marts only `ref()`
  - marts never reference sources
- Naming:
  - `stg_<entity>`, `int_<entity>_<what>`, `fct_<process>`, `dim_<entity>`
  - snake_case columns
  - units in names where useful (`_pct`, `_price`)
- SQL style:
  - lowercase keywords
  - one import CTE per `ref()`/`source()` at the top (`with x as (select * from {{ ref('x') }})`), then logic CTEs, then `select * from final`
  - compute each window function once per CTE, never copy-paste the same `lag()`
- Every model has a YAML entry with a description and a stated grain. Every mart has a uniqueness test on its grain.
- Tests use `data_tests:`. Generic tests come from dbt core or `dbt_utils` only. **No new packages.**
- Never average percentages across rows. Use ratio metrics (sum ÷ sum).

## Python conventions

- Keep the loader a single readable script: functions, `logging`, `argparse`. No classes, frameworks or test suites.
- Validate fetched data **before** any `WRITE_TRUNCATE`. A partial fetch must never overwrite a good table.
- Config comes from environment variables with defaults. Use `os.environ.setdefault`; never overwrite an existing environment variable.

## Working rules

- Scope: do what the task or `SPEC.md` asks. Do not add features, abstractions, packages or "nice to haves". If you think something is missing, write it under "Suggestions" in the PR description instead of building it.
- Verify before claiming done. Run the build or tests and report the actual output.
- If you could not verify something, say **UNVERIFIED** and include the exact error.
- **Honesty:** never invent numbers, results, row counts or performance claims in the README, comments or the PR. This is a portfolio piece, and every claim must be true and reproducible.
- Commits: small and descriptive. On multi-phase tasks, use one commit per phase (`phase N: ...`).
