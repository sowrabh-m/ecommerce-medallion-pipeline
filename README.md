# Ecommerce Medallion Pipeline (Airflow-orchestrated)

An end-to-end batch data pipeline that lands synthetic ecommerce data in S3, loads it into Snowflake through a bronze/silver/gold medallion architecture, transforms it with dbt, and orchestrates the whole thing daily with Apache Airflow — sensor-triggered, retried on failure, with a pass/fail branch at the end.

## Status
✅ Core pipeline built and verified end-to-end (manual run against a historical date, `2026-01-15`).

## Architecture

```
Python generator ─▶ S3 (raw landing, dated folders)
                        │
                 Airflow S3KeySensor (per day)
                        │
                 SnowflakeHook COPY INTO ─▶ Snowflake BRONZE (raw tables)
                        │
                 dbt run/test ─▶ Snowflake SILVER (cleaned staging views)
                        │
                        └────────────▶ Snowflake GOLD (dim/fact/mart tables)
                        │
              pass ─▶ success_alert   fail ─▶ failure_alert
```

- **Bronze**: raw `customers`, `products`, `orders`, `order_items` tables, loaded as-is via `COPY INTO` from S3.
- **Silver**: `stg_*` dbt views — trimmed/lowercased/cast, one-to-one with bronze sources.
- **Gold**: `dim_customers`, `dim_products`, `fct_order_items` (grain: one row per order line, price snapshotted at order time), `mart_daily_revenue`.

## Stack
- **Source data**: synthetic ecommerce data (customers, products, daily order/order_item batches) generated with Python + Faker, deliberately landed as **dated daily files** to give Airflow's sensor something real to watch for.
- **Landing**: AWS S3, IAM role + trust policy scoped to one bucket.
- **Warehouse**: Snowflake — storage integration (no static AWS keys in Snowflake), dedicated warehouse/database/role with SYSADMIN/SECURITYADMIN RBAC split, three schemas (`BRONZE`/`SILVER`/`GOLD`).
- **Transformation**: dbt-core + dbt-snowflake.
- **Orchestration**: Apache Airflow via plain `docker-compose` (not Astro CLI — deliberately, to see the real webserver/scheduler/dag-processor/triggerer architecture), `LocalExecutor`, a custom Docker image with dbt installed in an isolated venv (kept separate from Airflow's own dependencies) plus the Amazon and Snowflake provider packages.

## Why this dataset
Fresh dataset for this project (not reusing earlier taxi/telecom data), deliberately shaped as daily incremental batches rather than one static dump — the point of this project is learning orchestration, and a sensor waiting for "today's file" only makes sense if data actually arrives incrementally.

## Setup

**AWS**: S3 bucket → IAM policy (S3 read/write scoped to the bucket) → IAM role with a placeholder trust policy → Snowflake `CREATE STORAGE INTEGRATION` → swap the placeholder trust policy for the real Snowflake IAM user ARN + external ID from `DESC STORAGE INTEGRATION`.

**Snowflake**: warehouse/database/3 schemas as SYSADMIN, dedicated role as SECURITYADMIN, grants wiring the role to the warehouse/database/schemas/storage integration, stage + file format in `BRONZE`.

**dbt**: `~/.dbt/profiles.yml` with key-pair auth (RSA key pair registered on the Snowflake user, reused across this account's projects — key-pair auth is per-user, not per-project).

**Airflow**: `docker-compose up` (official Apache compose file + a `docker-compose.override.yml` layering a custom-built image and volume mounts), two connections set up in the UI (`aws_default`, `snowflake_default` — key-pair auth, no password field), DAG in `airflow/dags/`.

Full step-by-step rebuild instructions, including every mistake hit along the way, are in [`docs/PROJECT3_ECOMMERCE_MEDALLION.md`](docs/PROJECT3_ECOMMERCE_MEDALLION.md) (English) and [`docs/PROJECT3_ECOMMERCE_MEDALLION_KANNADA.md`](docs/PROJECT3_ECOMMERCE_MEDALLION_KANNADA.md) (Kanglish).

## The DAG

`ecommerce_medallion_daily` — one task per stage, `@daily` schedule, `catchup=False`:

1. `wait_for_orders_file` / `wait_for_order_items_file` — `S3KeySensor`s polling for that day's raw files.
2. `load_to_bronze` — `PythonOperator` running `COPY INTO` for that day's files via `SnowflakeHook`.
3. `dbt_run` / `dbt_test` — `BashOperator`s invoking the isolated-venv `dbt` binary.
4. `success_alert` / `failure_alert` — the latter uses `trigger_rule="one_failed"` instead of a `BranchPythonOperator`, so it only fires on an actual `dbt_test` failure.

All tasks retry 3x with a 2-minute delay (`default_args`).

## Real infrastructure problems hit and fixed (not staged for this README)
- Trust policy written but never actually applied — caused an `AssumeRole` failure several steps later; root cause was assuming a described command had been run without confirming its output.
- dbt installed into Airflow's own Python environment broke Celery's CLI parsing (dependency conflict) — fixed by isolating dbt into its own venv inside the image.
- A separate, unrelated Celery/Airflow 3.0.2 bug crashed the worker regardless — solved by switching to `LocalExecutor`, which is also the right choice for a single-machine deployment like this one.
- Custom Docker builds were silently not replacing the running image for several attempts — fixed by giving the custom image an explicit, unambiguous tag instead of relying on Compose to overwrite the pulled image's tag.
- A generated Airflow secret config file (`airflow.cfg`) and dbt's build artifacts (`target/`, `logs/`) were accidentally committed due to `.gitignore` path mismatches — caught and cleaned up before calling the project done.

Full detail on each of these, with exact commands and root-cause analysis, is in the docs linked above.

## What I'd do differently at scale
- **`CeleryExecutor`/`KubernetesExecutor`** instead of `LocalExecutor`, once running on more than one machine.
- **Snowpipe** (event-driven, near-real-time) instead of batch `COPY INTO` on a daily sensor, if latency mattered.
- **Real alerting** (Slack/PagerDuty webhook) in place of the placeholder `EmptyOperator`s.
- **Incremental dbt models** for `fct_order_items` instead of full-table rebuilds, once data volume grew past a trivial size.
- **Terraform** for the AWS/Snowflake Phase 0 setup instead of hand-run CLI commands, for a team environment where this needs to be reproducible by more than one person.
