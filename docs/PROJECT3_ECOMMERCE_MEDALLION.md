# Project 3 — Ecommerce Medallion Pipeline (Airflow-orchestrated)

One doc — decisions, Phase 0 infra build, the real mistake hit, and a rebuild checklist — added as we go, same pattern as Projects 1 & 2.

---

## Goal

New concept for this project: **Apache Airflow orchestration** (plain `docker-compose.yaml`, not Astro CLI — deliberately, to see the real architecture: webserver / scheduler / triggerer / metadata DB). Fresh dataset — ecommerce orders/order_items/customers/products — built into a **bronze → silver → gold medallion** layout in Snowflake, with dbt doing the transforms and Airflow driving the DAG (sensor → extract → load → dbt run/test → branch → alert, with retries).

---

## Decision log

### Decision 1 — Fresh dataset, not wrapping Projects 1/2
Every project in the 10-project roadmap gets its own dataset built from scratch, for maximum hands-on repetition, rather than reusing/wrapping earlier projects' data or infra. Project 3 = ecommerce, deliberately not telecom/taxi again.

### Decision 2 — Airflow via plain `docker-compose.yaml`
Not Astro CLI. Astro CLI hides the actual Airflow architecture behind its own tooling — since this is the first Airflow project, the goal is to understand webserver/scheduler/triggerer/metadata-DB wiring directly, not through an abstraction layer.

### Decision 3 — Naming convention: drop "portfolio" / "sowrabh"
Starting this project, all new resource names (S3 buckets, IAM roles/policies, Snowflake objects) use a `de-` / `DE_` prefix — no `sowrabh-` or `portfolio` anywhere. Reasoning: these projects are being built toward production-grade depth, not treated as a portfolio checkbox. Projects 1 & 2's existing resources keep their old names for now — they'll be torn down and rebuilt under this convention later, once all 10 concepts are learned.

### Decision 4 — AWS via CLI this project, Console from Project 4 onward
Resume already claims solid AWS hands-on experience (originally built via Console), but CLI syntax specifically has gone rusty. Project 3 is used to relearn CLI (it's "fixed syntax," easy to drill). From Project 4 onward, AWS work switches back to Console — clicking through IAM role/trust-policy/permissions-policy relationships reinforces the mental model better than reading CLI JSON.

---

## Repo setup

```bash
cd Projects/ecommerce-medallion-pipeline
git init
# .gitignore written (Python/.venv, secrets, dbt target/logs, Airflow logs/cfg/db, data/raw/*)
git add .gitignore
git commit -m "Initial commit: add .gitignore"

gh repo create sowrabh-m/ecommerce-medallion-pipeline --private --source=. --remote=origin
git push -u origin master
```

Repo: `github.com/sowrabh-m/ecommerce-medallion-pipeline` (private).

---

## Phase 0 — AWS infra (via CLI)

### Step 1 — S3 bucket

```bash
aws s3api create-bucket --bucket de-ecommerce-medallion --region us-east-1
```

### Step 2 — IAM policy (what access is allowed)

`infra/aws/s3-policy.json`:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:GetObjectVersion"],
            "Resource": "arn:aws:s3:::de-ecommerce-medallion/*"
        },
        {
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
            "Resource": "arn:aws:s3:::de-ecommerce-medallion"
        }
    ]
}
```

Two statements: object-level actions (`Get/PutObject`) need the `/*` ARN suffix since they act on things *inside* the bucket; bucket-level actions (`ListBucket`, `GetBucketLocation`) need the bare bucket ARN since they act on the bucket itself.

```bash
aws iam create-policy \
  --policy-name de-ecommerce-medallion-s3-policy \
  --policy-document file://infra/aws/s3-policy.json
```
→ returns `Arn`, needed below.

### Step 3 — IAM role (who is allowed to use that access)

Chicken-and-egg problem: the *real* trust policy needs Snowflake's IAM user ARN + external ID, but those don't exist until the storage integration is created in Snowflake — which itself needs the role's ARN. Fix: create the role with a **placeholder** trust policy first (trusting your own account), then swap in the real values after.

Placeholder (`<account_id>` = your AWS account ID):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::<account_id>:root" },
      "Action": "sts:AssumeRole"
    }
  ]
}
```
`:root` on a `Principal.AWS` ARN means "the whole account," not the root user specifically.

```bash
aws iam create-role \
  --role-name de-ecommerce-medallion-role \
  --assume-role-policy-document file:///tmp/de-ecommerce-trust-placeholder.json

aws iam attach-role-policy \
  --role-name de-ecommerce-medallion-role \
  --policy-arn arn:aws:iam::<account_id>:policy/de-ecommerce-medallion-s3-policy
```

---

## Phase 0 — Snowflake side

### Step 4 — storage integration

Run as ACCOUNTADMIN (a storage integration reaches outside Snowflake into another cloud account, so `CREATE INTEGRATION` is locked to the highest-privilege role by default):

```sql
CREATE STORAGE INTEGRATION de_ecommerce_medallion_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account_id>:role/de-ecommerce-medallion-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://de-ecommerce-medallion/');

DESC STORAGE INTEGRATION de_ecommerce_medallion_int;
```

`DESC` returns `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` — the real values needed to lock down the AWS trust policy.

### Step 5 — real trust policy

`infra/aws/trust-policy.json` (real values, safe to commit since the repo is private):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "<STORAGE_AWS_IAM_USER_ARN from DESC>" },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": { "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID from DESC>" }
      }
    }
  ]
}
```

The `Condition` matters: without it, anyone holding that Snowflake IAM user's credentials (shared across many Snowflake customer accounts) could assume the role. The external ID is what proves it's specifically *this* Snowflake account's integration.

```bash
aws iam update-assume-role-policy \
  --role-name de-ecommerce-medallion-role \
  --policy-document file://infra/aws/trust-policy.json
```

**⚠️ Real mistake hit here:** this command was written out but never actually run — the session got sidetracked into a discussion about where to store the policy JSON files (`/tmp` vs. committed in `infra/aws/`) right after the command was given, and the "did you run it" confirmation step got skipped. Result, several steps later when testing the stage:

```
Error assuming AWS_ROLE: User: arn:aws:iam::<snowflake_iam_user> is not authorized to perform:
sts:AssumeRole on resource: arn:aws:iam::<account_id>:role/de-ecommerce-medallion-role
```

Diagnosed by re-checking the role's live trust policy:
```bash
aws iam get-role --role-name de-ecommerce-medallion-role --query 'Role.AssumeRolePolicyDocument'
```
— it still showed the placeholder (`:root`), confirming Step 5's update never took effect. Re-running the `update-assume-role-policy` command fixed it immediately.

**Lesson:** after handing over a command that changes remote state (AWS/Snowflake), don't move to the next topic until its output is actually pasted back and confirmed — a command *described* is not a command *run*.

### Step 6 — warehouse, database, schemas, role (RBAC pattern)

```sql
-- Compute + storage objects (SYSADMIN owns these)
USE ROLE SYSADMIN;

CREATE WAREHOUSE DE_ECOMMERCE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE DE_ECOMMERCE_DB;
CREATE SCHEMA DE_ECOMMERCE_DB.BRONZE;
CREATE SCHEMA DE_ECOMMERCE_DB.SILVER;
CREATE SCHEMA DE_ECOMMERCE_DB.GOLD;

-- Dedicated role (SECURITYADMIN owns role creation/assignment)
USE ROLE SECURITYADMIN;
CREATE ROLE DE_ECOMMERCE_ROLE;
GRANT ROLE DE_ECOMMERCE_ROLE TO USER <your_snowflake_username>;

-- Grant the role rights to use the objects above (SYSADMIN owns object grants)
USE ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE DE_ECOMMERCE_WH TO ROLE DE_ECOMMERCE_ROLE;
GRANT OPERATE ON WAREHOUSE DE_ECOMMERCE_WH TO ROLE DE_ECOMMERCE_ROLE;
GRANT USAGE ON DATABASE DE_ECOMMERCE_DB TO ROLE DE_ECOMMERCE_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE DE_ECOMMERCE_DB TO ROLE DE_ECOMMERCE_ROLE;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA DE_ECOMMERCE_DB.BRONZE TO ROLE DE_ECOMMERCE_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA DE_ECOMMERCE_DB.SILVER TO ROLE DE_ECOMMERCE_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA DE_ECOMMERCE_DB.GOLD TO ROLE DE_ECOMMERCE_ROLE;
GRANT USAGE ON INTEGRATION de_ecommerce_medallion_int TO ROLE DE_ECOMMERCE_ROLE;
```

SECURITYADMIN and SYSADMIN are deliberately separated in Snowflake's default RBAC hierarchy — one manages *who can act as whom* (roles, role-to-user grants), the other manages *what objects exist and who can use them*. Doing everything as ACCOUNTADMIN works but is the anti-pattern real security audits flag.

### Step 7 — stage + file format, and end-to-end verification

```sql
USE ROLE DE_ECOMMERCE_ROLE;
USE WAREHOUSE DE_ECOMMERCE_WH;
USE DATABASE DE_ECOMMERCE_DB;
USE SCHEMA BRONZE;

CREATE FILE FORMAT DE_ECOMMERCE_DB.BRONZE.CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE STAGE DE_ECOMMERCE_DB.BRONZE.RAW_STAGE
  URL = 's3://de-ecommerce-medallion/'
  STORAGE_INTEGRATION = de_ecommerce_medallion_int
  FILE_FORMAT = DE_ECOMMERCE_DB.BRONZE.CSV_FORMAT;

LIST @DE_ECOMMERCE_DB.BRONZE.RAW_STAGE;
```

**Verified 2026-09-16:** `LIST` returned cleanly (bucket empty at this point, zero rows, no error) — proves the full chain (Snowflake storage integration → AWS IAM role → trust policy → S3 bucket) is wired correctly end to end.

---

## Rebuild checklist (do this end-to-end without help)

1. `git init`, `.gitignore`, first commit, `gh repo create ... --private --source=. --remote=origin`, push.
2. AWS: create S3 bucket → create IAM policy scoped to that bucket → create IAM role with a **placeholder** trust policy → attach policy to role.
3. Snowflake (ACCOUNTADMIN): `CREATE STORAGE INTEGRATION` pointing at the role ARN + bucket → `DESC STORAGE INTEGRATION` → copy `STORAGE_AWS_IAM_USER_ARN` + `STORAGE_AWS_EXTERNAL_ID`.
4. AWS: write the **real** trust policy with those two values → `aws iam update-assume-role-policy` → **confirm with `aws iam get-role ... --query 'Role.AssumeRolePolicyDocument'` before moving on.**
5. Snowflake (SYSADMIN/SECURITYADMIN split): warehouse, database, 3 schemas (bronze/silver/gold), dedicated role, grants, `GRANT USAGE ON INTEGRATION ... TO ROLE ...`.
6. Snowflake (as the dedicated role): file format + stage in `BRONZE` referencing the storage integration → `LIST @stage` to prove connectivity.
7. Next: dataset generation (orders/order_items/customers/products) → dbt bronze/silver/gold models → Airflow DAG (sensor → extract → load → dbt run/test → branch → alert, with retries).

---

## Next steps

- Dataset generation script (ecommerce: orders, order_items, customers, products).
- dbt project scaffold + bronze/silver/gold models.
- Airflow `docker-compose.yaml` setup (webserver/scheduler/triggerer/metadata DB) and the pipeline DAG.

---

## Interview Q&A — custom Airflow images (study this, recite out loud)

Hit while wiring dbt into the Airflow worker container — dbt runs as a shell command, not a Python import, so it has to live inside the same image the task executes in, not just the host `.venv`.

**Q: Why do companies often need custom Airflow images?**
A: "Airflow's base image only has Airflow itself installed. The moment a DAG needs to run a tool like dbt, or a specific library version, that has to be baked into the image, since tasks execute inside the container, not on the host."

**Q: Have you ever built a custom Airflow image?**
A: "Yes — I ran plain Airflow via docker-compose for a project, and when I needed a task to run `dbt run`, I hit exactly this: dbt wasn't in the container. I wrote a small Dockerfile extending `apache/airflow`, added `pip install dbt-core dbt-snowflake`, and rebuilt the image so the worker/scheduler containers had dbt available as a CLI command."

**Q: Why not just install dbt at runtime instead of baking it into the image?**
A: "Reproducibility — if you install at runtime, every task run redownloads packages, is slower, and can silently drift to a different version over time. Baking it into the image at build time pins the exact version so every task run is identical."

**Q: What's the alternative to putting everything in one custom image?**
A: "`KubernetesPodOperator` or `DockerOperator` — instead of one big image with every tool installed, each task spins up its own short-lived container with just what it needs. Keeps the core Airflow image lean and avoids dependency conflicts between tools."

**Q: Why did you choose plain docker-compose over Astro CLI / a managed service?**
A: "To actually see the underlying architecture — webserver, scheduler, triggerer, metadata DB — and the real problems around it, like needing a custom image. Astro CLI or a managed service like MWAA builds that custom image for you automatically from a requirements file, which is convenient in production but would've hidden the mechanics I wanted to learn."

**Q: Why mount your DAGs folder as a volume instead of copying it into the image?**
A: "So editing a DAG file doesn't require rebuilding the image — the scheduler just picks up the change on disk. You only rebuild the image when *dependencies* change, not when pipeline logic changes."
