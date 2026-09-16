# Project 3 — Ecommerce Medallion Pipeline (Airflow-orchestrated)

Ondhe doc — decisions, Phase 0 infra build, real mistake hit, mattu rebuild checklist — namma nadeda hage add aaguthade, Project 1/2 pattern.

---

## Goal

Ee project ge hosa concept: **Apache Airflow orchestration** (plain `docker-compose.yaml`, Astro CLI alla — deliberately, real architecture nodoke: webserver / scheduler / triggerer / metadata DB). Hosa dataset — ecommerce orders/order_items/customers/products — **bronze → silver → gold medallion** layout nalli Snowflake ge, dbt transforms madutte mattu Airflow DAG drive maadutte (sensor → extract → load → dbt run/test → branch → alert, retries sahita).

---

## Decision log

### Decision 1 — Hosa dataset, Project 1/2 wrap maadalla
10-project roadmap nalli prati project ge tanna own dataset scratch inda build aaguthade, maximum hands-on repetition ge — earlier projects data/infra reuse/wrap maadalla. Project 3 = ecommerce, deliberately telecom/taxi alla.

### Decision 2 — Airflow plain `docker-compose.yaml` inda
Astro CLI alla. Astro CLI real Airflow architecture na tanna tooling hinde hide maadutte — idhu modhalane Airflow project agiruvudrinda, goal webserver/scheduler/triggerer/metadata-DB wiring nalli direct agi understand maadodhu, abstraction layer through alla.

### Decision 3 — Naming convention: "portfolio" / "sowrabh" drop
Ee project inda start aagi, ella hosa resource names (S3 buckets, IAM roles/policies, Snowflake objects) `de-` / `DE_` prefix use maaduthave — `sowrabh-` illa "portfolio" yelli illa. Reason: ee projects production-grade depth kade build aaguthiruvudhu, portfolio checkbox tarah alla. Project 1 & 2 dha existing resources ivaga tanna old names ulisikoluthave — ella 10 concepts learn aada mele, ee convention kelagide teardown + rebuild aaguthave.

### Decision 4 — AWS ee project ge CLI, Project 4 inda Console
Resume nalli already solid AWS hands-on experience claim ide (originally Console inda build madidhu), aadre CLI syntax specifically rusty aagide. Project 3 CLI relearn maadoke use aaguthade (idhu "fixed syntax," easy agi drill maadoke aguthade). Project 4 inda AWS work Console ge switch aaguthade — IAM role/trust-policy/permissions-policy relationships click maadi nododhu, CLI JSON odhoke intha better mental model kattutte.

---

## Repo setup

```bash
cd Projects/ecommerce-medallion-pipeline
git init
# .gitignore baredhivi (Python/.venv, secrets, dbt target/logs, Airflow logs/cfg/db, data/raw/*)
git add .gitignore
git commit -m "Initial commit: add .gitignore"

gh repo create sowrabh-m/ecommerce-medallion-pipeline --private --source=. --remote=origin
git push -u origin master
```

Repo: `github.com/sowrabh-m/ecommerce-medallion-pipeline` (private).

---

## Phase 0 — AWS infra (CLI inda)

### Step 1 — S3 bucket

```bash
aws s3api create-bucket --bucket de-ecommerce-medallion --region us-east-1
```

### Step 2 — IAM policy (enu access allow aagide)

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

Erdu statements: object-level actions (`Get/PutObject`) ge `/*` ARN suffix beku, bucket **olage** irodakke act aaguthave; bucket-level actions (`ListBucket`, `GetBucketLocation`) ge bare bucket ARN saku, bucket mele ne act aaguthave.

```bash
aws iam create-policy \
  --policy-name de-ecommerce-medallion-s3-policy \
  --policy-document file://infra/aws/s3-policy.json
```
→ `Arn` return aaguthade, kelagde beku.

### Step 3 — IAM role (yaaru ee access use madoke allowed)

Chicken-and-egg problem: **real** trust policy ge Snowflake dha IAM user ARN + external ID beku, aadre adhu Snowflake nalli storage integration create madoke munche exist agalla — adhake role dha ARN beku. Fix: modhalu **placeholder** trust policy inda role create maadu (ninna own account trust maadi), aadhme real values swap maadu.

Placeholder (`<account_id>` = ninna AWS account ID):
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
`Principal.AWS` ARN mele `:root` andre "whole account" antha, root user alla.

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

ACCOUNTADMIN inda run maadu (storage integration Snowflake hora hogi bere cloud account touch maaduthade, adhake `CREATE INTEGRATION` default agi highest-privilege role ge lock aagide):

```sql
CREATE STORAGE INTEGRATION de_ecommerce_medallion_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account_id>:role/de-ecommerce-medallion-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://de-ecommerce-medallion/');

DESC STORAGE INTEGRATION de_ecommerce_medallion_int;
```

`DESC` inda `STORAGE_AWS_IAM_USER_ARN` mattu `STORAGE_AWS_EXTERNAL_ID` sigutte — AWS trust policy lock down madoke real values beku.

### Step 5 — real trust policy

`infra/aws/trust-policy.json` (real values, private repo agirodrinda commit maadoke safe):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "<STORAGE_AWS_IAM_USER_ARN, DESC inda>" },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": { "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID, DESC inda>" }
      }
    }
  ]
}
```

`Condition` important: illandhre, aa Snowflake IAM user dha credentials (many Snowflake customer accounts naduve shared) hidkondhavru yaaru bekadhru ee role assume maadbahudhu. External ID ide "idhu specifically ee Snowflake account dhu integration" antha prove maadodhu.

```bash
aws iam update-assume-role-policy \
  --role-name de-ecommerce-medallion-role \
  --policy-document file://infra/aws/trust-policy.json
```

**⚠️ Real mistake idhe nalli aayithu:** ee command bara aayithu, aadre actually run aagalilla — command kotta immediate aftere session `/tmp` vs `infra/aws/` nalli policy JSON files elli store maadbeku antha discussion ge sidetrack aayithu, "run aaythe" antha confirm maadodhu skip aaythu. Result, several steps aadha mele stage test madoke hogadhaga:

```
Error assuming AWS_ROLE: User: arn:aws:iam::<snowflake_iam_user> is not authorized to perform:
sts:AssumeRole on resource: arn:aws:iam::<account_id>:role/de-ecommerce-medallion-role
```

Diagnose maadidhu role dha live trust policy re-check maadi:
```bash
aws iam get-role --role-name de-ecommerce-medallion-role --query 'Role.AssumeRolePolicyDocument'
```
— idhu innu placeholder (`:root`) torisithu, Step 5 dha update take aagalilla antha confirm aaythu. `update-assume-role-policy` command matte run maadidhaga immediate fix aaythu.

**Lesson:** remote state badalisodhu (AWS/Snowflake) command kotta mele, adhara output paste madi confirm madodhara varegu next topic ge hogbaardhu — command *describe* madodhu, command *run* aadhange alla.

### Step 6 — warehouse, database, schemas, role (RBAC pattern)

```sql
-- Compute + storage objects (SYSADMIN owns maaduthade)
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

-- Dedicated role (SECURITYADMIN role creation/assignment owns maaduthade)
USE ROLE SECURITYADMIN;
CREATE ROLE DE_ECOMMERCE_ROLE;
GRANT ROLE DE_ECOMMERCE_ROLE TO USER <ninna_snowflake_username>;

-- Role ge objects use maadoke rights kodu (SYSADMIN object grants owns maaduthade)
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

SECURITYADMIN mattu SYSADMIN Snowflake dha default RBAC hierarchy nalli deliberately separate aagide — ondhu "yaaru yaara tarah act maadbahudhu" antha manage maadutte (roles, role-to-user grants), innondhu "yenu objects exist aagide mattu yaaru use madbahudhu" antha manage maadutte. Ella ondhe ACCOUNTADMIN inda maadidre kelsa maaduthade, aadre real security audits idhe flag maaduthave.

### Step 7 — stage + file format, mattu end-to-end verification

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

**Verify aayithu 2026-09-16:** `LIST` clean agi return aaythu (bucket ivaga empty, zero rows, error illa) — idhu prove maaduthade full chain (Snowflake storage integration → AWS IAM role → trust policy → S3 bucket) end to end correctly wire aagide antha.

---

## Rebuild checklist (idhe end-to-end without help madoke)

1. `git init`, `.gitignore`, first commit, `gh repo create ... --private --source=. --remote=origin`, push.
2. AWS: S3 bucket create maadu → aa bucket ge scoped IAM policy create maadu → **placeholder** trust policy inda IAM role create maadu → policy na role ge attach maadu.
3. Snowflake (ACCOUNTADMIN): `CREATE STORAGE INTEGRATION` role ARN + bucket point maadi → `DESC STORAGE INTEGRATION` → `STORAGE_AWS_IAM_USER_ARN` + `STORAGE_AWS_EXTERNAL_ID` copy maadu.
4. AWS: aa erdu values inda **real** trust policy baredhu → `aws iam update-assume-role-policy` → **`aws iam get-role ... --query 'Role.AssumeRolePolicyDocument'` inda confirm maadi mundhe hogu.**
5. Snowflake (SYSADMIN/SECURITYADMIN split): warehouse, database, 3 schemas (bronze/silver/gold), dedicated role, grants, `GRANT USAGE ON INTEGRATION ... TO ROLE ...`.
6. Snowflake (dedicated role tarah): file format + stage `BRONZE` nalli storage integration reference maadi → `LIST @stage` connectivity prove madoke.
7. Next: dataset generation (orders/order_items/customers/products) → dbt bronze/silver/gold models → Airflow DAG (sensor → extract → load → dbt run/test → branch → alert, retries sahita).

---

## Next steps

- Dataset generation script (ecommerce: orders, order_items, customers, products).
- dbt project scaffold + bronze/silver/gold models.
- Airflow `docker-compose.yaml` setup (webserver/scheduler/triggerer/metadata DB) mattu pipeline DAG.
