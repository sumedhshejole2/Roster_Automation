# Roster Automation Project

## 📌 Overview
This project automates ingestion, transformation, validation, and loading of provider roster data from a PLM portal into **Amazon Redshift** for analytics.  
The pipeline is **orchestrated using Apache Airflow** and integrates with **AWS Lambda, S3, and Redshift**.

## 🚀 Architecture
1. **Ingestion:** PLM Portal → Kafka → Oracle (ROSTER_RAW).
2. **Export:** Airflow exports unprocessed rows from Oracle → S3 (Raw JSONL).
3. **Transformations:**
   - **ISF Lambda:** Normalizes JSON → Parquet (ISF format).
   - **Dart Lambda:** Validates ISF → CSV (for Redshift) + invalid records to Error bucket.
4. **Loading:** Airflow copies validated CSVs to Redshift staging → MERGE/UPSERT into final tables.
5. **Orchestration:** Entire pipeline is handled by a single Airflow DAG.

## 📂 Project Structure
```
docs/           → Project documentation
dags/           → Airflow DAGs
lambdas/        → AWS Lambda functions
sql/            → Oracle + Redshift SQL scripts
terraform/      → Infrastructure as Code
requirements.txt → Python dependencies
```

## ⚡ Quick start
1. Place your original documentation in `docs/`.
2. Edit environment variables in `dags/full_roster_pipeline_lambda.py` and the Terraform files.
3. Build and push Lambda container images (if using heavy libs) or package as zip/layers.
4. Deploy infra with Terraform (optional).
5. Add DAGs to your Airflow environment.

## 🛠️ Files included
- Airflow DAG: `dags/full_roster_pipeline_lambda.py`
- Lambda handlers: `lambdas/isf_transform_lambda.py`, `lambdas/dart_transform_lambda.py`
- Oracle exporter sample: `lambdas/exporter/export_to_s3.py`
- SQL: `sql/oracle_staging.sql`, `sql/redshift_staging.sql`, `sql/redshift_final.sql`
- Terraform: placeholder files in `terraform/`
- `requirements.txt` and `.gitignore`

## 📌 Notes
- Replace placeholder ARNs, secrets, connection IDs, and bucket names before deploying.
- For heavy dependencies like pandas/pyarrow, prefer Lambda container images or Lambda layers.
