# dags/full_roster_pipeline_lambda.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
import os
import json

# env-config / defaults (override via Airflow Variables or env vars)
S3_RAW_BUCKET = os.environ.get("S3_RAW_BUCKET", "plm-raw-bucket")
ISF_LAMBDA_NAME = os.environ.get("ISF_LAMBDA_NAME", "isf-transform-lambda")
DART_LAMBDA_NAME = os.environ.get("DART_LAMBDA_NAME", "dart-transform-lambda")
DART_BUCKET = os.environ.get("DART_BUCKET", "plm-dart-bucket")
REDSHIFT_CONN_ID = os.environ.get("REDSHIFT_CONN_ID", "redshift_conn")
AWS_CONN_ID = os.environ.get("AWS_CONN_ID", "aws_default")

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="full_roster_pipeline_lambda",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    def export_oracle_to_s3(**kwargs):
        """
        Implement export logic in a module `roster_export.export_to_s3`.
        This function must return a dict: {"s3_keys": [...], "run_id": "..."}.
        """
        run_id = f"airflow-{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
        # Example: import your real exporter module
        try:
            from roster_export.exporter import export_to_s3  # implement this separately
            result = export_to_s3(run_id=run_id)
        except Exception as e:
            # If exporter is not available, return NO_FILES so DAG can continue safely in dry-run
            result = {"s3_keys": [], "run_id": run_id, "error": str(e)}
        kwargs["ti"].xcom_push(key="raw_keys", value=result.get("s3_keys", []))
        kwargs["ti"].xcom_push(key="run_id", value=run_id)
        return result

    export_task = PythonOperator(
        task_id="export_oracle_to_s3",
        python_callable=export_oracle_to_s3,
        provide_context=True,
    )

    def invoke_lambda(lambda_name, payload):
        hook = AwsLambdaHook(function_name=lambda_name, aws_conn_id=AWS_CONN_ID)
        resp = hook.invoke_lambda(payload=json.dumps(payload), invocation_type="RequestResponse")
        if isinstance(resp, (bytes, bytearray)):
            resp = resp.decode("utf-8")
        try:
            return json.loads(resp)
        except Exception:
            return resp

    def invoke_isf_lambda(**kwargs):
        ti = kwargs["ti"]
        raw_keys = ti.xcom_pull(key="raw_keys", task_ids="export_oracle_to_s3") or []
        run_id = ti.xcom_pull(key="run_id", task_ids="export_oracle_to_s3") or f"airflow-{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
        if not raw_keys:
            return {"status": "NO_FILES", "isf_keys": []}
        payload = {"s3_keys": raw_keys, "job_id": run_id}
        resp = invoke_lambda(ISF_LAMBDA_NAME, payload)
        ti.xcom_push(key="isf_response", value=resp)
        return resp

    isf_invoke = PythonOperator(task_id="invoke_isf_lambda", python_callable=invoke_isf_lambda, provide_context=True)

    def invoke_dart_lambda(**kwargs):
        ti = kwargs["ti"]
        isf_resp = ti.xcom_pull(key="isf_response", task_ids="invoke_isf_lambda") or {}
        isf_keys = isf_resp.get("isf_keys", [])
        run_id = ti.xcom_pull(key="run_id", task_ids="export_oracle_to_s3")
        if not isf_keys:
            return {"status": "NO_ISF", "dart_keys": []}
        payload = {"isf_keys": isf_keys, "job_id": run_id}
        resp = invoke_lambda(DART_LAMBDA_NAME, payload)
        ti.xcom_push(key="dart_response", value=resp)
        return resp

    dart_invoke = PythonOperator(task_id="invoke_dart_lambda", python_callable=invoke_dart_lambda, provide_context=True)

    def load_to_redshift(**kwargs):
        ti = kwargs["ti"]
        dart_resp = ti.xcom_pull(key="dart_response", task_ids="invoke_dart_lambda") or {}
        dart_keys = dart_resp.get("dart_keys", [])
        run_id = ti.xcom_pull(key="run_id", task_ids="export_oracle_to_s3")
        if not dart_keys:
            return {"status": "NO_DART"}
        # perform COPY per file (or implement single COPY with prefix)
        # Here we show a simple psycopg2 approach using Redshift connection.
        import boto3
        from airflow.hooks.base import BaseHook
        import psycopg2

        # fetch redshift connection details (implement as you prefer)
        conn = BaseHook.get_connection(REDSHIFT_CONN_ID)
        conn_string = f"host={conn.host} port={conn.port} dbname={conn.schema} user={conn.login} password={conn.password}"
        pg = psycopg2.connect(conn_string)
        cur = pg.cursor()

        # Use an IAM role ARN or credentials for COPY depending on your setup.
        redshift_schema = os.environ.get("REDSHIFT_SCHEMA", "public")
        redshift_table = os.environ.get("REDSHIFT_TABLE", "roster_staging")
        iam_role = os.environ.get("REDSHIFT_COPY_ROLE_ARN")  # recommended

        for key in dart_keys:
            s3_path = f"s3://{DART_BUCKET}/{key}"
            copy_sql = f"""
                COPY {redshift_schema}.{redshift_table}
                FROM '{s3_path}'
                IAM_ROLE '{iam_role}'
                CSV
                IGNOREHEADER 1
                TIMEFORMAT 'auto';
            """
            cur.execute(copy_sql)
            pg.commit()

        # After COPYs, call stored procedure or run MERGE logic here (optional)
        cur.close()
        pg.close()
        return {"status": "COPIED", "files": dart_keys}

    load_task = PythonOperator(task_id="load_to_redshift", python_callable=load_to_redshift, provide_context=True)

    export_task >> isf_invoke >> dart_invoke >> load_task
