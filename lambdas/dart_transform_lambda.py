# dart_transform_lambda.py
import os
import io
import json
import boto3
import pandas as pd
from datetime import datetime, timezone

S3 = boto3.client("s3")
ISF_BUCKET = os.environ.get("ISF_BUCKET", "plm-isf-bucket")
DART_BUCKET = os.environ.get("DART_BUCKET", "plm-dart-bucket")
ERROR_BUCKET = os.environ.get("ERROR_BUCKET", "plm-error-bucket")
DART_PREFIX = os.environ.get("DART_PREFIX", "dart/")
ERROR_PREFIX = os.environ.get("ERROR_PREFIX", "errors/")

# business required fields
REQUIRED = ["provider_id", "date", "name"]

def read_parquet_s3(bucket, key):
    obj = S3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))

def write_csv_s3(text, bucket, key):
    S3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))

def write_parquet_s3_bytes(df, bucket, key):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    S3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

def validate(df: pd.DataFrame):
    df["_validation_error"] = ""
    valid_mask = pd.Series(True, index=df.index)
    for col in REQUIRED:
        if col not in df.columns:
            df["_validation_error"] += f"missing:{col};"
            valid_mask &= False
        else:
            if col == "date":
                parsed = pd.to_datetime(df["date"], errors="coerce")
                bad = parsed.isna()
                df.loc[bad, "_validation_error"] += "bad_date;"
                valid_mask[bad] = False
    return df[valid_mask].copy(), df[~valid_mask].copy()

def handler(event, context):
    """
    event = {"isf_keys":[...], "job_id":"..."}
    """
    keys = event.get("isf_keys", [])
    job_id = event.get("job_id", f"job-{int(datetime.now(timezone.utc).timestamp())}")
    dart_keys = []
    error_keys = []
    total_valid = 0
    total_invalid = 0

    for key in keys:
        try:
            df = read_parquet_s3(ISF_BUCKET, key)
            valid_df, invalid_df = validate(df)
            base_name = key.split('/')[-1].replace('.parquet', '')
            if not valid_df.empty:
                # rename columns to target column names expected by Redshift/COPY
                out_df = valid_df.rename(columns={
                    "provider_id": "providerId",
                    "date": "rosterDate",
                    "name": "providerName"
                })
                csv_text = out_df.to_csv(index=False)
                out_key = f"{DART_PREFIX}{job_id}/{base_name}--{job_id}.csv"
                write_csv_s3(csv_text, DART_BUCKET, out_key)
                dart_keys.append(out_key)
                total_valid += len(out_df)
            if not invalid_df.empty:
                err_key = f"{ERROR_PREFIX}{job_id}/{base_name}--{job_id}.parquet"
                write_parquet_s3_bytes(invalid_df, ERROR_BUCKET, err_key)
                error_keys.append(err_key)
                total_invalid += len(invalid_df)
        except Exception as e:
            error_keys.append({"isf_key": key, "error": str(e)})

    return {
        "status": "OK",
        "dart_keys": dart_keys,
        "error_keys": error_keys,
        "total_valid": total_valid,
        "total_invalid": total_invalid
    }
