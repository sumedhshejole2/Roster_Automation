# isf_transform_lambda.py
import os
import io
import json
import math
import boto3
import pandas as pd
from datetime import datetime, timezone

S3 = boto3.client("s3")
RAW_BUCKET = os.environ.get("S3_RAW_BUCKET", "plm-raw-bucket")
ISF_BUCKET = os.environ.get("ISF_BUCKET", "plm-isf-bucket")
ISF_PREFIX = os.environ.get("ISF_PREFIX", "isf/")
MAX_ROWS_PER_CHUNK = int(os.environ.get("MAX_ROWS_PER_CHUNK", "100000"))

def read_jsonl_s3(bucket: str, key: str):
    obj = S3.get_object(Bucket=bucket, Key=key)
    text = obj["Body"].read().decode("utf-8")
    for line in text.splitlines():
        if line.strip():
            try:
                yield json.loads(line)
            except Exception:
                yield {"_raw": line}

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    # ensure required columns exist
    if "provider_id" not in df.columns:
        df["provider_id"] = None
    # handle common date column names
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
    # dedupe by provider_id + date if present
    dedupe_cols = [c for c in ("provider_id", "date") if c in df.columns]
    if dedupe_cols:
        df = df.drop_duplicates(subset=dedupe_cols, keep="last")
    return df

def write_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()

def handler(event, context):
    """
    Lambda handler.
    event = {"s3_keys":[...], "job_id":"..."}
    """
    keys = event.get("s3_keys", [])
    job_id = event.get("job_id", f"job-{int(datetime.now(timezone.utc).timestamp())}")
    produced = []
    errors = []
    processed = 0

    for key in keys:
        try:
            rows = list(read_jsonl_s3(RAW_BUCKET, key))
            if not rows:
                continue
            n_chunks = math.ceil(len(rows) / MAX_ROWS_PER_CHUNK)
            for idx in range(n_chunks):
                chunk = rows[idx * MAX_ROWS_PER_CHUNK:(idx + 1) * MAX_ROWS_PER_CHUNK]
                df = pd.DataFrame(chunk)
                df = normalize_df(df)
                df["_src_bucket"] = RAW_BUCKET
                df["_src_key"] = key
                df["_ingested_at"] = pd.Timestamp.utcnow()
                filename = key.split("/")[-1].rsplit(".", 1)[0]
                out_key = f"{ISF_PREFIX}{job_id}/{filename}--{job_id}--part{idx}.parquet"
                S3.put_object(Bucket=ISF_BUCKET, Key=out_key, Body=write_parquet_bytes(df))
                produced.append(out_key)
                processed += len(df)
        except Exception as e:
            errors.append({"key": key, "error": str(e)})

    status = "OK" if not errors else "ERROR"
    return {"status": status, "isf_keys": produced, "processed_count": processed, "errors": errors}
