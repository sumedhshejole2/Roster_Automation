# export_to_s3.py
# Sample exporter that connects to Oracle, streams rows and writes JSONL files to S3.
import os
import json
import tempfile
import boto3
import oracledb

S3 = boto3.client("s3")
RAW_BUCKET = os.environ.get("S3_RAW_BUCKET", "plm-raw-bucket")
ORACLE_DSN = os.environ.get("ORACLE_DSN", "host:port/service")
ORACLE_USER = os.environ.get("ORACLE_USER", "user")
ORACLE_PASSWORD = os.environ.get("ORACLE_PASSWORD", "pass")

def export_to_s3(run_id: str, batch_size: int = 1000):
    """Streams rows from ROSTER_RAW where export_status IS NULL, writes JSONL files to S3.
    Returns: {"s3_keys":[...], "run_id": run_id}
    """
    conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
    cur = conn.cursor()
    sql = "SELECT id, payload FROM roster_raw WHERE export_status IS NULL AND ROWNUM <= :limit"  # adapt as needed
    cur.execute(sql, {"limit": 10000})
    s3_keys = []
    rows = []
    count = 0
    tmp = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.jsonl')
    try:
        for r in cur:
            _id, payload = r
            try:
                obj = json.loads(payload)
            except Exception:
                obj = {"_raw": payload}
            rows.append(obj)
            count += 1
            if count % batch_size == 0:
                tmp_path = tmp.name
                with open(tmp_path, 'a', encoding='utf-8') as fh:
                    for rr in rows:
                        fh.write(json.dumps(rr, default=str) + '\n')
                s3_key = f"raw/{run_id}/export_{count}.jsonl"
                S3.upload_file(tmp_path, RAW_BUCKET, s3_key)
                s3_keys.append(s3_key)
                rows = []
                tmp = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.jsonl')
        # final chunk
        if rows:
            tmp_path = tmp.name
            with open(tmp_path, 'a', encoding='utf-8') as fh:
                for rr in rows:
                    fh.write(json.dumps(rr, default=str) + '\n')
            s3_key = f"raw/{run_id}/export_final.jsonl"
            S3.upload_file(tmp_path, RAW_BUCKET, s3_key)
            s3_keys.append(s3_key)
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
    # Ideally update ROSTER_RAW export_status here (requires appropriate privileges)
    return {"s3_keys": s3_keys, "run_id": run_id}
