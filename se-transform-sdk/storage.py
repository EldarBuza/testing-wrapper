import os, io, json, hashlib
from pathlib import Path
from typing import Any, Dict, Optional

class StorageIO:
    def read_text(self, bucket: str, key: str) -> str: ...
    def write_text(self, bucket: str, key: str, data: str) -> Dict[str, Any]: ...
    def read_jsonl(self, bucket: str, key: str): ...
    def write_jsonl(self, bucket: str, key: str, df) -> Dict[str, Any]: ...
    def uri(self, bucket: str, key: str) -> str: ...
    def exists(self, bucket: str, key: str) -> bool: ...

class LocalFSIO(StorageIO):
    def __init__(self, root: Optional[str] = None):
        self.root = Path(root or os.getenv("SE_LOCAL_S3_ROOT", "./local_s3")).resolve()

    def _p(self, bucket: str, key: str) -> Path:
        return (self.root / bucket / key).resolve()

    def read_text(self, bucket: str, key: str) -> str:
        return self._p(bucket, key).read_text(encoding="utf-8")

    def write_text(self, bucket: str, key: str, data: str) -> Dict[str, Any]:
        p = self._p(bucket, key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(data, encoding="utf-8")
        return {"etag": hashlib.md5(data.encode("utf-8")).hexdigest(), "bytes": len(data)}

    def read_jsonl(self, bucket: str, key: str):
        import pandas as pd
        text = self.read_text(bucket, key)
        rows = [json.loads(l) for l in text.splitlines() if l]
        return pd.DataFrame(rows)

    def write_jsonl(self, bucket: str, key: str, df):
        buf = io.StringIO()
        for rec in json.loads(df.to_json(orient="records")):
            buf.write(json.dumps(rec) + "\n")
        return self.write_text(bucket, key, buf.getvalue())

    def uri(self, bucket: str, key: str) -> str:
        return f"s3://{bucket}/{key}"

    def exists(self, bucket: str, key: str) -> bool:
        return self._p(bucket, key).exists()

class S3IO(StorageIO):
    def __init__(self, region: Optional[str] = None, endpoint_url: Optional[str] = None):
        import boto3
        self.s3 = boto3.client(
            "s3",
            region_name=region or os.getenv("AWS_REGION"),
            endpoint_url=endpoint_url or os.getenv("SE_S3_ENDPOINT_URL"),
        )

    def read_text(self, bucket: str, key: str) -> str:
        obj = self.s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")

    def write_text(self, bucket: str, key: str, data: str) -> Dict[str, Any]:
        resp = self.s3.put_object(Bucket=bucket, Key=key, Body=data.encode("utf-8"))
        etag = (resp.get("ETag") or "").strip('"')
        return {"etag": etag, "bytes": len(data)}

    def read_jsonl(self, bucket: str, key: str):
        import pandas as pd, json
        text = self.read_text(bucket, key)
        rows = [json.loads(l) for l in text.splitlines() if l]
        return pd.DataFrame(rows)

    def write_jsonl(self, bucket: str, key: str, df):
        import json
        buf = io.StringIO()
        for rec in json.loads(df.to_json(orient="records")):
            buf.write(json.dumps(rec) + "\n")
        return self.write_text(bucket, key, buf.getvalue())

    def uri(self, bucket: str, key: str) -> str:
        return f"s3://{bucket}/{key}"

    def exists(self, bucket: str, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False

def get_storage() -> StorageIO:
    backend = os.getenv("SE_STORAGE_BACKEND", "local").lower()
    if backend == "s3":
        return S3IO()
    return LocalFSIO()