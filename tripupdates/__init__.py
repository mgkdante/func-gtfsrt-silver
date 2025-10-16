import io, os, re, logging
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from shared.gtfs_parsers import parse_tripupdates

# ENV: set ACCOUNT_URL=https://<account>.blob.core.windows.net
#      optionally SILVER_CONTAINER=silver
ACCOUNT_URL = os.getenv("ACCOUNT_URL")
SILVER_CONTAINER = os.getenv("SILVER_CONTAINER", "silver")

# Fallback regex if you don't pass {date} as a binding param
_DT_RE = re.compile(r"dt=(\d{4}-\d{2}-\d{2})")

def _extract_dt(blob_name: str, date_param: str | None) -> str:
    if date_param:
        return date_param
    m = _DT_RE.search(blob_name or "")
    if m:
        return m.group(1)
    return datetime.now(timezone.utc).date().isoformat()

def _upload_parquet(container: str, dst_path: str, table: pa.Table, overwrite: bool = False):
    if not ACCOUNT_URL:
        raise RuntimeError("ACCOUNT_URL env var is required (e.g., https://stdatalaketransitdemo.blob.core.windows.net)")
    cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    bsc = BlobServiceClient(account_url=ACCOUNT_URL, credential=cred)

    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    client = bsc.get_container_client(container)
    client.upload_blob(name=dst_path, data=buf, overwrite=overwrite)

def main(myblob: func.InputStream, date: str = "", hour: str = "", filename: str = ""):
    """
    Blob trigger signature matches tokens from function.json:
      path = "bronze/raw/gtfsrt/tripupdates/dt={date}/hr={hour}/{filename}.pb"
    """
    try:
        blob_name = getattr(myblob, "name", "")
        dt = _extract_dt(blob_name, date_param=(date or None))

        payload = myblob.read()
        rows = parse_tripupdates(payload)
        if not rows:
            logging.info("TripUpdates: no rows parsed from %s", blob_name)
            return

        df = pd.DataFrame(rows)
        # Normalize dt to a date column
        df["dt"] = pd.to_datetime(dt).dt.date
        table = pa.Table.from_pandas(df, preserve_index=False)

        stamp = int(datetime.now(timezone.utc).timestamp())
        # Keep a stable, partitioned layout in Silver
        dst = f"clean/gtfsrt/tripupdates/dt={dt}/part-{stamp}.parquet"

        _upload_parquet(SILVER_CONTAINER, dst, table, overwrite=False)
        logging.info(
            "TripUpdates: wrote %d rows to silver: %s (src=%s dt=%s hr=%s file=%s)",
            len(df), dst, blob_name, date, hour, filename
        )

    except Exception as ex:
        logging.exception("TripUpdates: failed processing blob: %s | error=%s", getattr(myblob, "name", ""), ex)
        raise
