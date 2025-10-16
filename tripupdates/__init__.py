import io, os, re, logging, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timezone
from shared.gtfs_parsers import parse_tripupdates


ACCOUNT_URL = os.getenv("ACCOUNT_URL")                   # https://stdatalaketransitdemo.blob.core.windows.net
SILVER_CONTAINER = os.getenv("SILVER_CONTAINER", "silver")
_DT_RE = re.compile(r"dt=(\d{4}-\d{2}-\d{2})")

def _extract_dt(blob_name: str) -> str:
    m = _DT_RE.search(blob_name)
    if m:
        return m.group(1)
    return datetime.now(timezone.utc).date().isoformat()

def _upload_parquet(container: str, dst_path: str, table: pa.Table):
    cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    bsc = BlobServiceClient(account_url=ACCOUNT_URL, credential=cred)
    buf = io.BytesIO(); pq.write_table(table, buf); buf.seek(0)
    bsc.get_container_client(container).upload_blob(name=dst_path, data=buf, overwrite=False)

def main(myblob: func.InputStream):
    name = myblob.name
    dt = _extract_dt(name)

    payload = myblob.read()
    rows = parse_tripupdates(payload)
    if not rows:
        logging.info("TripUpdates: no rows parsed from %s", name)
        return

    df = pd.DataFrame(rows)
    df["dt"] = pd.to_datetime(dt).dt.date
    table = pa.Table.from_pandas(df, preserve_index=False)

    stamp = int(datetime.utcnow().timestamp())
    dst = f"clean/gtfsrt/tripupdates/dt={dt}/part-{stamp}.parquet"
    _upload_parquet(SILVER_CONTAINER, dst, table)
    logging.info("TripUpdates: wrote %d rows to silver: %s", len(df), dst)
