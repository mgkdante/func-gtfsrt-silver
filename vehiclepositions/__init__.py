import io, os, re, logging, time
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

#from shared.gtfs_parsers import parse_vehiclepositions
def parse_vehiclepositions(_): return []

# === CONFIG ===
ACCOUNT_URL = os.getenv("ACCOUNT_URL")  # e.g. https://stdatalaketransitdemo.blob.core.windows.net
SILVER_CONTAINER = os.getenv("SILVER_CONTAINER", "silver")
_DT_RE = re.compile(r"dt=(\\d{4}-\\d{2}-\\d{2})")

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

def _extract_dt(blob_name: str, date_param: str | None) -> str:
    if date_param:
        return date_param
    m = _DT_RE.search(blob_name or "")
    if m:
        return m.group(1)
    return datetime.now(timezone.utc).date().isoformat()

def _upload_parquet(container: str, dst_path: str, table: pa.Table, overwrite: bool = False):
    """Upload Parquet to Silver container using managed identity."""
    cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    bsc = BlobServiceClient(account_url=ACCOUNT_URL, credential=cred)

    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    client = bsc.get_container_client(container)
    client.upload_blob(name=dst_path, data=buf, overwrite=overwrite)

def main(myblob: func.InputStream, date: str = "", hour: str = "", filename: str = ""):
    """
    Triggered when a new GTFS-RT VehiclePositions blob arrives in Bronze.
    Example blob path:
        bronze/raw/gtfsrt/vehiclepositions/dt=2025-10-16/hr=02/vehiclepositions_20251016020324.pb
    """
    blob_name = getattr(myblob, "name", "")
    start = time.time()

    logging.info("=== VehiclePositions Function Triggered ===")
    logging.info("Source blob: %s", blob_name)
    logging.info("Binding context: date=%s, hour=%s, filename=%s", date, hour, filename)

    try:
        dt = _extract_dt(blob_name, date_param=(date or None))

        payload = myblob.read()
        size_bytes = len(payload)
        logging.info("Downloaded blob (%d bytes)", size_bytes)

        rows = parse_vehiclepositions(payload)
        if not rows:
            logging.warning("VehiclePositions: no rows parsed from %s", blob_name)
            return

        df = pd.DataFrame(rows)
        df["dt"] = pd.to_datetime(dt).dt.date
        row_count = len(df)
        logging.info("Parsed %d rows from %s", row_count, blob_name)

        table = pa.Table.from_pandas(df, preserve_index=False)

        stamp = int(datetime.now(timezone.utc).timestamp())
        dst = f"clean/gtfsrt/vehiclepositions/dt={dt}/part-{stamp}.parquet"
        logging.info("Uploading to Silver: %s", dst)

        _upload_parquet(SILVER_CONTAINER, dst, table, overwrite=False)
        elapsed = round(time.time() - start, 2)

        logging.info(
            "✅ VehiclePositions: wrote %d rows (%d bytes) to silver:%s [elapsed: %.2fs]",
            row_count,
            size_bytes,
            dst,
            elapsed,
        )

    except Exception as ex:
        logging.exception(
            "❌ VehiclePositions: failed processing blob: %s | error=%s", blob_name, ex
        )
        raise

    finally:
        logging.info("=== VehiclePositions Function Completed ===\n")
