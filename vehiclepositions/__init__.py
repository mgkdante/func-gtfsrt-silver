import json, logging, io, os
from datetime import datetime, timezone

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, BlobServiceClient
import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq

from shared.gtfs_parsers import parse_vehiclepositions

ACCOUNT_URL = os.getenv("ACCOUNT_URL")
SILVER_CONTAINER = os.getenv("SILVER_CONTAINER", "silver")

def _upload_parquet(dst_path: str, table: pa.Table):
    cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    bsc = BlobServiceClient(account_url=ACCOUNT_URL, credential=cred)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    bsc.get_container_client(SILVER_CONTAINER).upload_blob(name=dst_path, data=buf, overwrite=False)

def main(event: func.EventGridEvent):
    """Event Grid trigger: process VehiclePositions .pb file and write parquet to Silver."""
    try:
        data = event.get_json()
        blob_url = data["url"]
        logging.info("EventGrid VehiclePositions trigger: %s", blob_url)

        cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
        blob = BlobClient.from_blob_url(blob_url, credential=cred)
        payload = blob.download_blob().readall()

        rows = parse_vehiclepositions(payload)
        if not rows:
            logging.info("VehiclePositions: no rows parsed for %s", blob_url)
            return

        df = pd.DataFrame(rows)
        dt = datetime.now(timezone.utc).date().isoformat()
        df["dt"] = pd.to_datetime(dt).dt.date
        table = pa.Table.from_pandas(df, preserve_index=False)

        stamp = int(datetime.now(timezone.utc).timestamp())
        dst = f"clean/gtfsrt/vehiclepositions/dt={dt}/part-{stamp}.parquet"
        _upload_parquet(dst, table)
        logging.info("✅ VehiclePositions: wrote %d rows to %s", len(df), dst)

    except Exception as ex:
        logging.exception("❌ VehiclePositions: failed to process event: %s", ex)
        raise
