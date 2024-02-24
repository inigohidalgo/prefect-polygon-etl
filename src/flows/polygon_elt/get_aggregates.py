import json

import prefect as pf
from .client import get_polygon_client
import datetime
import datetime
from polygon import RESTClient
import prefect.blocks.system as pfbs
import s3fs
import polars as pl
from delta_rs_etl.upsert import upsert

from enum import Enum

class BronzeConstants(Enum):
    TIMESTAMP_INSERTION_COLUMN = "ingestion_timestamp"


def get_daily_data(ticker, date_from, date_to, client: RESTClient):
    return client.get_aggs(ticker, 1, "day", date_from, date_to)



raw_fs = s3fs.S3FileSystem(
    key=pfbs.Secret.load("minio-access-key").get(),
    secret=pfbs.Secret.load("minio-secret-key").get(),
    client_kwargs={"endpoint_url": "http://localhost:9000"},
)


pl_s3_delta_config = {
    "aws_endpoint": "http://localhost:9000",
    "access_key": pfbs.Secret.load("minio-access-key").get(),
    "secret_key": pfbs.Secret.load("minio-secret-key").get(),
    "region": "us-east-1",
    "allow_http": True,
}

@pf.task
def aggregations_to_bronze(aggregations, ticker):
    """Add query metadata to the table"""
    return (
        pl.DataFrame(aggregations)
        .with_columns(
            pl.lit(ticker).alias("ticker"),
            pl.lit(datetime.datetime.utcnow()).alias(BronzeConstants.TIMESTAMP_INSERTION_COLUMN.value),
            )
        )

@pf.flow
def load_aggregates_to_bronze(ticker, date_from: datetime.date, date_to: datetime.date, container: str = "etl/polygon/raw"):
    logger = pf.get_run_logger()
    client = get_polygon_client()
    daily_aggs = get_daily_data(ticker, date_from, date_to, client)
    logger.info(f"Got {len(daily_aggs)} daily aggregates for {ticker} from {date_from} to {date_to}")
    
    daily_aggs_pl = aggregations_to_bronze(daily_aggs, ticker)

    daily_aggs_pl.write_parquet(
        f"s3://{container}/{ticker}/daily_aggs/{date_from}_{date_to}.parquet",
        use_pyarrow=True,
        pyarrow_options={"filesystem": raw_fs},

    )


if __name__=="__main__":
    load_aggregates_to_bronze.serve(name="get-aggregates")