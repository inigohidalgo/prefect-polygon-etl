import json

import prefect as pf
from polygon_api.client import get_polygon_client
from prefect_etl.storage_config import MinIOCredentials
import datetime
import datetime
from polygon import RESTClient
import prefect.blocks.system as pfbs
import s3fs
import polars as pl
from delta_rs_etl.upsert import upsert
from deltalake import DeltaTable, write_deltalake
import os
from enum import Enum

os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
class BronzeConstants(Enum):
    TIMESTAMP_INSERTION_COLUMN = "ingestion_timestamp"

MINIO_CREDENTIAL_SECRET_KEY = "minio-credentials"
# Config

def get_daily_data(ticker, date_from, date_to, client: RESTClient):
    return client.get_aggs(ticker, 1, "day", date_from, date_to)


# Bronze

def transform_raw_aggregations_to_bronze(aggregations, ticker):
    """Add query metadata to the table"""
    return (
        pl.DataFrame(aggregations)
        .with_columns(
            pl.lit(ticker).alias("ticker"),
            pl.lit(datetime.datetime.utcnow()).alias(BronzeConstants.TIMESTAMP_INSERTION_COLUMN.value),
            )
        )


def bronze_query_parameters_to_path(ticker, date_from, date_to, container="etl/polygon/raw"):
    return f"s3://{container}/{ticker}/daily_aggs/{date_from}_{date_to}.parquet"


@pf.task
def aggregates_load_raw_and_transform_to_bronze(ticker, date_from: datetime.date, date_to: datetime.date, container: str = "etl/polygon/raw"):
    logger = pf.get_run_logger()
    client = get_polygon_client()
    daily_aggs = get_daily_data(ticker, date_from, date_to, client)
    logger.info(f"Got {len(daily_aggs)} daily aggregates for {ticker} from {date_from} to {date_to}")
    
    daily_aggs_pl = transform_raw_aggregations_to_bronze(daily_aggs, ticker)
    return daily_aggs_pl

@pf.task
def aggregates_save_bronze(daily_aggs_pl, ticker, date_from, date_to, container: str = "etl/polygon/raw"):
    save_path = bronze_query_parameters_to_path(ticker, date_from, date_to, container)
    logger = pf.get_run_logger()
    logger.info(f"Saving daily aggregates to {save_path}")
    daily_aggs_pl.write_parquet(
        save_path,
        use_pyarrow=True,
        pyarrow_options={"filesystem": MinIOCredentials.load(MINIO_CREDENTIAL_SECRET_KEY).s3fs},

    )
    return save_path

@pf.flow
def aggregates_raw_to_bronze(ticker, date_from: datetime.date, date_to: datetime.date, container: str = "etl/polygon/raw"):
    daily_aggs = aggregates_load_raw_and_transform_to_bronze(ticker, date_from, date_to, container)
    save_path = aggregates_save_bronze(daily_aggs, ticker, date_from, date_to, container)
    return daily_aggs

@pf.task
def aggregates_load_bronze(ticker, date_from, date_to, container="etl/polygon/raw"):
    minio_credentials = MinIOCredentials.load(MINIO_CREDENTIAL_SECRET_KEY)

    return pl.scan_parquet(
        bronze_query_parameters_to_path(ticker, date_from, date_to, container),
        **minio_credentials.rust_s3_kwargs,
    )

# Silver

@pf.task
def aggregates_transform_bronze_to_silver(aggregates):
    # drop duplicates
    return aggregates

@pf.task
def aggregates_save_silver(aggregates, ticker, date_from, date_to, container: str = "etl/polygon/silver"):
    s3_path_silver = f"s3://{container}/daily_aggs/"
    pf.get_run_logger().info(f"Saving {len(aggregates)} records to {s3_path_silver}")
    minio_credentials = MinIOCredentials.load(MINIO_CREDENTIAL_SECRET_KEY)
    try:
        dt = DeltaTable(s3_path_silver, storage_options=minio_credentials.rust_s3_kwargs)
        upsert(aggregates.to_arrow(), dt, ["ticker", "timestamp"])
        pf.get_run_logger().info(f"Upserted {len(aggregates)} records to {s3_path_silver}")
    except Exception as e:
        if "no log files" in str(e):
            logger = pf.get_run_logger()
            logger.info("No log files found, creating new DeltaTable")
            write_deltalake(s3_path_silver, storage_options=minio_credentials.rust_s3_kwargs, data=aggregates.to_arrow())
            dt = DeltaTable(s3_path_silver, storage_options=minio_credentials.rust_s3_kwargs)
    return dt


def aggregates_bronze_to_silver(ticker, date_from, date_to, container: str = "etl/polygon/silver"):
    aggregates_bronze = aggregates_load_bronze(ticker, date_from, date_to, container)
    aggregates_silver = aggregates_transform_bronze_to_silver(aggregates_bronze)
    aggregates_save_silver(aggregates_silver, ticker, date_from, date_to, container)

@pf.flow
def aggregates_raw_to_silver(ticker, date_from: datetime.date, date_to: datetime.date, container: str = "etl/polygon/"):
    aggregates_bronze = aggregates_raw_to_bronze(ticker, date_from, date_to, container + "bronze")
    aggregates_silver = aggregates_transform_bronze_to_silver(aggregates_bronze)
    aggregates_save_silver(aggregates_silver, ticker, date_from, date_to, container + "silver")

if __name__=="__main__":
    date_to = datetime.date.today()
    date_from = date_to - datetime.timedelta(days=30)
    aggregates_raw_to_silver("AAPL", date_from, date_to)