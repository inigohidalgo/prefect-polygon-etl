from enum import Enum
from typing import Optional
import polars as pl
from polygon import RESTClient
from polygon.rest.models import Agg
import datetime
import prefect as pf
import prefect.blocks.system as pfbs
import time

from polygon_etl.config.polygon_client import get_polygon_client
from polygon_etl.config.storage import MinIOCredentials, MINIO_CREDENTIAL_SECRET_KEY


class Constants(Enum):
    TIMESTAMP_INSERTION_COLUMN = "ingestion_timestamp"


def bronze_query_parameters_to_path(
    ticker, date_from, date_to, container="etl/polygon/raw"
):
    return f"s3://{container}/{ticker}/daily_aggs/{date_from}_{date_to}.parquet"


def get_daily_data(
    ticker: str,
    date_from: datetime.datetime,
    date_to: datetime.datetime,
    client: RESTClient,
):
    return client.get_aggs(ticker, 1, "day", date_from, date_to)


def transform_raw_aggregations_to_bronze(aggregations: list[Agg], ticker: str):
    """Add query metadata to the table"""
    return pl.DataFrame(aggregations).with_columns(
        pl.lit(ticker).alias("ticker"),
        pl.lit(datetime.datetime.utcnow()).alias(
            Constants.TIMESTAMP_INSERTION_COLUMN.value
        ),
    )


@pf.task
def aggregates_load_raw_and_transform_to_bronze(
    ticker,
    date_from: datetime.date,
    date_to: datetime.date,
    container: str = "etl/polygon/raw",
):
    logger = pf.get_run_logger()
    client = get_polygon_client()
    daily_aggs = get_daily_data(ticker, date_from, date_to, client)
    logger.info(
        f"Got {len(daily_aggs)} daily aggregates for {ticker} from {date_from} to {date_to}"
    )

    daily_aggs_pl = transform_raw_aggregations_to_bronze(daily_aggs, ticker)
    return daily_aggs_pl


@pf.task
def aggregates_save_bronze(
    daily_aggs_pl: pl.DataFrame,
    ticker: str,
    date_from: datetime.datetime,
    date_to: datetime.datetime,
    container: str = "etl/polygon/raw",
):
    save_path = bronze_query_parameters_to_path(ticker, date_from, date_to, container)
    logger = pf.get_run_logger()
    logger.info(f"Saving daily aggregates to {save_path}")
    daily_aggs_pl.write_parquet(
        save_path,
        use_pyarrow=True,
        pyarrow_options={
            "filesystem": MinIOCredentials.load(MINIO_CREDENTIAL_SECRET_KEY).s3fs
        },
    )
    return save_path


@pf.task
def aggregates_load_bronze(ticker, date_from, date_to, container="etl/polygon/raw"):
    minio_credentials = MinIOCredentials.load(MINIO_CREDENTIAL_SECRET_KEY)

    return pl.scan_parquet(
        bronze_query_parameters_to_path(ticker, date_from, date_to, container),
        **minio_credentials.rust_s3_kwargs,
    )


@pf.flow
def aggregates_raw_to_bronze(
    ticker: str,
    date_from: datetime.date,
    date_to: datetime.date,
    container: str = "etl/polygon/raw",
):
    pf.get_run_logger().info(
        f"Saving BRONZE daily aggregates for {ticker} from {date_from} to {date_to}"
    )
    daily_aggs = aggregates_load_raw_and_transform_to_bronze(
        ticker, date_from, date_to, container
    )
    aggregates_save_bronze(daily_aggs, ticker, date_from, date_to, container)
    return daily_aggs


@pf.flow
def aggregates_from_list_of_tickers_raw_to_bronze(
    tickers: list[str],
    date_from: datetime.date,
    date_to: datetime.date,
    container: str = "etl/polygon/raw",
    api_calls_per_minute: int = 5,
):
    logger = pf.get_run_logger()
    time_start = time.monotonic()
    n_calls = 0
    for ticker in tickers:
        logger.info("Loading daily aggregates for {ticker}")
        aggregates_raw_to_bronze(ticker, date_from, date_to, container)
        n_calls += 1
        if n_calls >= api_calls_per_minute:
            time_end = time.monotonic()
            time_elapsed = time_end - time_start
            if time_elapsed < 60:
                logger.info(
                    f"Exceeded {api_calls_per_minute} calls per minute. Sleeping for {60 - time_elapsed} seconds"
                )
                time.sleep(60 - time_elapsed)
                time_elapsed = 0
                n_calls = 0

    logger.info(f"Loaded {len(tickers)} tickers successfully")


@pf.flow
def get_aggregates_from_preconfigured_list_of_tickers_raw_to_bronze(
    tickers: Optional[list[str]] = None,
    n_days_back: int = 30,
    container: Optional[str] = "etl/polygon/raw",
    api_calls_per_minute: Optional[int] = 5,
):
    tickers = tickers or pfbs.JSON.load("polygon-daily-aggregates-tickers").value
    date_to = datetime.date.today() - datetime.timedelta(days=1)
    date_from = date_to - datetime.timedelta(days=n_days_back)
    aggregates_from_list_of_tickers_raw_to_bronze(
        tickers, date_from, date_to, container, api_calls_per_minute
    )


if __name__ == "__main__":
    get_aggregates_from_preconfigured_list_of_tickers_raw_to_bronze()
