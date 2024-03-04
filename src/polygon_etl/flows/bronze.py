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
    BUCKET = "etl/polygon/bronze"


def bronze_query_parameters_to_path(
    ticker,
    date_from,
    date_to,
    container=Constants.BUCKET.value,
):
    return f"s3://{container}/stocks/aggs/daily/{ticker}/{date_from}_{date_to}.parquet"


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


@pf.task(retries=1, retry_delay_seconds=60)
def aggregates_load_raw_and_transform_to_bronze(
    ticker,
    date_from: datetime.date,
    date_to: datetime.date,
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
def aggregates_load_bronze(
    ticker,
    date_from,
    date_to,
    container=Constants.BUCKET.value,
):
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
    container: str = Constants.BUCKET.value,
):
    pf.get_run_logger().info(
        f"Saving BRONZE daily aggregates for {ticker} from {date_from} to {date_to}"
    )
    daily_aggs = aggregates_load_raw_and_transform_to_bronze(ticker, date_from, date_to)
    aggregates_save_bronze(daily_aggs, ticker, date_from, date_to, container)
    return daily_aggs


@pf.flow
def aggregates_from_list_of_tickers_raw_to_bronze(
    tickers: list[str],
    date_from: datetime.date,
    date_to: datetime.date,
    container: str = Constants.BUCKET.value,
    continue_on_error: bool = False,
):
    for ticker in tickers:
        try:
            aggregates_raw_to_bronze(ticker, date_from, date_to, container)
        except Exception as e:
            if continue_on_error:
                pf.get_run_logger().error(
                    f"Error while processing {ticker}: {e}. Continuing..."
                )
                continue
            else:
                raise e

    pf.get_run_logger().info(f"Loaded {len(tickers)} tickers successfully")


@pf.flow
def get_aggregates_from_preconfigured_list_of_tickers_raw_to_bronze(
    tickers: Optional[list[str]] = None,
    n_days_back: int = 30,
    container: Optional[str] = Constants.BUCKET.value,
):
    tickers = tickers or pfbs.JSON.load("polygon-daily-aggregates-tickers").value
    date_to = datetime.date.today() - datetime.timedelta(days=1)
    date_from = date_to - datetime.timedelta(days=n_days_back)
    aggregates_from_list_of_tickers_raw_to_bronze(
        tickers, date_from, date_to, container, continue_on_error=True
    )


if __name__ == "__main__":
    get_aggregates_from_preconfigured_list_of_tickers_raw_to_bronze()
