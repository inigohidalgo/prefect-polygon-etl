import datetime

import prefect as pf
from deltalake import DeltaTable, write_deltalake

from delta_rs_etl.upsert import upsert
from prefect_etl_config.storage_config import MinIOCredentials, MINIO_CREDENTIAL_SECRET_KEY



from flows.polygon_elt.bronze import aggregates_load_bronze, aggregates_raw_to_bronze


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

@pf.flow
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