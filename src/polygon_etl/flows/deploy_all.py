from datetime import timedelta, datetime
from prefect.client.schemas.schedules import IntervalSchedule

import prefect as pf

from environment import save_prefect_secret, save_minio_credentials
from polygon_elt import bronze, silver


if __name__ == "__main__":
    pf.serve(
        save_prefect_secret.to_deployment(name="save-prefect-secret"),
        save_minio_credentials.to_deployment(name="save-minio-credentials"),
        silver.aggregates_raw_to_silver.to_deployment(
            name="aggregates-raw-to-silver"
        ),
        bronze.get_aggregates_from_preconfigured_list_of_tickers_raw_to_bronze.to_deployment(
            name="daily-aggregates-raw-to-bronze-multiple-tickers",
            interval=timedelta(days=1),
            ),
        )

