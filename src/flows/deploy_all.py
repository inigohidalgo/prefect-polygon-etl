from datetime import timedelta

import prefect as pf

from environment import save_prefect_secret, save_minio_credentials
from polygon_elt import get_aggregates


if __name__ == "__main__":
    pf.serve(
        save_prefect_secret.to_deployment(name="save-prefect-secret"),
        save_minio_credentials.to_deployment(name="save-minio-credentials"),
        get_aggregates.aggregates_raw_to_silver.to_deployment(name="aggregates-raw-to-silver"),
        get_aggregates.aggregates_from_list_of_tickers_raw_to_bronze.to_deployment(name="aggregates-from-list-of-tickers-raw-to-bronze"),
    )