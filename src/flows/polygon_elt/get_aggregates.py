import json

import prefect as pf
from .client import get_polygon_client
import datetime
import datetime
from polygon import RESTClient
import prefect.blocks.system as pfbs
import s3fs



def get_daily_data(ticker, date_from, date_to, client: RESTClient):
    return client.get_aggs(ticker, 1, "day", date_from, date_to)



raw_fs = s3fs.S3FileSystem(
    key=pfbs.Secret.load("minio-access-key").get(),
    secret=pfbs.Secret.load("minio-secret-key").get(),
    client_kwargs={"endpoint_url": "http://localhost:9000"},
)




@pf.flow
def get_aggregates(ticker, date_from: datetime.date, date_to: datetime.date, container: str = "etl/polygon/raw"):
    logger = pf.get_run_logger()
    client = get_polygon_client()
    daily_aggs = get_daily_data(ticker, date_from, date_to, client)
    logger.info(f"Got {len(daily_aggs)} daily aggregates for {ticker} from {date_from} to {date_to}")
    daily_agggs_dicts = [agg.__dict__ for agg in daily_aggs]
    raw_json_path = f"{container}/{ticker}/{date_from}__{date_to}.json"
    logger.info(f"Writing daily aggregates to {raw_json_path}")
    with raw_fs.open(raw_json_path, "w") as f:
        json.dump(daily_agggs_dicts, f)


if __name__=="__main__":
    get_aggregates.serve(name="get-aggregates")