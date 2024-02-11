import prefect as pf
from .client import get_polygon_client
import datetime
import datetime
from polygon import RESTClient

def get_daily_data(ticker, date_from, date_to, client: RESTClient):
    return client.get_aggs(ticker, 1, "day", date_from, date_to)


@pf.flow
def get_aggregates(ticker, date_from: datetime.date, date_to: datetime.date):
    logger = pf.get_run_logger()
    client = get_polygon_client()
    logger.info(get_daily_data(ticker, date_from, date_to, client))

if __name__=="__main__":
    get_aggregates.serve(name="get-aggregates")