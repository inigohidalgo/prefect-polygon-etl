import prefect.blocks.system as pfbs
from polygon import RESTClient



def get_polygon_client():
    polygon_key = pfbs.Secret.load("polygon-api-key").get()
    return RESTClient(polygon_key)