import prefect.blocks.system as pfbs
from polygon import RESTClient


polygon_key = pfbs.Secret.load("polygon-api-key").get()

def get_polygon_client():
    return RESTClient(polygon_key)