# ETL from polygon.io using prefect

Setup

## Start prefect server

https://github.com/inigohidalgo/prefect-server-docker

### Verify installation

`python src/flows/examples/github_stats.py`

This should set up a deployment in the server which will query the number of stars in a repo.

## Deploy flows

### Set up credentials

`python src/flows/environment/__init__.py`

This will create 2 flows, `save_prefect_secret` and `save_minio_credentials`.

#### Polygon

Sign up for an account at polygon.io and set up an API key.

Run the flow and save a secret with name `polygon-api-key` and the value of your API key from polygon.io
```bash
prefect deployment run save-prefect-secret/save-prefect-secret --param secret_name=polygon-api-key --param secret_value=$POLYGON_API_KEY
```


#### Minio

Get your minio credentials and run the flow to save them as secrets.
```bash
prefect deployment run save-minio-credentials/save-minio-credentials --param minio_access_key=$MINIO_ACCESS_KEY --param minio_secret_key=$MINIO_SECRET_KEY --param host=http://localhost:9000
```

### Set up deployments

`python src/flows/deploy_all.py`

Save the following block:

JSON / polygon-daily-aggregates-tickers - ["AAPL", "MSFT"...]

Run the flow:
```bash
prefect deployment run get-aggregates-from-preconfigured-list-of-tickers-raw-to-bronze/daily-aggregates-raw-to-bronze-multiple-tickers
```

This will load the data from the list of tickers into the bronze bucket.