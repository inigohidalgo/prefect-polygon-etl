# ETL from polygon.io using prefect

Setup

## Start prefect server

\#TODO

### Verify installation

`python src/flows/examples/github_stats.py`

This should set up a deployment in the server which will query the number of stars in a repo.

## Deploy flows

`python src/flows/deploy_all.py`

## Polygon ELT

### Set up credentials

Sign up for an account at polygon.io and set up an API key.

Through the UI, run the flow and save a secret with name `polygon-api-key` and the value of your API key from polygon.io

### Run the flow

Manually trigger the flow `get-aggregates` for a certain date range and ticker.