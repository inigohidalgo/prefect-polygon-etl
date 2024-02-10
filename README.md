# ETL from polygon.io using prefect

Setup

## Start prefect server

\#TODO

### Verify installation

`python src/flows/examples/github_stats.py`

This should set up a deployment in the server which will query the number of stars in a repo.

## Set up credentials

Run `src/save_prefect_secret.py` to serve a prefect deployment which will save a secret to the prefect server.

Through the UI, run the flow and save a secret with name `polygon-api-key` and the value of your API key from polygon.io