from datetime import timedelta

import prefect as pf

from environment import save_prefect_secret
from examples import github_stats
from polygon_elt import get_aggregates


if __name__ == "__main__":
    pf.serve(
        save_prefect_secret.to_deployment(name="save-prefect-secret"),
        github_stats.get_deployment(),
        get_aggregates.get_aggregates.to_deployment(name="get-aggregates"),
    )