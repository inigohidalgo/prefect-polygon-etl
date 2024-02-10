from datetime import timedelta

import prefect as pf

from environment import save_prefect_secret
from examples import github_stats

if __name__ == "__main__":
    pf.serve(
        save_prefect_secret.to_deployment(name="save-prefect-secret"),
        github_stats.get_deployment(),
    )