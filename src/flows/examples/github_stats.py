import httpx
import prefect as pf
from datetime import timedelta


def get_url(url: str, params: dict = None):
    response = httpx.get(url, params=params)
    response.raise_for_status()
    return response.json()


@pf.flow
def get_repo_info(repo_name: str = "inigohidalgo/kedro-ibis-dataset"):
    url = f"https://api.github.com/repos/{repo_name}"
    repo = pf.task(get_url)(url)
    logger = pf.get_run_logger()
    logger.info("%s repository statistics 🤓:", repo_name)
    logger.info(f"Stars 🌠 : %d", repo["stargazers_count"])
    logger.info(f"Forks 🍴 : %d", repo["forks_count"])


def get_deployment(**kwargs):
    deployment_kwargs = {
        "name": "get-repo-info",
        "interval": timedelta(days=7),
    }
    deployment_kwargs.update(kwargs)

    return get_repo_info.to_deployment(**deployment_kwargs)

if __name__ == "__main__":
    pf.serve(get_deployment())

