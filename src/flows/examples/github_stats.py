import httpx
import prefect as pf
from datetime import timedelta


def get_url(url: str, params: dict = None):
    response = httpx.get(url, params=params)
    response.raise_for_status()
    return response.json()


@pf.flow
def get_repo_info(repo_name: str = "PrefectHQ/prefect"):
    url = f"https://api.github.com/repos/{repo_name}"
    repo = pf.task(get_url)(url)
    logger = pf.get_run_logger()
    logger.info("%s repository statistics 🤓:", repo_name)
    logger.info(f"Stars 🌠 : %d", repo["stargazers_count"])
    logger.info(f"Forks 🍴 : %d", repo["forks_count"])



if __name__ == "__main__":
    # get_repo_info.serve(name="my-first-deployment")
    my_repo_check = get_repo_info.to_deployment(interval=timedelta(minutes=30), name="my-repo-info", tags=["github", "api"], parameters={"repo_name": "inigohidalgo/kedro-ibis-dataset"})
    pf.serve(my_repo_check)

