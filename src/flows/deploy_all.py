import prefect as pf

from environment import save_prefect_secret

if __name__ == "__main__":
    pf.serve(
        save_prefect_secret.to_deployment(name="save-prefect-secret")
    )