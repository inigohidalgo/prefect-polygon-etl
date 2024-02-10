import prefect as pf
import prefect.blocks as pfbs

@pf.flow
def save_prefect_secret(secret_name: str, secret_value: str):
    pfbs.system.Secret(value=secret_value).save(secret_name)

if __name__ == "__main__":
    save_prefect_secret.serve(name="save-prefect-secret")