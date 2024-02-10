import prefect as pf
import prefect.blocks as pfbs
from pydantic import SecretStr


@pf.flow
def save_prefect_secret(secret_name: str, secret_value: SecretStr):
    pfbs.system.Secret(value=secret_value).save(secret_name)

if __name__ == "__main__":
    save_prefect_secret.serve()