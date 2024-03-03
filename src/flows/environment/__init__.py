import prefect as pf
from typing import Optional
import prefect.blocks as pfbs
from pydantic import SecretStr
from prefect_etl_config.storage_config import MinIOCredentials

@pf.flow
def save_prefect_secret(secret_name: str, secret_value: str):
    pfbs.system.Secret(value=secret_value).save(secret_name)

@pf.flow
def save_minio_credentials(access_key_id: SecretStr, secret_access_key: SecretStr, endpoint_url: Optional[str]=None, secret_name: str = "minio_credentials"):
    MinIOCredentials(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, endpoint_url=endpoint_url).save(secret_name)

if __name__ == "__main__":
    pf.serve(
        save_prefect_secret.to_deployment("save_prefect_secret"),
        save_minio_credentials.to_deployment("save_minio_credentials")
    )