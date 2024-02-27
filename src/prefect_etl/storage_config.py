from typing import Optional
from prefect.blocks.core import Block
from pydantic import SecretStr

class MinIOCredentials(Block):
    aws_access_key_id: Optional[SecretStr] = None
    aws_secret_access_key: Optional[SecretStr] = None
    endpoint_url: Optional[str] = None

