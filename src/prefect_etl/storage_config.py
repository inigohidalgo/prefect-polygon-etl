from typing import Optional
from prefect.blocks.core import Block
from pydantic import SecretStr
import s3fs

MINIO_CREDENTIAL_SECRET_KEY = "minio-credentials"


class MinIOCredentials(Block):
    aws_access_key_id: Optional[SecretStr] = None
    aws_secret_access_key: Optional[SecretStr] = None
    endpoint_url: Optional[str] = None

    @property
    def s3fs_kwargs(self):
        return {
            "key": self.aws_access_key_id.get_secret_value(),
            "secret": self.aws_secret_access_key.get_secret_value(),
            "client_kwargs": {"endpoint_url": self.endpoint_url},
        }

    @property
    def s3fs(self):
        return s3fs.S3FileSystem(**self.s3fs_kwargs)

    @property
    def rust_s3_kwargs(self):
        return {
            "endpoint": self.endpoint_url,
            "access_key_id": self.aws_access_key_id.get_secret_value(),
            "secret_access_key": self.aws_secret_access_key.get_secret_value(),
            "region": "us-east-1",
            "allow_http": "true" if self.endpoint_url.startswith("http") else "false",
        }
