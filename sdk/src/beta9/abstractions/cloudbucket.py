from dataclasses import dataclass
from typing import Optional

from ..abstractions.base import BaseAbstraction
from ..clients.gateway import CloudBucket as CloudBucketProto
from ..clients.gateway import CloudBucketConfig


@dataclass
class Credentials:
    access_key: str
    secret: str
    endpoint: Optional[str] = None


class CloudBucket(BaseAbstraction):
    def __init__(self, name: str, mount_path: str, credentials: Credentials):
        """
        Creates a CloudBucket instance.

        When your container runs, your bucket will be available at `./{name}` and `/buckets/{name}`.

        Parameters:
            name (str):
                The name of the bucket, a descriptive identifier for the data bucket.
            mount_path (str):
                The path where the bucket is mounted within the container environment.
            credentials (Credentials):
                The credentials to access the bucket. This includes the access key, secret, and endpoint. The endpoint is optional if you are connecting to an AWS S3 bucket, but required for R2 Cloud Storage.

        Example:
            ```python
            from beta9 import CloudBucket, Credentials

            TODO: fill this out
            ```

        """
        self.name = name
        self.mount_path = mount_path
        self.credentials = credentials
        self.ready = False
        self.cloudbucket_id = None
        self.mount_path = mount_path

    def export(self):
        return CloudBucketProto(
            id=self.cloudbucket_id,
            bucket_name=self.name,
            mount_path=self.mount_path,
            config=CloudBucketConfig(
                access_key=self.credentials.access_key,
                secret=self.credentials.secret,
                endpoint=self.credentials.endpoint,
            ),
        )
