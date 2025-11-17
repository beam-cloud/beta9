"""Terraform state backend configuration."""

import boto3
from botocore.exceptions import ClientError

from models.config import InfraModel, CloudProvider


def ensure_state_backend(infra: InfraModel) -> None:
    """
    Ensure state backend resources exist.

    For AWS:
    - Creates S3 bucket for state storage
    - Creates DynamoDB table for state locking

    Args:
        infra: Infrastructure model

    Raises:
        Exception: If backend resources cannot be created
    """
    if infra.cloud_provider == CloudProvider.AWS:
        _ensure_aws_state_backend(infra)
    elif infra.cloud_provider == CloudProvider.GCP:
        _ensure_gcp_state_backend(infra)
    else:
        raise ValueError(f"Unsupported cloud provider: {infra.cloud_provider}")


def _ensure_aws_state_backend(infra: InfraModel) -> None:
    """
    Ensure AWS state backend (S3 + DynamoDB) exists.

    Args:
        infra: Infrastructure model
    """
    # Get boto3 session with assumed role
    if infra.role_arn:
        sts = boto3.client("sts")
        assumed_role = sts.assume_role(
            RoleArn=infra.role_arn,
            RoleSessionName=f"beta9-state-setup-{infra.environment_id}",
            ExternalId=infra.external_id,
        )

        credentials = assumed_role["Credentials"]
        session = boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=infra.region,
        )
    else:
        session = boto3.Session(region_name=infra.region)

    s3 = session.client("s3")
    dynamodb = session.client("dynamodb")

    bucket_name = infra.state_backend_bucket
    table_name = f"{bucket_name}-lock"

    # Create S3 bucket
    try:
        if infra.region == "us-east-1":
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": infra.region},
            )

        # Enable versioning
        s3.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"},
        )

        # Enable encryption
        s3.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256",
                        },
                        "BucketKeyEnabled": True,
                    }
                ]
            },
        )

        # Block public access
        s3.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )

        print(f"Created S3 bucket: {bucket_name}")

    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            print(f"S3 bucket already exists: {bucket_name}")
        else:
            raise

    # Create DynamoDB table for locking
    try:
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "LockID", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "LockID", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
            Tags=[{"Key": k, "Value": v} for k, v in infra.tags.items()],
        )

        # Wait for table to be created
        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=table_name, WaiterConfig={"Delay": 2, "MaxAttempts": 30})

        print(f"Created DynamoDB table: {table_name}")

    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceInUseException":
            print(f"DynamoDB table already exists: {table_name}")
        else:
            raise


def _ensure_gcp_state_backend(infra: InfraModel) -> None:
    """
    Ensure GCP state backend (GCS bucket) exists.

    Args:
        infra: Infrastructure model
    """
    from google.cloud import storage
    from google.api_core.exceptions import Conflict

    client = storage.Client(project=infra.account_id)
    bucket_name = infra.state_backend_bucket

    try:
        bucket = client.create_bucket(bucket_name, location=infra.region)

        # Enable versioning
        bucket.versioning_enabled = True
        bucket.patch()

        print(f"Created GCS bucket: {bucket_name}")

    except Conflict:
        print(f"GCS bucket already exists: {bucket_name}")


def configure_backend(infra: InfraModel) -> dict:
    """
    Generate backend configuration for Terraform.

    Args:
        infra: Infrastructure model

    Returns:
        Backend configuration dict
    """
    if infra.cloud_provider == CloudProvider.AWS:
        return {
            "type": "s3",
            "config": {
                "bucket": infra.state_backend_bucket,
                "key": infra.state_backend_key,
                "region": infra.state_backend_region,
                "encrypt": True,
                "dynamodb_table": f"{infra.state_backend_bucket}-lock",
                "role_arn": infra.role_arn,
                "external_id": infra.external_id,
            },
        }
    elif infra.cloud_provider == CloudProvider.GCP:
        return {
            "type": "gcs",
            "config": {
                "bucket": infra.state_backend_bucket,
                "prefix": infra.state_backend_key.rstrip("/terraform.tfstate"),
            },
        }
    else:
        raise ValueError(f"Unsupported cloud provider: {infra.cloud_provider}")
