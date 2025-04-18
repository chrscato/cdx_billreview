import os
import boto3
import json
from dotenv import load_dotenv

load_dotenv()  # pulls AWS_* and S3_BUCKET into os.environ

# Initialize once
_S3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION"),
)
_BUCKET = os.getenv("S3_BUCKET")


def list_objects(prefix: str):
    """List all object keys in the bucket under a prefix."""
    paginator = _S3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def download(key: str, local_path: str):
    """Download a single S3 key to a local file path."""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    _S3.download_file(_BUCKET, key, local_path)
    return local_path


def upload(local_path: str, key: str):
    """Upload a local file to S3 under the given key."""
    _S3.upload_file(local_path, _BUCKET, key)


def move(src_key: str, dest_key: str):
    """Move (copy + delete) an object within the bucket."""
    _S3.copy_object(Bucket=_BUCKET,
                    CopySource={"Bucket": _BUCKET, "Key": src_key},
                    Key=dest_key)
    _S3.delete_object(Bucket=_BUCKET, Key=src_key)


def get_s3_json(key: str) -> dict:
    """Get JSON data from an S3 object."""
    response = _S3.get_object(Bucket=_BUCKET, Key=key)
    return json.loads(response['Body'].read().decode('utf-8'))


def upload_json_to_s3(data: dict, key: str):
    """Upload JSON data directly to S3 without creating a local file."""
    json_str = json.dumps(data, indent=2)
    _S3.put_object(
        Bucket=_BUCKET,
        Key=key,
        Body=json_str.encode('utf-8'),
        ContentType='application/json'
    )
