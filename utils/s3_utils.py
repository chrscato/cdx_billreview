import os
import boto3
import json
import time
import logging
from dotenv import load_dotenv

load_dotenv()  # pulls AWS_* and S3_BUCKET into os.environ

# Set up logging
logger = logging.getLogger(__name__)

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


def move_with_confirmation(source_key, dest_key, s3_client=None, max_retries=3):
    """
    Safely move an S3 object by copying first, then deleting the original only after
    confirming the copy was successful. Includes retries and verification.
    
    Args:
        source_key (str): S3 key of the source object
        dest_key (str): S3 key for the destination object
        s3_client (boto3.client, optional): Initialized S3 client, or None to use default
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        tuple: (success, result)
            - success (bool): True if move was successful
            - result (dict): Details about the operation
    """
    logger.info(f"Starting robust move operation: {source_key} -> {dest_key}")
    
    result = {
        'source_key': source_key,
        'dest_key': dest_key,
        'source_exists': False,
        'copy_success': False,
        'delete_success': False,
        'verification': {
            'dest_exists': False,
            'etag_match': False,
            'size_match': False
        },
        'retries': {
            'copy': 0,
            'delete': 0
        },
        'error': None,
        'warning': None
    }
    
    # Initialize S3 client if not provided
    if s3_client is None:
        s3_client = _S3
    
    bucket = _BUCKET
    
    # Check if source exists before attempting move
    try:
        source_head = s3_client.head_object(Bucket=bucket, Key=source_key)
        result['source_exists'] = True
        result['source_size'] = source_head.get('ContentLength', 0)
        result['source_etag'] = source_head.get('ETag', '').strip('"')
        logger.debug(f"Source file exists: {source_key}, Size: {result['source_size']}, ETag: {result['source_etag']}")
    except Exception as e:
        result['error'] = f"Source file does not exist or is not accessible: {str(e)}"
        logger.error(result['error'])
        return False, result
    
    # Step 1: Copy with retries
    retry_count = 0
    while retry_count < max_retries:
        try:
            # Copy the object
            copy_response = s3_client.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': source_key},
                Key=dest_key
            )
            result['copy_success'] = True
            result['copy_etag'] = copy_response.get('CopyObjectResult', {}).get('ETag', '').strip('"')
            result['retries']['copy'] = retry_count
            logger.info(f"Successfully copied: {source_key} -> {dest_key} (Attempt: {retry_count+1})")
            break
        except Exception as e:
            retry_count += 1
            wait_time = (2 ** retry_count) * 0.1  # Exponential backoff
            result['retries']['copy'] = retry_count
            logger.warning(f"Copy attempt {retry_count}/{max_retries} failed: {str(e)}, retrying in {wait_time:.2f}s")
            
            if retry_count >= max_retries:
                result['error'] = f"Failed to copy after {max_retries} attempts: {str(e)}"
                logger.error(result['error'])
                return False, result
            
            time.sleep(wait_time)
    
    # Step 2: Verify the copy
    try:
        # Check if destination exists and compare with source
        dest_head = s3_client.head_object(Bucket=bucket, Key=dest_key)
        result['verification']['dest_exists'] = True
        dest_size = dest_head.get('ContentLength', 0)
        dest_etag = dest_head.get('ETag', '').strip('"')
        
        # Compare size and etag
        result['verification']['size_match'] = (dest_size == result['source_size'])
        result['verification']['etag_match'] = (dest_etag == result['source_etag'])
        result['verification']['dest_size'] = dest_size
        result['verification']['dest_etag'] = dest_etag
        
        if not result['verification']['size_match']:
            logger.warning(f"Size mismatch: Source={result['source_size']}, Dest={dest_size}")
            
        if not result['verification']['etag_match']:
            logger.warning(f"ETag mismatch: Source={result['source_etag']}, Dest={dest_etag}")
        
        verification_passed = result['verification']['dest_exists'] and result['verification']['size_match']
        
        if verification_passed:
            logger.info(f"Verification passed for {dest_key}")
        else:
            result['error'] = "Verification failed: Size or content mismatch"
            logger.error(result['error'])
            return False, result
            
    except Exception as e:
        result['error'] = f"Copy verification failed: {str(e)}"
        logger.error(result['error'])
        return False, result
    
    # Step 3: Delete original with retries
    retry_count = 0
    while retry_count < max_retries:
        try:
            delete_response = s3_client.delete_object(
                Bucket=bucket,
                Key=source_key
            )
            result['delete_success'] = True
            result['retries']['delete'] = retry_count
            logger.info(f"Successfully deleted original: {source_key} (Attempt: {retry_count+1})")
            break
        except Exception as e:
            retry_count += 1
            wait_time = (2 ** retry_count) * 0.1  # Exponential backoff
            result['retries']['delete'] = retry_count
            logger.warning(f"Delete attempt {retry_count}/{max_retries} failed: {str(e)}, retrying in {wait_time:.2f}s")
            
            if retry_count >= max_retries:
                # Still consider this a success as the copy worked and was verified
                result['warning'] = f"Copy succeeded but delete failed after {max_retries} attempts: {str(e)}"
                logger.warning(result['warning'])
                return True, result
            
            time.sleep(wait_time)
    
    # Final check to ensure source is gone
    try:
        s3_client.head_object(Bucket=bucket, Key=source_key)
        # If we get here, the source still exists
        result['warning'] = "Delete appeared to succeed but source file still exists"
        logger.warning(result['warning'])
    except:
        # This is actually good - we expect a 404 error
        logger.debug(f"Confirmed source file no longer exists: {source_key}")
    
    logger.info(f"Move operation completed successfully: {source_key} -> {dest_key}")
    return True, result


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


def delete(key: str):
    """Delete an object from S3."""
    _S3.delete_object(Bucket=_BUCKET, Key=key)
