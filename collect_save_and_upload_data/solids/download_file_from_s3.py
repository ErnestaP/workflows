import os
from dagster import solid, InputDefinition, String
import boto3

from constants import LOCAL_FOLDER_FOR_DOWNLOADED_FILES_FROM_S3


def create_local_dir_for_downloaded_files(context, local_dir):
    if os.path.exists(local_dir):
        context.log.warn(f'{LOCAL_FOLDER_FOR_DOWNLOADED_FILES_FROM_S3} folder already exists')
        pass
    else:
        os.mkdir(LOCAL_FOLDER_FOR_DOWNLOADED_FILES_FROM_S3)


@solid(required_resource_keys={"aws"},
       input_defs=[InputDefinition(name="s3_key", dagster_type=String)])
def download_file_from_s3(context, s3_key):
    # creating local dir for downloaded files from s3
    local_dir = os.path.join(os.getcwd(), LOCAL_FOLDER_FOR_DOWNLOADED_FILES_FROM_S3)
    create_local_dir_for_downloaded_files(context, local_dir)

    aws_access_key_id = context.resources.aws["aws_access_key_id"]
    aws_secret_access_key = context.resources.aws["aws_secret_access_key"]
    endpoint_url = context.resources.aws["endpoint_url"]

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url
    )
    bucket_name = context.resources.aws["raw_files_bucket"]

    file_name = os.path.basename(s3_key)
    target_path = os.path.join(local_dir, file_name)
    try:
        context.log.info('Nothing')
        s3_client.download_file(bucket_name, s3_key, target_path)
    except ValueError:
        context.log.info(f'FAIL: File {s3_key} download fail')
    finally:
        context.log.info(f'OK: File {s3_key} downloaded successfully')
        return s3_key
