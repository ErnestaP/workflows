import os
from dagster import solid, InputDefinition, Nothing, String, DynamicOutputDefinition, DynamicOutput
import boto3

from utils.generators import generate_mapping_key
from constants import DUMMY_FILES_SUB_KEY


@solid(required_resource_keys={"aws"},
       input_defs=[InputDefinition("start", Nothing)],
       output_defs=[DynamicOutputDefinition(String)])
def uploading_files_to_s3(context):
    aws_access_key_id = context.resources.aws["aws_access_key_id"]
    aws_secret_access_key = context.resources.aws["aws_secret_access_key"]
    endpoint_url = context.resources.aws["endpoint_url"]

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url
    )
    s3_resource = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url
    )

    bucket_name = context.resources.aws["raw_files_bucket"]

    pwd = os.getcwd()
    path_to_files = os.path.join(pwd, 'downloaded')
    file_names = [file_name for file_name in os.listdir(path_to_files) if
                  os.path.isfile(os.path.join(path_to_files, file_name))]
    for file_name in file_names:
        path_to_file = os.path.join(pwd, f'downloaded/{file_name}')
        key = f"{DUMMY_FILES_SUB_KEY}/{file_name}"

        # s3_resource.Bucket(bucket_name).put_object(
        #     Key=key,
        #     Body=open(path_to_file, 'rb')
        # )
    # return keys of files, where they were uploaded.
    # Because after this step we will use just Keys of s3 to re-download files to modify them
    for key in s3_client.list_objects(Bucket=bucket_name)['Contents']:
        # keys, which ended with '/' are dirs, we need just files
        # files, which we want to download from s3
        if not key['Key'].endswith('/') and f'{DUMMY_FILES_SUB_KEY}/' in key['Key']:
            context.log.info(key['Key'])
            yield DynamicOutput(value=key['Key'],
                                mapping_key=generate_mapping_key())
