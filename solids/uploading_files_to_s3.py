import os

from dagster import solid, InputDefinition, Nothing
import boto3


@solid(required_resource_keys={"aws"},
       input_defs=[InputDefinition("start", Nothing)])
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

    bucket_name = 'scoap3-test-ernesta'
    pwd = os.getcwd()
    path_to_files = os.path.join(pwd, 'downloaded')
    file_names = [file_name for file_name in os.listdir(path_to_files) if
                  os.path.isfile(os.path.join(path_to_files, file_name))]
    for file_name in file_names:
        path_to_file = os.path.join(pwd, f'downloaded/{file_name}')
        key = f"dummy_files/{file_name}"

        s3_resource.Bucket(bucket_name).put_object(
            Key=key,
            Body=open(path_to_file, 'rb')
        )

    for key in s3_client.list_objects(Bucket='scoap3-test-ernesta')['Contents']:
        context.log.info(key['Key'])
