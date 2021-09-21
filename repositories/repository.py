from dagster import repository

from pipelines.test import test
from collect_save_and_upload_data.solids.uploading_files_to_s3 import uploading_files_to_s3


@repository
def main_repository():
    return[test,
           uploading_files_to_s3]
