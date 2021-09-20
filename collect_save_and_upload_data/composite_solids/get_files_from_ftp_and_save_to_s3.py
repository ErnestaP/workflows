from dagster import composite_solid, DynamicOutputDefinition, String

from collect_save_and_upload_data.solids.connect_to_ftp_server import connect_to_ftp_server
from collect_save_and_upload_data.solids.collect_files_to_download import collect_files_to_download
from collect_save_and_upload_data.solids.uploading_files_to_ftp import uploading_files_to_ftp
from collect_save_and_upload_data.solids.dowloanda_file_from_ftp import download_a_file_from_ftp
from collect_save_and_upload_data.solids.uploading_files_to_s3 import uploading_files_to_s3


@composite_solid(output_defs=[DynamicOutputDefinition(String)])
def get_files_from_ftp_and_save_to_s3():
    ftp = connect_to_ftp_server()
    # for testing reasons
    start = uploading_files_to_ftp(ftp)
    collected_files = collect_files_to_download(ftp, start)
    all_results = collected_files.map(download_a_file_from_ftp)
    s3_keys = uploading_files_to_s3(all_results.collect())
    return s3_keys

