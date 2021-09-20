from dagster import composite_solid, DynamicOutputDefinition, DynamicOutput, String

from solids.connect_to_ftp_server import connect_to_ftp_server
from solids.collect_files_to_download import collect_files_to_download
from solids.create_dummy_dir import create_dummy_dir
from solids.dowloanda_file_from_ftp import download_a_file_from_ftp
from solids.uploading_files_to_s3 import uploading_files_to_s3
from utils.generators import generate_mapping_key


@composite_solid(output_defs=[DynamicOutputDefinition(String
                                                      )])
def get_files_from_ftp_and_save_to_s3():
    ftp = connect_to_ftp_server()
    # for testing reasons
    start = create_dummy_dir(ftp)
    collected_files = collect_files_to_download(ftp, start)
    all_results = collected_files.map(download_a_file_from_ftp)
    s3_keys = uploading_files_to_s3(all_results.collect())
    return s3_keys

