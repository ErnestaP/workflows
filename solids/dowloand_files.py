from dagster import solid, OutputDefinition

from ..hepcrawl.utils import ftp_connection_info


@solid(required_resource_keys={"ftp_host", "ftp_netrc", "ftp_user", "ftp_password"})
def download_files_from_ftp(context):
    ftp_host, ftp_params = ftp_connection_info(context.resources.ftp_host, context.resources.ftp_netrc)

