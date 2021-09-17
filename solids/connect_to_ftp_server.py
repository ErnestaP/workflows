from dagster import solid
import ftputil

from utils.ftp_connection import ftp_session_factory


@solid(required_resource_keys={"ftp"})
def connect_to_ftp_server(context) -> ftputil.FTPHost:
    parameters = context.resources.ftp.values()
    ftp_host = ftputil.FTPHost(*parameters, session_factory = ftp_session_factory)
    ftp_host.use_list_a_option = False

    context.log.info('FTP connection established.')

    return ftp_host
