from dagster import solid
import ftplib


@solid(required_resource_keys={"ftp"})
def connect_to_ftp_server(context) -> ftplib.FTP:
    parameters = context.resources.ftp.values()
    ftp = ftplib.FTP(*parameters)
    ftp.encoding = "utf-8"
    return ftp



