from dagster import DagsterType
from ftplib import FTP

FTPDagsterType = DagsterType(
    name="FTPServerType",
    type_check_fn=lambda _, value: isinstance(value, FTP),
)