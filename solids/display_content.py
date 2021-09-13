from dagster import solid, InputDefinition
from dagster_types import FTPDagsterType


@solid(input_defs=[InputDefinition(name="ftp", dagster_type=FTPDagsterType)])
def display_content(context, ftp):
    context.log.info(str(ftp.dir()))

