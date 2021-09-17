import os
from dagster import solid, InputDefinition, DynamicOutputDefinition, DynamicOutput, Nothing
from dagster_types import FTPDagsterType


from utils.generators import generate_mapping_key


@solid(input_defs=[InputDefinition(name='ftp', dagster_type=FTPDagsterType),
                   InputDefinition("start", Nothing)],
       output_defs=[DynamicOutputDefinition(dict)])
def collect_files_to_download(context, ftp) -> list:
    """
    Collects all the files in under the 'ftp_folder' folder.
    Files starting with a dot (.) are omitted.
    :ftp FTPHost:
    :return: list of all found file's path
    """

    ftp_folder = '.'
    for path, dirs, files in ftp.walk(ftp_folder):
        for filename in files:
            if filename.startswith('.'):
                continue
            full_path = os.path.join(path, filename)
            if filename.endswith('.zip') or filename == 'go.xml':
                yield DynamicOutput(value={'file_path': full_path, 'ftp': ftp},
                                    mapping_key=generate_mapping_key())
            else:
                context.log.warning(f'File with invalid extension on FTP path={full_path}')
