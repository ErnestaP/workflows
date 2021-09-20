import os
from dagster import solid, InputDefinition
from dagster_types import FTPDagsterType

from constants import LOCAL_FOLDER_FOR_DOWNLOADED_FILES, DUMMY_FILES_NAMES


@solid(required_resource_keys={"ftp"},
       input_defs=[InputDefinition(name='ftp', dagster_type=FTPDagsterType)])
def uploading_files_to_ftp(context, ftp):
    ftp_folder = context.resources.ftp["ftp_folder"]
    # creating dummy file for uploading to the server
    for file_name in DUMMY_FILES_NAMES:
        f = open(file_name, "w")
        f.close()

    if os.path.exists(os.path.join(os.getcwd(), LOCAL_FOLDER_FOR_DOWNLOADED_FILES)):
        context.log.warn(f'{LOCAL_FOLDER_FOR_DOWNLOADED_FILES} folder already exists')
        pass
    else:
        os.mkdir(LOCAL_FOLDER_FOR_DOWNLOADED_FILES)

    # make sure that you're in root dir
    ftp.chdir('/')
    pwd = ftp.getcwd()
    if ftp.path.exists(ftp.path.join(pwd, ftp_folder)):
        context.log.warn(f'{ftp_folder} already exists')
        pass
    else:
        ftp.mkdir(ftp_folder)
        context.log.info(f'Created {ftp_folder}')

    ftp.chdir(ftp_folder)
    path = ftp.getcwd()

    # uploading dummy test file to ftp server
    for file_name in DUMMY_FILES_NAMES:
        ftp.upload_if_newer(file_name, file_name)

    dirs_list = ftp.listdir(path)

    context.log.info(f'Dummy file was moved to {path}: files/dirs there: {str(dirs_list)}')
