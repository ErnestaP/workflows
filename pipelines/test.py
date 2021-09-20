from dagster import pipeline, PresetDefinition, execute_pipeline, ModeDefinition

from utils.paths import get_ftp_resources_yaml_path
from resources.ftp_resources import ftp_resource
from resources.aws_resources import aws_resource
from collect_save_and_upload_data.composite_solids.get_files_from_ftp_and_save_to_s3 import get_files_from_ftp_and_save_to_s3
from collect_save_and_upload_data.solids.download_file_from_s3 import download_file_from_s3
from collect_save_and_upload_data.solids.crawler_parser import crawler_parser
from steps.composite_solids.run_all_steps import run_all_steps

path_of_ftp_resources_yaml = get_ftp_resources_yaml_path('ftp_resources.yaml')
path_of_ftp_resources_test_yaml = get_ftp_resources_yaml_path('ftp_resources_dev.yaml')
path_of_aws_resources_yaml = get_ftp_resources_yaml_path('aws_resources_dev.yaml')
path_of_aws_resources_test_yaml = get_ftp_resources_yaml_path('aws_resources_prod.yaml')


@pipeline(
    mode_defs=[
        ModeDefinition(
            name="prod",
            resource_defs={"ftp": ftp_resource, "aws": aws_resource}
        ),
        ModeDefinition(
            name="dev",
            resource_defs={"ftp": ftp_resource, "aws": aws_resource}
        )
    ],
    preset_defs=[
        PresetDefinition.from_files(
            "prod",
            config_files=[
                path_of_ftp_resources_yaml,
                path_of_aws_resources_yaml
            ],
            mode="prod",
        ),
        PresetDefinition.from_files(
            "dev",
            config_files=[
                path_of_ftp_resources_test_yaml,
                path_of_aws_resources_test_yaml
            ],
            mode="dev",
        ),
    ],

)
def test():
    all_s3_keys = get_files_from_ftp_and_save_to_s3()

    # some files might be not downloaded, if at least one thread of download_file_from_s3 will fail
    downloaded_files_with_s3_keys = all_s3_keys.map(download_file_from_s3)

    file_names = downloaded_files_with_s3_keys.map(crawler_parser)

    # steps
    file_names.map(run_all_steps)


if __name__ == "__main__":
    # start_presets_main
    result = execute_pipeline(test, preset="dev")
    # end_presets_main
    assert result.success
