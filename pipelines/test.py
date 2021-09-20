from dagster import pipeline, PresetDefinition, execute_pipeline, ModeDefinition, file_relative_path

from utils.paths import get_ftp_resources_yaml_path
from resources.ftp_resources import ftp_resource
from resources.aws_resources import aws_resource
from composite_solids.get_files_from_ftp_and_save_to_s3 import get_files_from_ftp_and_save_to_s3
from solids.display_content import display_content
from solids.download_file_from_s3 import download_file_from_s3

path_of_ftp_resources_yaml = get_ftp_resources_yaml_path('ftp_resources.yaml')
path_of_ftp_resources_test_yaml = get_ftp_resources_yaml_path('ftp_resources_dev.yaml')
path_of_aws_resources_yaml = get_ftp_resources_yaml_path('aws_resources_dev.yaml')
path_of_aws_resources_test_yaml = get_ftp_resources_yaml_path('aws_resources_prod.yaml')


@pipeline(
    mode_defs=[
        ModeDefinition(
            name="prod",
            resource_defs={ "ftp": ftp_resource,"aws": aws_resource}
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
    s3_keys = get_files_from_ftp_and_save_to_s3()
    s3_keys.map(download_file_from_s3)


if __name__ == "__main__":
    # start_presets_main
    result = execute_pipeline(test, preset="dev")
    # end_presets_main
    assert result.success
