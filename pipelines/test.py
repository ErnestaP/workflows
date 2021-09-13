from dagster import pipeline, PresetDefinition, execute_pipeline, ModeDefinition, file_relative_path
from solids.connect_to_ftp_server import connect_to_ftp_server
from solids.display_content import display_content

from utils.paths import get_ftp_resources_yaml_path
from resources.ftp_resources import ftp_resource


path_of_ftp_resources_yaml = get_ftp_resources_yaml_path('ftp_resources.yaml')
path_of_ftp_resources_test_yaml = get_ftp_resources_yaml_path('ftp_resources_dev.yaml')


@pipeline(
    mode_defs=[
        ModeDefinition(
            name="prod",
            resource_defs={"ftp": ftp_resource}
        ),
        ModeDefinition(
            name="dev",
            resource_defs={"ftp": ftp_resource}
        )
    ],
    preset_defs=[
        PresetDefinition.from_files(
            "prod",
            config_files=[
                path_of_ftp_resources_yaml
            ],
            mode="prod",
        ),
        PresetDefinition.from_files(
            "dev",
            config_files=[
                path_of_ftp_resources_test_yaml
            ],
            mode="dev",
        ),
    ],

)
def test():
    ftp = connect_to_ftp_server()
    display_content(ftp)


if __name__ == "__main__":
    # start_presets_main
    result = execute_pipeline(test, preset="dev")
    # end_presets_main
    assert result.success
