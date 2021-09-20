from dagster import solid, InputDefinition, String


@solid(input_defs=[InputDefinition(name="file_name", dagster_type=String)])
def crawler_parser(context, file_name):
    context.log.info(f'pseudo parsing file {file_name}')
    return file_name
