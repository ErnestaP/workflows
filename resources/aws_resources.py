from dagster import Field, String, resource


@resource(config_schema={"aws_access_key_id": Field(String),
                         "aws_secret_access_key": Field(String),
                         "endpoint_url": Field(String)})
def aws_resource(context):
    return context.resource_config


