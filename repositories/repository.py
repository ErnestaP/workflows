from dagster import repository

from pipelines.test import test


@repository
def main_repository():
    return[test]