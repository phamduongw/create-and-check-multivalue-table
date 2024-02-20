import os
from utils import read_file_content, write_to_file


def get_connector_name(table_name):
    template = ""

    for word in table_name.split("_"):
        template += word.capitalize()

    return template


def create_connector(table_name):
    CONNECTOR_FILE_NAME = "{INDEX}.connector_T_{CONNECTOR_NAME}2Dw.json"
    CONNECTOR_NAME = get_connector_name(table_name)

    write_to_file(
        "{}/{}".format(os.environ["BUILD_PATH"], CONNECTOR_FILE_NAME)
        .replace("{INDEX}", os.environ["INDEX"])
        .replace("{CONNECTOR_NAME}", CONNECTOR_NAME),
        read_file_content(
            "template/{}/{}".format(os.environ["TEMPLATE_TYPE"], CONNECTOR_FILE_NAME)
        )
        .replace("{TABLE_NAME}", table_name)
        .replace("{CONNECTOR_NAME}", CONNECTOR_NAME),
    )
