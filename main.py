import os
import shutil
from utils import read_file_content, read_env_file
from ddl import create_ddl
from stream import create_stream
from connector import create_connector


def main(table_index, table_name):
    env_file_content = (
        read_file_content(".env")
        .replace("{INDEX}", table_index)
        .replace("{TABLE_NAME}", table_name)
        .split("\n")
    )
    read_env_file(env_file_content)

    if create_stream(table_name) != False:
        # DDL
        create_ddl(table_name)

        # CONNECTOR
        create_connector(table_name)

        # EXCEL
        EXCEL_FILE_NAME = "Checklist-golive-{TABLE_NAME}.xlsx"
        shutil.copy(
            "template/{}/{}".format(os.environ["TEMPLATE_TYPE"], EXCEL_FILE_NAME),
            "{}/{}".format(os.environ["BUILD_PATH"], EXCEL_FILE_NAME).replace(
                "{TABLE_NAME}", table_name
            ),
        )


if __name__ == "__main__":
    BUILD_FOLDER = "build"
    if os.path.exists(BUILD_FOLDER):
        shutil.rmtree(BUILD_FOLDER)

    for table_info in read_file_content("data/table_name.txt").split("\n"):
        if table_info:
            table_index, table_name = table_info.split(".")
            main("{:02d}".format(int(table_index.strip())), table_name.strip())
