import os
import time
from utils import read_file_content, write_to_file


def create_ddl(table_name):
    DDL_FILE_NAME = "{INDEX}.DW_{TABLE_NAME}_DDL.sql"

    write_to_file(
        "{}/{}".format(os.environ["BUILD_PATH"], DDL_FILE_NAME)
        .replace("{INDEX}", os.environ["INDEX"])
        .replace("{TABLE_NAME}", table_name),
        read_file_content(
            "template/{}/{}".format(os.environ["TEMPLATE_TYPE"], DDL_FILE_NAME)
        )
        .replace(
            "{CURRENT_TIME}",
            time.strftime("%d/%m/%Y", time.gmtime(time.time() + 7 * 3600)),
        )
        .replace("{TABLE_NAME}", table_name),
    )
