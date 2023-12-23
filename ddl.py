import os
import time
from utils import read_file_content, write_to_file


def create_ddl(table_name):
    write_to_file(
        "{}".format(os.environ["DDL_FILE"]),
        read_file_content("template/ddl/{}".format("{TABLE_NAME}_DDL_v0.1.sql"))
        .replace(
            "{CURRENT_TIME}",
            time.strftime("%d/%m/%Y", time.gmtime(time.time() + 7 * 3600)),
        )
        .replace("{TABLE_NAME}", table_name),
    )
