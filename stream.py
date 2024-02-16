import os
import time
from services import list_streams_extended
from utils import create_folder, write_to_file, read_file_content


def get_all_streams_and_topics():
    all_streams_and_topics = []

    source_descriptions = list_streams_extended()[0]["sourceDescriptions"]

    for source_description in source_descriptions:
        sinks = []

        for read_query in source_description["readQueries"]:
            for sink in read_query["sinks"]:
                sinks.append(sink)

        all_streams_and_topics.append(
            {
                "name": source_description["name"],
                "sinks": sinks,
                "statement": source_description["statement"],
            }
        )

    return all_streams_and_topics


def get_stream_flow(ods_stream):
    stream_flow = []

    def get_stream_flow_item(ods_stream):
        stream_flow.append(ods_stream)

        for stream_info in ALL_STREAMS_AND_TOPICS:
            for sink in stream_info["sinks"]:
                if sink == ods_stream:
                    get_stream_flow_item(stream_info["name"])

    get_stream_flow_item(ods_stream)

    return stream_flow


def create_statement_of_stream(stream_id, stream_prefix, stream_suffix):
    TEMPLATE_STRING = "{TABLE_NAME}"
    CURRENT_TIME = time.strftime("%d/%m/%Y", time.gmtime(time.time() + 7 * 3600))

    INIT_FILE = "0.{}.INIT_{}_{}{}_v0.1.sql".format(
        stream_id,
        stream_prefix,
        TEMPLATE_STRING,
        stream_suffix,
    )
    CDC_FILE = "0.{}.{}_{}{}_v0.1.sql".format(
        stream_id,
        stream_prefix,
        TEMPLATE_STRING,
        stream_suffix,
    )

    write_to_file(
        "{}/{}".format(os.environ["INIT_STREAM_FOLDER"], INIT_FILE).replace(
            "{TABLE_NAME}", TABLE_NAME
        ),
        read_file_content("template/stream/{}".format(INIT_FILE))
        .replace("{CURRENT_TIME}", CURRENT_TIME)
        .replace("{TABLE_NAME}", TABLE_NAME),
    )
    write_to_file(
        "{}/{}".format(os.environ["CDC_STREAM_FOLDER"], CDC_FILE).replace(
            "{TABLE_NAME}", TABLE_NAME
        ),
        read_file_content("template/stream/{}".format(CDC_FILE))
        .replace("{CURRENT_TIME}", CURRENT_TIME)
        .replace("{TABLE_NAME}", TABLE_NAME),
    )


def get_statement_of_stream(stream_name):
    for stream_info in ALL_STREAMS_AND_TOPICS:
        if stream_info["name"] == stream_name:
            create_folder("/".join(os.environ["DESCRIBE_FILE"].split("/")[:-1]))
            with open(os.environ["DESCRIBE_FILE"], "a+") as file:
                file.write(stream_info["statement"] + "\n\n")


def describe_ods_stream(ods_stream):
    stream_flow = get_stream_flow("DW_{}".format(ods_stream.strip()))[::-1]

    if len(stream_flow) == 1:
        print("-- {}\n{}\n".format(stream_flow[0], "ERROR!"))
        return False

    for stream_name in stream_flow:
        get_statement_of_stream(stream_name)

    return True


def create_stream(ods_stream):
    # ALL_STREAMS_AND_TOPICS
    global ALL_STREAMS_AND_TOPICS
    ALL_STREAMS_AND_TOPICS = get_all_streams_and_topics()

    if describe_ods_stream(ods_stream):
        # TABLE NAME
        global TABLE_NAME
        TABLE_NAME = os.environ["TABLE_NAME"]

        create_statement_of_stream(1, "FBNK", "")
        create_statement_of_stream(2, "FBNK", "_MAPPED")
        create_statement_of_stream(3, "DW", "")

        return True

    return False
