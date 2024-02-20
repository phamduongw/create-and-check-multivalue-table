import os
import time
from services import list_streams_extended
from utils import read_env_file, write_to_file, read_file_content


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


def get_stream_flow(final_stream):
    stream_flow = []

    def get_stream_flow_item(pre_stream):
        stream_flow.append(pre_stream)

        for stream_info in ALL_STREAMS_AND_TOPICS:
            for sink in stream_info["sinks"]:
                if sink == pre_stream:
                    get_stream_flow_item(stream_info["name"])

    get_stream_flow_item("DW_{}".format(final_stream.strip()))

    return stream_flow[::-1]


def get_statement_of_stream(stream_name):
    DESCRIBE_FILE_NAME = "DESCRIBE-DW_{TABLE_NAME}.sql"
    for stream_info in ALL_STREAMS_AND_TOPICS:
        if stream_info["name"] == stream_name:
            with open(
                "{}/{}".format(
                    os.environ["BUILD_PATH"],
                    DESCRIBE_FILE_NAME.replace("{TABLE_NAME}", TABLE_NAME),
                ),
                "a+",
            ) as file:
                file.write(stream_info["statement"] + "\n\n")


def create_stream(final_stream):
    global ALL_STREAMS_AND_TOPICS
    ALL_STREAMS_AND_TOPICS = get_all_streams_and_topics()
    global TABLE_NAME
    TABLE_NAME = os.environ["TABLE_NAME"]
    global INDEX
    INDEX = os.environ["INDEX"]

    stream_flow = get_stream_flow(final_stream)

    if len(stream_flow) == 1:
        message = "NOT FOUND!"
    elif len(stream_flow) == 3:
        template_type = "single-value"
    elif len(stream_flow) == 4:
        template_type = "multi-value"
    else:
        message = "ERROR!"

    if "message" in locals():
        print("-- {}\n{}\n".format(stream_flow[0], message))
        return False

    if "template_type" in locals():
        env_file_content = (
            read_file_content(".env")
            .replace("{TEMPLATE_TYPE}", template_type)
            .replace("{INDEX}", INDEX)
            .replace("{TABLE_NAME}", TABLE_NAME)
            .split("\n")
        )
        read_env_file(env_file_content)

    CURRENT_TIME = time.strftime("%d/%m/%Y", time.gmtime(time.time() + 7 * 3600))
    STREAM_FILE_NAME = "{INDEX}.DW_{TABLE_NAME}.sql"

    write_to_file(
        "{}/{}".format(os.environ["BUILD_PATH"], STREAM_FILE_NAME)
        .replace("{INDEX}", INDEX)
        .replace("{TABLE_NAME}", TABLE_NAME),
        read_file_content(
            "template/{}/{}".format(os.environ["TEMPLATE_TYPE"], STREAM_FILE_NAME)
        )
        .replace("{CURRENT_TIME}", CURRENT_TIME)
        .replace("{TABLE_NAME}", TABLE_NAME),
    )

    for stream_name in stream_flow:
        get_statement_of_stream(stream_name)
