-- Ngày thay đổi	:
-- Người thay đổi	:
-- Phiên bản 		:
-- Mô tả			: Stream init data từ topic của GG

CREATE OR REPLACE STREAM FBNK_{TABLE_NAME}_INIT (
    ROWKEY STRING KEY,
    RECID STRING, 
    `TABLE` STRING,
    SCN STRING,
    OP_TYPE STRING,
    OP_TS STRING,
    CURRENT_TS STRING,
    POS STRING,
    XID STRING, 
    LOOKUP_KEY STRING,
    XMLRECORD STRING)
WITH (FORMAT='avro', KAFKA_TOPIC='FBNK_{TABLE_NAME}_INIT');