-- Ngày thay đổi	: {CURRENT_TIME}
-- Người thay đổi	:
-- Phiên bản 		  :
-- Mô tả			    :


CREATE OR REPLACE STREAM FBNK_{TABLE_NAME} (
    ROWKEY STRING KEY,
    LOOKUP_KEY STRING,
    RECID STRING,
    -- Để ý xem có phải bảng his hay không nếu có thêm trường recver (Note cho BNH: Sửa xong thì xoá) --
    `TABLE` STRING,
    SCN STRING,
    OP_TYPE STRING,
    OP_TS STRING,
    CURRENT_TS STRING,
    POS STRING,
    XID STRING,
    XMLRECORD STRING)
WITH (FORMAT='avro', KAFKA_TOPIC='FBNK_{TABLE_NAME}');


-----set properties khi không cần parse những giá trị multivalue bị trống-----
set 'ksql.functions._global_.xml.parser.pad.empty.tags'='false'
-----set properties khi muốn parse multivalue
set 'ksql.functions._global_.xml.parser.add.multivalue.index'='true';
---- set giá trị single append vào muilti -----
set 'ksql.functions._global_.xml.parser.append.sv.to.multivalue'='true' ;

CREATE OR REPLACE STREAM FBNK_{TABLE_NAME}_MAPPED WITH (KAFKA_TOPIC='FBNK_{TABLE_NAME}_MAPPED',PARTITIONS=1) AS SELECT
  DATA.ROWKEY ROWKEY,
  DATA.LOOKUP_KEY LOOKUP_KEY,
  DATA.RECID RECID,
  -- Để ý xem có phải bảng his hay không nếu có thêm trường recver (Note cho BNH: Sửa xong thì xoá) --
  DATA.OP_TS OP_TS,  
  DATA.CURRENT_TS REP_TS,  
  TIMESTAMPTOSTRING(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss.SSSSSS') CURRENT_TS,
  DATA.`TABLE` TABLE_NAME,
  DATA.SCN COMMIT_SCN,  
  DATA.OP_TYPE COMMIT_ACTION, 
  PARSE_T24_RECORD(DATA.XMLRECORD, 'MAP_SS_FBNK_{TABLE_NAME}-value', '#') XMLRECORD, -- chọn delimiter nào ko xuất hiện trong value gốc VD:# --
  (CASE WHEN ((SCP.IS_COB_COMPLETED = true) AND (CAST(DATA.SCN AS BIGINT) > SCP.COMMIT_SCN)) THEN  SCP.TODAY  ELSE SCP.LAST_WORKING_DAY  END) BANKING_DATE 
FROM FBNK_{TABLE_NAME}  DATA
LEFT JOIN FBNK_SEAB_COB_PROCESS_TRANS_HIGH SCP ON ((SCP.ROWKEY = DATA.LOOKUP_KEY)) -- bên team Seab tự kiểm tra xem cùng partition join hay chưa --
PARTITION BY DATA.ROWKEY
EMIT CHANGES;


CREATE OR REPLACE STREAM DW_{TABLE_NAME} WITH (KAFKA_TOPIC='DW_{TABLE_NAME}',PARTITIONS=1) AS SELECT
  DATA.ROWKEY ROWKEY,
  DATA.LOOKUP_KEY LOOKUP_KEY,
  DATA.RECID RECID,
  -- Để ý xem có phải bảng his hay không nếu có thêm trường recver (Note cho BNH: Sửa xong thì xoá) --
  DATA.OP_TS OP_TS,
  DATA.REP_TS REP_TS,
  TIMESTAMPTOSTRING(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.SSSSSS') CURRENT_TS,
  PARSE_TIMESTAMP(DATA.OP_TS, 'yyyy-MM-dd HH:mm:ss.SSSSSS') COMMIT_TS,
  PARSE_TIMESTAMP(DATA.REP_TS, 'yyyy-MM-dd HH:mm:ss.SSSSSS') REPLICAT_TS,
  PARSE_TIMESTAMP(TIMESTAMPTOSTRING(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.SSSSSS'), 'yyyy-MM-dd HH:mm:ss.SSSSSS') STREAM_TS,
  DATA.TABLE_NAME TABLE_NAME,
  PARSE_DATE(DATA.BANKING_DATE, 'yyyyMMdd') BANKING_DATE,
  CAST(DATA.COMMIT_SCN AS BIGINT) COMMIT_SCN,
  DATA.COMMIT_ACTION COMMIT_ACTION,
--{FIELD_NAMES},
FROM FBNK_{TABLE_NAME}_MAPPED DATA
EMIT CHANGES;