-- Ngày thay đổi	:
-- Người thay đổi	:
-- Phiên bản 		  :
-- Mô tả			    : Stream sink/transform dữ liệu init trước khi đẩy xuống ODS

CREATE OR REPLACE STREAM ODS_{TABLE_NAME}_INIT AS SELECT
  DATA.ROWKEY ROWKEY,
  DATA.RECID RECID,
  DATA.OP_TS OP_TS,
  DATA.REP_TS REP_TS,
  TIMESTAMPTOSTRING(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.SSSSSS') CURRENT_TS,
  PARSE_TIMESTAMP(DATA.OP_TS, 'yyyy-MM-dd HH:mm:ss.SSSSSS') COMMIT_TS,
  PARSE_TIMESTAMP(DATA.REP_TS, 'yyyy-MM-dd HH:mm:ss.SSSSSS') REPLICAT_TS,
  PARSE_TIMESTAMP(TIMESTAMPTOSTRING(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.SSSSSS'), 'yyyy-MM-dd HH:mm:ss.SSSSSS') STREAM_TS,
  DATA.TABLE_NAME TABLE_NAME,
  DATA.COMMIT_SCN COMMIT_SCN,
  CAST(DATA.SCN AS BIGINT) COMMIT_SCN,
  DATA.COMMIT_ACTION COMMIT_ACTION,
--{FIELD_NAMES},
  CAST(NVL(DATA.XMLRECORD['IDX'], '1') AS INTEGER) V_M,
  CAST(NVL(DATA.XMLRECORD['IDX_S'], '1') AS INTEGER) V_S,
  (CASE WHEN ((DATA.XMLRECORD['RECID'] = (DATA.RECID + '_TOMBSTONE')) OR (DATA.COMMIT_ACTION = 'D')) THEN 'D' ELSE 'LIVE' END) FLAG_STATUS,
  (CASE WHEN ((SCP.IS_COB_COMPLETED = true) AND (CAST(DATA.COMMIT_SCN AS BIGINT) COMMIT_SCN > SCP.COMMIT_SCN)) THEN PARSE_DATE(SCP.TODAY, 'yyyyMMdd') ELSE PARSE_DATE(SCP.LAST_WORKING_DAY, 'yyyyMMdd') END) BANKING_DATE
FROM FBNK_{TABLE_NAME}_INIT_MULTIVALUE DATA
INNER JOIN FBNK_SEAB_COB_PROCESS_HIGH SCP ON ((SCP.RECID = DATA.LOOKUP_KEY)) --Team SB kiểm tra lại topic cần join --
PARTITION BY DATA.ROWKEY
EMIT CHANGES;