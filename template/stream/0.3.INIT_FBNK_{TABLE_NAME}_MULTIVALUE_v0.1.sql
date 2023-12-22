-- Ngày thay đổi	:
-- Người thay đổi	:
-- Phiên bản 		  :
-- Mô tả			    :

CREATE OR REPLACE STREAM FBNK_{TABLE_NAME}_INIT_MULTIVALUE AS SELECT
  DATA.ROWKEY ROWKEY,
  DATA.RECID RECID,
  DATA.OP_TS OP_TS,
  DATA.CURRENT_TS REP_TS,
  TIMESTAMPTOSTRING(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.SSSSSS') CURRENT_TS,
  DATA.TABLE_NAME TABLE_NAME,
  DATA.COMMIT_SCN COMMIT_SCN,
  DATA.COMMIT_ACTION COMMIT_ACTION,
  DATA.LOOKUP_KEY LOOKUP_KEY,
--{FIELD_NAMES},
  PARSE_T24_MULTIVAL(DATA.RECID, DATA.XMLRECORD, '{TABLE_NAME}_INIT_MULTIVALUE', ARRAY[''], ARRAY[''], '#') XMLRECORD --chọn delimiter nào ko xuất hiện trong value gốc VD:#
FROM FBNK_{TABLE_NAME}_INIT_MAPPED DATA
EMIT CHANGES;