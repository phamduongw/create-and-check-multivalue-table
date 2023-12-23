-- Ngày thay đổi	: {CURRENT_TIME}
-- Người thay đổi	:
-- Phiên bản 		  :
-- Mô tả			    : Tạo stream mapped để parsing dữ liệu xml

-----set properties khi không cần parse những giá trị multivalue bị trống-----
set 'ksql.functions._global_.xml.parser.pad.empty.tags'='false'
-----set properties khi muốn parse multivalue
set 'ksql.functions._global_.xml.parser.add.multivalue.index'='true';

CREATE OR REPLACE STREAM FBNK_{TABLE_NAME}_INIT_MAPPED AS SELECT
  DATA.ROWKEY ROWKEY,
  DATA.LOOKUP_KEY LOOKUP_KEY,
  DATA.RECID RECID,  
  DATA.OP_TS OP_TS,  
  DATA.CURRENT_TS REP_TS,  
  TIMESTAMPTOSTRING(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss.SSSSSS') CURRENT_TS,
  DATA.`TABLE` TABLE_NAME,
  DATA.SCN COMMIT_SCN,  
  DATA.OP_TYPE COMMIT_ACTION,  
  PARSE_T24_RECORD(DATA.XMLRECORD, 'FLAT_SS-FBNK_{TABLE_NAME}-value', '#') XMLRECORD --chọn delimiter nào ko xuất hiện trong value gốc VD:#
FROM FBNK_{TABLE_NAME}_INIT DATA
EMIT CHANGES;