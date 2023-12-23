-- Ngày thay đổi	: {CURRENT_TIME} 
-- Người thay đổi	:
-- Phiên bản 		  :
-- Mô tả			    : Tạo bảng mới


CREATE TABLE ODS_{TABLE_NAME}
   (  RECID VARCHAR2(200)  NOT NULL , 
	  V_M NUMBER NOT NULL,
	  V_S NUMBER NOT NULL,


    COMMIT_SCN NUMBER, 
	  COMMIT_ACTION VARCHAR2(255) , 
	  FLAG_STATUS VARCHAR2(255,)
    COMMIT_TS TIMESTAMP (6),  
    REPLICAT_TS TIMESTAMP (6), 
    STREAM_TS TIMESTAMP (6), 
    TIME_UPDATE TIMESTAMP (6),
    BANKING_DATE DATE);
-- Create/Recreate indexes 
-- Create/Recreate primary, unique and foreign key constraints 
alter table ODS_{TABLE_NAME}
  add constraint ODS_{TABLE_NAME}_PK primary key (RECID, V_M, V_S);
