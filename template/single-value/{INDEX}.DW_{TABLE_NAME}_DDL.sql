-- Ngày thay đổi  : {CURRENT_TIME}
-- Người thay đổi :
-- Phiên bản 	  :
-- Mô tả		  : Tạo bảng mới


CREATE TABLE DW_{TABLE_NAME}
   (  RECID VARCHAR2(200)  NOT NULL , -- Để ý xem lấy bao nhiêu kí tự (Note cho BNH: Sửa xong thì xoá) --

   

    COMMIT_SCN NUMBER, 
	COMMIT_ACTION VARCHAR2(255) , 
    COMMIT_TS TIMESTAMP (6),  
    REPLICAT_TS TIMESTAMP (6), 
    STREAM_TS TIMESTAMP (6), 
    TIME_UPDATE TIMESTAMP (6),
    BANKING_DATE DATE NOT NULL) ;
-- Create/Recreate indexes 
-- Create/Recreate primary, unique and foreign key constraints 
alter table DW_{TABLE_NAME}
  add constraint DW_{TABLE_NAME}_PK primary key (RECID, BANKING_DATE);