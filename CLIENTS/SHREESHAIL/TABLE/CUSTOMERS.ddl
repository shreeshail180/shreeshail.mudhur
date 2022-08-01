create or replace TABLE CUSTOMERS (
	ID NUMBER(38,0),
	FULL_NAME VARCHAR(16777216),
	EMAIL VARCHAR(16777216),
	PHONE VARCHAR(16777216),
	SPENT NUMBER(38,0),
	SALARY NUMBER(38,0),
	NAME VARCHAR(16777216),
	CREATE_DATE DATE DEFAULT CURRENT_DATE()
);