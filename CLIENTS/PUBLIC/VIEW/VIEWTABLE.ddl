create or replace view VIEWTABLE(
	FULL_NAME,
	EMAIL,
	PHONE,
	SPENT
) as select full_name,email,phone,spent from customers;