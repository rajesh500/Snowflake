CREATE TASK SNOWFLAKE_SDD.SDD_PP.my_hourly_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '10 MINUTE'
AS
BEGIN
truncate table if exists SNOWFLAKE_SDD.SDD_PP.ORDERS_STG;
copy into SNOWFLAKE_SDD.SDD_PP.ORDERS_STG
from (
  select $1, $2, $3, $4, $5, metadata$filename, METADATA$FILE_ROW_NUMBER, current_timestamp() 
  from @SNOWFLAKE_SDD.SDD_PP.orders
)
file_format = (type = csv, skip_header = 1)
on_error = abort_statement
purge = true;

merge into SNOWFLAKE_SDD.SDD_PP.CUSTOMER_ORDERS tgt
using SNOWFLAKE_SDD.SDD_PP.ORDERS_STG as src 
on src.customer = tgt.customer 
  and src.delivery_date = tgt.delivery_date 
  and src.baked_good_type = tgt.baked_good_type
when matched then 
  update set tgt.quantity = src.quantity, 
    tgt.source_file_name = src.source_file_name, 
    tgt.load_ts = current_timestamp()
when not matched then
  insert (customer, order_date, delivery_date, baked_good_type, 
    quantity, source_file_name, load_ts)
  values(src.customer, src.order_date, src.delivery_date, 
    src.baked_good_type, src.quantity, src.source_file_name,
    current_timestamp());

    end ;