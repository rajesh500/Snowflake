use database SNOWFLAKE_SDD;
use schema SDD_PP;

truncate table if exists ORDERS_STG;

CREATE STAGE IF NOT EXISTS orders;

list @orders;

select $1, $2, $3, $4, $5 from @orders;


create table if not exists ORDERS_STG (
  customer varchar,
  order_date date,
  delivery_date date,
  baked_good_type varchar,
  quantity number,
  source_file_name varchar,
  id number,
  load_ts timestamp
);




copy into ORDERS_STG
from (
  select $1, $2, $3, $4, $5, metadata$filename, METADATA$FILE_ROW_NUMBER, current_timestamp() 
  from @orders
)
file_format = (type = csv, skip_header = 1)
on_error = abort_statement
purge = true;

select * from ORDERS_STG;


use database SNOWFLAKE_SDD;
use schema SDD_PP;

create or replace table CUSTOMER_ORDERS (
  customer varchar,
  order_date date,
  delivery_date date,
  baked_good_type varchar,
  quantity number,
  source_file_name varchar,
  load_ts timestamp
); 


merge into CUSTOMER_ORDERS tgt
using ORDERS_STG as src 
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


    select * from CUSTOMER_ORDERS order by delivery_date desc;