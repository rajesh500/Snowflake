use database SNOWFLAKE_SDD;
use schema SDD_PP;

create storage integration my_external_integration
type = external_stage
storage_provider = 'Azure'
enabled = true 
azure_tenant_id = '1234abcd-xxx-56efgh78' --use your own Tenant ID
storage_allowed_locations = ('azure://bakeryorders001.blob.core.windows.net/orderfiles/');

grant usage on integration my_external_integration to role SYSADMIN;

create or replace stage external_stage
storage_integration = my_external_integration
url = '<path>';

list@external_stage;

create file format ORDERS_CSV_FORMAT
  type = csv
  field_delimiter = ','
  skip_header = 1;


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
  from @external_stage
)
file_format = ORDERS_CSV_FORMAT
on_error = abort_statement;


select * from orders_stg;