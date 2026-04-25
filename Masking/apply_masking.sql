select *
from tb_101.raw_pos.truck;

select CURRENT_ROLE();

CREATE OR REPLACE MASKING POLICY tb_101.raw_pos.city_mask AS (val STRING) 
  RETURNS STRING ->
  CASE 
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val
    ELSE '****c****'
  END;

use database tb_101;
use schema raw_pos;

ALTER TABLE tb_101.raw_pos.truck MODIFY COLUMN primary_city SET MASKING POLICY city_mask;
ALTER TABLE tb_101.raw_pos.truck MODIFY COLUMN primary_city UNSET MASKING POLICY;

use role accountadmin;

select *
from tb_101.raw_pos.truck;

use role sysadmin;
select *
from tb_101.raw_pos.truck;
