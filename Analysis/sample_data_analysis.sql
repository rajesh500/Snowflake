create or replace database tb_101;
create or replace schema tb_101.raw_pos;
create or replace schema tb_101.raw_customer;
create or replace schema tb_101.harmonized;
create or replace schema tb_101.analytics;

use role securityadmin;

create role if not exists to_admin
comment = 'Admin role';

create role if not exists to_data_eng
comment = 'data engineer role';

create role if not exists to_dev
comment = 'developer';


grant role to_admin to role sysadmin; 
grant role to_data_eng to role to_admin; 
grant role to_dev to role to_data_eng; 


USE ROLE accountadmin;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE to_data_eng;

GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE to_admin;

USE ROLE securityadmin;

GRANT USAGE ON DATABASE tb_101 TO ROLE to_admin;
GRANT USAGE ON DATABASE tb_101 TO ROLE to_data_eng;
GRANT USAGE ON DATABASE tb_101 TO ROLE to_dev;

GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE to_admin;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE to_data_eng;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE to_dev;

GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE to_admin;
GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE to_data_eng;
GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE to_dev;

GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE to_admin;
GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE to_data_eng;
GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE to_dev;

GRANT ALL ON SCHEMA tb_101.analytics TO ROLE to_admin;
GRANT ALL ON SCHEMA tb_101.analytics TO ROLE to_data_eng;
GRANT ALL ON SCHEMA tb_101.analytics TO ROLE to_dev;


GRANT ALL ON SCHEMA tb_101.public TO ROLE to_admin;
GRANT ALL ON SCHEMA tb_101.public TO ROLE to_data_eng;
GRANT ALL ON SCHEMA tb_101.public TO ROLE to_dev;

GRANT ALL ON SCHEMA tb_101.raw_customer TO ROLE to_admin;
GRANT ALL ON SCHEMA tb_101.raw_customer TO ROLE to_data_eng;
GRANT ALL ON SCHEMA tb_101.raw_customer TO ROLE to_dev;

use role sysadmin;

create or replace file format tb_101.public.csv_ff
type = 'csv';

create or replace stage tb_101.public.s3load
comment = "load data from s3"
url = 's3://sfquickstarts/frostbyte_tastybytes/'
file_format = tb_101.public.csv_ff;

CREATE OR REPLACE TABLE tb_101.raw_pos.country
(
    country_id NUMBER(18,0),
    country VARCHAR(16777216),
    iso_currency VARCHAR(3),
    iso_country VARCHAR(2),
    city_id NUMBER(19,0),
    city VARCHAR(16777216),
    city_population VARCHAR(16777216)
);

copy into tb_101.raw_pos.country 
from @tb_101.public.s3load/raw_pos/country;


CREATE OR REPLACE TABLE tb_101.raw_pos.franchise 
(
    franchise_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216) 
);

copy into tb_101.raw_pos.franchise 
from @tb_101.public.s3load/raw_pos/franchise;

CREATE OR REPLACE TABLE tb_101.raw_pos.location
(
    location_id NUMBER(19,0),
    placekey VARCHAR(16777216),
    location VARCHAR(16777216),
    city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    country VARCHAR(16777216)
);

copy into tb_101.raw_pos.location 
from @tb_101.public.s3load/raw_pos/location;


CREATE OR REPLACE TABLE tb_101.raw_pos.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

SELECT  $1, $2 --ARRAY_SIZE(SPLIT($1, '|')) AS column_count 
FROM @tb_101.public.s3load/raw_pos/menu (FILE_FORMAT => tb_101.public.csv_ff)
LIMIT 1;

copy into tb_101.raw_pos.menu from(
select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
from @tb_101.public.s3load/raw_pos/menu);



CREATE OR REPLACE TABLE tb_101.raw_pos.truck
(
    truck_id NUMBER(38,0),
    menu_type_id NUMBER(38,0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag NUMBER(38,0),
    year NUMBER(38,0),
    make VARCHAR(16777216),
    model VARCHAR(16777216),
    ev_flag NUMBER(38,0),
    franchise_id NUMBER(38,0),
    truck_opening_date DATE
);

COPY INTO tb_101.raw_pos.truck
FROM @tb_101.public.s3load/raw_pos/truck/;


CREATE OR REPLACE TABLE tb_101.raw_pos.order_header
(
    order_id NUMBER(38,0),
    truck_id NUMBER(38,0),
    location_id FLOAT,
    customer_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38,0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38,4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38,4)
);


COPY INTO tb_101.raw_pos.order_header
FROM @tb_101.public.s3load/raw_pos/order_header/;


CREATE OR REPLACE TABLE tb_101.raw_pos.order_detail 
(
    order_detail_id NUMBER(38,0),
    order_id NUMBER(38,0),
    menu_item_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    line_number NUMBER(38,0),
    quantity NUMBER(5,0),
    unit_price NUMBER(38,4),
    price NUMBER(38,4),
    order_item_discount_amount VARCHAR(16777216)
);


COPY INTO tb_101.raw_pos.order_detail
FROM @tb_101.public.s3load/raw_pos/order_detail/;



CREATE OR REPLACE TABLE tb_101.raw_customer.customer_loyalty
(
    customer_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    postal_code VARCHAR(16777216),
    preferred_language VARCHAR(16777216),
    gender VARCHAR(16777216),
    favourite_brand VARCHAR(16777216),
    marital_status VARCHAR(16777216),
    children_count VARCHAR(16777216),
    sign_up_date DATE,
    birthday_date DATE,
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216)
);

COPY INTO tb_101.raw_customer.customer_loyalty
FROM @tb_101.public.s3load/raw_customer/customer_loyalty/;

-- orders_v view
CREATE OR REPLACE VIEW tb_101.harmonized.orders_v
    AS
SELECT 
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total
FROM tb_101.raw_pos.order_detail od
JOIN tb_101.raw_pos.order_header oh
    ON od.order_id = oh.order_id
JOIN tb_101.raw_pos.truck t
    ON oh.truck_id = t.truck_id
JOIN tb_101.raw_pos.franchise f
    ON t.franchise_id = f.franchise_id
JOIN tb_101.raw_pos.location l
    ON oh.location_id = l.location_id
LEFT JOIN tb_101.raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id;

-- loyalty_metrics_v view
CREATE OR REPLACE VIEW tb_101.harmonized.customer_loyalty_metrics_v
    AS
SELECT 
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM tb_101.raw_customer.customer_loyalty cl
JOIN tb_101.raw_pos.order_header oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail;


select * from tb_101.harmonized.orders_v limit 10;
select * from tb_101.harmonized.customer_loyalty_metrics_v limit 10;