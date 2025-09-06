use role sysadmin;
create or replace database my_db;
use database my_db;
create or replace schema data_quality;
create or replace schema crm;

-- Customer data looks like:
create table crm.customer as 
select * from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.customer;

-- The column cust_c_gender is missing
alter table crm.customer
add column cust_c_gender string;

update crm.customer
set cust_c_gender = 
    case c_salutation
        when 'Ms.' then 'F'
        when 'Mr.' then 'M'
        when 'Miss' then 'F'
        when 'Sir' then 'M'
        when 'Mrs' then 'F'
    end
;

-- Just a check if the case statement works
select 
    c_salutation,
        case c_salutation
        when 'Ms.' then 'F'
        when 'Mr.' then 'M'
        when 'Miss' then 'F'
        when 'Sir' then 'M'
        when 'Mrs' then 'F' end as gender
from crm.customer;
    

select *
from crm.customer
where C_CUSTOMER_SK in (41697040, 41697039)
;
select *
from crm.customer
where c_salutation ='Dr.'
;

select c_salutation
from crm.customer
group by c_salutation
;

use my_db.data_quality;
create tag if not exists rule;
-- TODO : Change operator/expression/thresold as variant to be flexible for futur rule.
create or replace table rule (code string, name string, description string, type string, attribute_type string,operator string, expression string, threshold int, creation_date DATETIME, update_date DATETIME,  version int, active boolean);
insert into data_quality.rule values 
('R001', 'Valid english salutation value','Checks if the salutation is a correct english salutation', 'STRING','LOV', 'IN', $$('Mr.','Dr.','Miss','Ms.','Sir','Mrs.')$$, 80,current_timestamp(), current_timestamp(),1,true);
insert into data_quality.rule values 
('R002', 'Valid english gender value','Checks if the gender is a correct english salutation', 'STRING','LOV', 'IN', $$('F','M')$$, 80,current_timestamp(), current_timestamp(),1,true);
insert into data_quality.rule values 
('R003', 'Valid Month ','Checks if the month is a correct value', 'STRING','LOV', 'IN', $$('1','2','3','4','5','6','7','8','9','10','11','12')$$, 100,current_timestamp(), current_timestamp(),1,true);
insert into data_quality.rule values 
('R004', 'Email ','Checks if email is a correct value', 'STRING','PAT', 'IN', 'email', null,current_timestamp(), current_timestamp(),1,true);
insert into data_quality.rule values 
('R005', 'SSN ','Checks if value match specific SSN Number', 'STRING','PAT', 'IN', '0-90-9....', null,current_timestamp(), current_timestamp(),1,true);


alter table crm.customer alter column c_salutation set tag data_quality.rule = 'R001';
alter table crm.customer alter column cust_c_gender set tag data_quality.rule = 'R002';
alter table crm.customer alter column c_birth_month set tag data_quality.rule = 'R003';

-- Function & Procedures
-- function to find best value in a list
-- python 3.8 is not supported anymore, changed to 3.10
create or replace function data_quality.find_best_value(value string,attribute_type string, list string, threshold int)
returns string
language python
runtime_version = '3.10'
handler = 'find_best_value'
packages = ('fuzzywuzzy')
as
$$
import fuzzywuzzy
from fuzzywuzzy import fuzz
def find_best_value(value,attribute_type, list, threshold):
    list = list.replace("(", "").replace(")", "").replace("'", "").split(',')
    dict = {}
    for x in list:
        if attribute_type == 'STRING':
            similarity = fuzz.ratio(x.lower(), value.lower())
            if similarity >= threshold:
                dict[x] = similarity 
        if attribute_type == 'NUMBER':
            result =  float(value) - float(x)
            dict[x] = result 
        if len(dict) > 0:
            best_value = min(dict, key=dict.get)
        else:
            best_value = value
    return best_value
$$;

select find_best_value('apples','STRING',$$('apple', 'banana', 'cherry')$$, 80);
select find_best_value('3.5','NUMBER',$$('0','1','4')$$, 80);

-- function to check if value is an email best value in a list
create or replace function data_quality.is_email(email string)
returns boolean
language python
runtime_version = '3.10'
handler = 'is_email'
packages = ('email-validator')
as
$$
from email_validator import validate_email, EmailNotValidError
def is_email(email):
    try:
      validation = validate_email(email, check_deliverability=False)
      email = validation.email
      return True;
    except EmailNotValidError as e:
      return False;
$$;
select data_quality.is_email('test@gmail.com');
select data_quality.is_email('testgmail.com');


-- Procedure Master
-- Needed to add $$ in the body
create or replace procedure master_clean_table(table_name string)
returns table (table_name string, column_name string, nb_rows_cleaned integer)
language sql
execute as caller
as
$$
begin
    alter table table_name add column if not exists clean_metrics variant;
    call clean_table_lov(table_name);
    call clean_table_transformation(table_name);
    call clean_check_pattern(table_name);
    call clean_table_deduplication(table_name);
end;
$$
;

-- Procedure for LOV STRING
create or replace procedure clean_table_lov(table_name string)
returns table (table_name string, column_name string, nb_rows_cleaned integer)
language sql
execute as caller
as
declare
    val string;
    select_statement varchar;
    update_statement varchar;
    record_count NUMBER := 0;
    tag_references_x_rules resultset default ( 
    select 'select uuid,'|| COLUMN_NAME ||' from "' ||  OBJECT_DATABASE || '"."' || OBJECT_SCHEMA ||  '"."'  || OBJECT_NAME || '" where not("' ||           COLUMN_NAME || '" ' || rule.operator || ' ' || rule.expression || ')' DQ_SQL, COLUMN_NAME, rule.code as code
    from table(information_schema.tag_references_all_columns(:table_name::string, 'table'))
    join data_quality.rule on rule.code = TAG_VALUE
    where (tag_database, tag_schema, tag_name, level) = ('MY_DB','DATA_QUALITY','RULE','COLUMN')
    and type = 'LOV'
    and rule.active
  );
    c1 cursor for tag_references_x_rules;
    res resultset;
begin
    -- create or replace temporary table tmp_result (table_name string, column_name string,  nb_rows_cleaned integer);
    -- make permanent table for debugging reasons
    create or replace table tmp_result (table_name string, column_name string,  nb_rows_cleaned integer);
    call data_quality.custom_logger('clean_table_lov', 'Table tmp_result created', NULL);
    for rules in c1 do
        record_count := record_count + 1;
        call data_quality.custom_logger('clean_custom_lov', 'Number of records in cursor' , record_count );
        select_statement := '
        create or replace table tmp_rows_cleaned as
        with 
          rows_identified as (
            '||rules.dq_sql ||'
          ),
          rule as (select expression, threshold from data_quality.rule where code = \''|| rules.code ||'\')
        select 
          ri.*, 
          find_best_value('|| rules.column_name ||',ru.attribute_type, ru.expression, ru.threshold) bv  
        from rows_identified ri
        join rule ru on 1=1';
        res := (execute immediate :select_statement);
        update_statement := '
        update ' || :table_name ||' sc
        set sc. '|| rules.column_name ||' = tmp.bv
        from tmp_rows_cleaned tmp
        where sc.uuid = tmp.uuid';
        execute immediate update_statement;
        execute immediate 'insert into tmp_result select  \''|| :table_name ||'\',\''|| rules.column_name ||'\',count(*) from tmp_rows_cleaned';
    end for;
    res := (execute immediate 'select * from tmp_result');
    return table(res);
end;
-- Pattern Validation without cleaning
create or replace procedure clean_check_pattern(table_name string)
returns table (table_name string, column_name string, nb_rows_cleaned integer)
language sql
execute as caller
as
declare
    val string;
    select_statement varchar;
    update_statement varchar;
    tag_references_x_rules resultset default ( 
    select 'select uuid,'|| COLUMN_NAME ||' from "' ||  OBJECT_DATABASE || '"."' || OBJECT_SCHEMA ||  '"."'  || OBJECT_NAME || '" where not("' ||           COLUMN_NAME || '" ' || rule.operator || ' ' || rule.expression || ')' DQ_SQL, COLUMN_NAME, rule.code as code
    from table(information_schema.tag_references_all_columns(:table_name::string, 'table'))
    join data_quality.rule on rule.code = TAG_VALUE
    where (tag_database, tag_schema, tag_name, level) = ('MY_DB','DATA_QUALITY','RULE','COLUMN')
    and type = 'PAT'
    and rule.active
  );
    c1 cursor for tag_references_x_rules;
    res resultset;
begin

    return table(res);
end;

-- Deduplication with key 
-- Coherence

-- Add your Procedure Below and Update Master

/*****************************
            TEST
******************************/
-- Create DataSet for Testing
-- test 1 - LOV - Salutation
update crm.customer  set c_salutation = 'Mis' where C_CUSTOMER_SK = 41697040;
select *
from crm.customer
where C_CUSTOMER_SK = 41697040;
call data_quality.clean_table_lov('customer');

-- test 2 - LOV - Gender
update crm.customer  set cust_c_gender = 'ff' where C_CUSTOMER_SK = 41697040;
call data_quality.clean_table_lov('customer');
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
select *
from data_quality.tmp_result;

select *
from data_quality.process_logging
order by id desc
limit 20;

-- test 3 - LOV -- BirthMonth
update crm.customer  set c_birth_month = 20 where C_CUSTOMER_SK = 41697039;
call data_quality.clean_table_lov('crm.customer');

-- test 4 - PAT -- Email TODO
update crm.customer  set c_birth_month = 20 where C_CUSTOMER_SK = 41697039;
call data_quality.clean_table_lov('crm.customer');

-- Add your TESTS BELOW

-- Admin stuff

create table process_logging (
    id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    process varchar(255),
    step varchar(255),
    value text,
    creation_time datetime
);

create or replace procedure custom_logger(process string, step string, value string)
returns varchar
language sql
execute as caller
as
begin
    insert into process_logging (process, step, value, creation_time) values(
        :process, :step, :value, current_timestamp()
    );
end;


use schema crm;
select *
  from table(my_db.information_schema.tag_references_all_columns('CUSTOMER', 'table'));

select *
from data_quality.rule
order by code
limit 100;


select 'select uuid,'|| COLUMN_NAME ||' from "' ||  OBJECT_DATABASE || '"."' || OBJECT_SCHEMA ||  '"."'  || OBJECT_NAME || '" where not("' ||           COLUMN_NAME || '" ' || rule.operator || ' ' || rule.expression || ')' DQ_SQL, COLUMN_NAME, rule.code as code
    from table(information_schema.tag_references_all_columns('CUSTOMER', 'table'))
    join data_quality.rule on rule.code = TAG_VALUE
    where (tag_database, tag_schema, tag_name, level) = ('MY_DB','DATA_QUALITY','RULE','COLUMN')
    and type = 'LOV'
    and rule.active
;
