use database my_db;
create or replace schema by_claude;
use schema by_claude;
CALL VALIDATE_DATA_QUALITY('MY_DB', 'BY_CLAUDE', 'CUSTOMERS');

select *
from debug_log
order by timestamp desc
limit 100;
CREATE OR REPLACE PROCEDURE VALIDATE_DATA_QUALITY(
    target_database STRING,
    target_schema STRING,
    target_table STRING DEFAULT NULL  -- NULL = alle tabellen in schema
    )
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
validation_id STRING DEFAULT CONCAT('DQ_', DATE_PART('epoch', CURRENT_TIMESTAMP())::STRING);
cursor_tags CURSOR FOR
        SELECT
        t.object_name,
        t.column_name,
        t.tag_value as rule_code
        FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(?, 'TABLE')) t
        WHERE t.tag_name LIKE 'DATA_QUALITY_RULE%';  -- Ondersteunt ook DATA_QUALITY_RULE_2, etc.
        -- AND t.object_name = :target_table;

rule_function STRING;
validation_sql STRING;
result_count number(38,0) DEFAULT 0;
error_count number(38,0) DEFAULT 0;
total_processed number(38,0) DEFAULT 0;

BEGIN

-- Start logging
CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'START', 
    'Starting validation for: ' || :target_database || '.' || :target_schema || 
    CASE WHEN :target_table IS NOT NULL THEN '.' || :target_table ELSE '' END);

open cursor_tags using (:target_table);
-- Loop door alle gevonden tags
FOR tag_record IN cursor_tags DO
    BEGIN
        CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'START LOOP', 'Just started');
        -- CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'RETURN VALUE', tag_record);
        -- Haal regel definitie op
        SELECT function_name INTO :rule_function 
        FROM DQ_RULES 
        WHERE rule_code = tag_record.rule_code 
        AND active = TRUE;

        CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'after first select', :rule_function);
        
        IF (:rule_function IS NULL) THEN
            CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'RULE_NOT_FOUND', 
                'Rule ' || tag_record.rule_code || ' not found or inactive', 'WARNING');
            CONTINUE;
        END IF;
        
        CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'RULE_FOUND', 
            'Processing: ' || tag_record.rule_code || ' -> ' || :rule_function || 
            ' on ' || tag_record.object_name || '.' || tag_record.column_name);
        
        -- Bouw dynamische SQL voor validatie
        validation_sql := 
            'INSERT INTO DQ_VALIDATION_RESULTS ' ||
            '(validation_id, table_name, column_name, rule_code, record_id, is_valid, validation_timestamp) ' ||
            'SELECT ' ||
            '''' || :validation_id || ''', ' ||
            '''' || tag_record.object_name || ''', ' ||
            '''' || tag_record.column_name || ''', ' ||
            '''' || tag_record.rule_code || ''', ' ||
            'ROW_NUMBER() OVER (ORDER BY 1)::STRING, ' ||
            :rule_function || '(' || tag_record.column_name || '), ' ||
            'CURRENT_TIMESTAMP() ' ||
            'FROM ' || :target_database || '.' || :target_schema || '.' || tag_record.object_name;
        
        CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'EXECUTE_SQL', 
            'SQL: ' || :validation_sql);
        
        -- Voer validatie uit
        EXECUTE IMMEDIATE :validation_sql;
        
        -- Tel resultaten
        SELECT COUNT(*) INTO :result_count 
        FROM DQ_VALIDATION_RESULTS 
        WHERE validation_id = :validation_id 
            AND rule_code = tag_record.rule_code
            AND table_name = tag_record.object_name
            AND column_name = tag_record.column_name;
        
        total_processed := :total_processed + :result_count;
        
        CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'RULE_COMPLETE', 
            'Rule ' || tag_record.rule_code || ' processed ' || :result_count || ' records');
            
    EXCEPTION
        WHEN OTHER THEN
            LET error_msg string := 'Error processing rule ' || tag_record.rule_code || ': ' || :SQLERRM;
            CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'RULE_ERROR', :error_msg, 'ERROR');
            
            -- Log error in results tabel
            INSERT INTO DQ_VALIDATION_RESULTS 
            (validation_id, table_name, column_name, rule_code, record_id, is_valid, error_message) 
            VALUES (:validation_id, tag_record.table_name, tag_record.column_name, 
                    tag_record.rule_code, 'ERROR', FALSE, :error_msg);
                    
            error_count := :error_count + 1;
    END;
END FOR;

-- Controleer of er regels gevonden zijn
IF (:total_processed = 0 AND :error_count = 0) THEN
    CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'NO_RULES_FOUND', 
        'No data quality rules found for specified target', 'WARNING');
    RETURN 'No data quality rules found for: ' || :target_database || '.' || :target_schema ||
           CASE WHEN :target_table IS NOT NULL THEN '.' || :target_table ELSE '' END;
END IF;

-- Finale summary
CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'COMPLETE', 
    'Validation ' || :validation_id || ' completed. Records processed: ' || :total_processed || ', Errors: ' || :error_count);

RETURN 'Validation completed. ID: ' || :validation_id || 
       ', Records processed: ' || :total_processed || 
       ', Rules with errors: ' || :error_count;

EXCEPTION
WHEN OTHER THEN
CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'FATAL_ERROR',
'Fatal error: ' || :SQLERRM, 'ERROR');
RETURN 'FAILED: ' || :SQLERRM;
END;
$$;

-- =====================================================
-- DATA QUALITY FRAMEWORK SETUP
-- =====================================================

-- 1. REGEL DEFINITIE TABEL
CREATE OR REPLACE TABLE DQ_RULES (
    rule_code STRING PRIMARY KEY,
    function_name STRING NOT NULL,
    description STRING,
    active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

--Voorbeeld regels
INSERT INTO DQ_RULES VALUES
('R001', 'GELDIG_EMAILADRES', 'Controleert email formaat', TRUE, CURRENT_TIMESTAMP()),
('R002', 'MAX_1_JAAR_OUD', 'Controleert of datum max 1 jaar oud is', TRUE, CURRENT_TIMESTAMP()),
('R003', 'NIET_LEEG', 'Controleert of veld niet leeg is', TRUE, CURRENT_TIMESTAMP());

-- 2. RESULTATEN TABEL
CREATE OR REPLACE TABLE DQ_VALIDATION_RESULTS (
    validation_id STRING,
    table_name STRING,
    column_name STRING,
    rule_code STRING,
    record_id STRING,
    is_valid BOOLEAN,
    error_message STRING,
    validation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 3. DEBUG/LOGGING TABEL
CREATE OR REPLACE TABLE DEBUG_LOG (
    procedure_name STRING,
    step_name STRING,
    message STRING,
    log_level STRING DEFAULT 'INFO',
    session_id STRING,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

select *
from debug_log
limit 100;
-- =====================================================
-- LOGGING PROCEDURE
-- =====================================================
CREATE OR REPLACE PROCEDURE LOG_DEBUG(
    procedure_name STRING,
    step_name STRING,
    message STRING,
    log_level STRING DEFAULT 'INFO'
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
INSERT INTO DEBUG_LOG (procedure_name, step_name, message, log_level, session_id, timestamp)
VALUES (:procedure_name, :step_name, :message, :log_level, CURRENT_SESSION(), CURRENT_TIMESTAMP());
RETURN 'Logged: ' || :step_name;
END;
$$;

-- =====================================================
-- HOOFDPROCEDURE VOOR DATA QUALITY VALIDATIE
-- =====================================================
CREATE OR REPLACE PROCEDURE VALIDATE_DATA_QUALITY(
    target_database STRING,
    target_schema STRING,
    target_table STRING DEFAULT NULL  -- NULL = alle tabellen in schema
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
validation_id STRING DEFAULT CONCAT('DQ_', DATE_PART('epoch', CURRENT_TIMESTAMP())::STRING);
cursor_tags CURSOR FOR
SELECT
    t.table_name,
    t.column_name,
    t.tag_value as rule_code
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(:target_database, :target_schema)) t
WHERE t.tag_name = 'DATA_QUALITY_RULE'
    AND (:target_table IS NULL OR t.table_name = :target_table);

rule_function STRING;
validation_sql STRING;
result_count INT DEFAULT 0;
error_count INT DEFAULT 0;

BEGIN

-- Start logging
CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'START', 
    'Starting validation for: ' || :target_database || '.' || :target_schema || 
    CASE WHEN :target_table IS NOT NULL THEN '.' || :target_table ELSE '' END);

-- Loop door alle gevonden tags
FOR tag_record IN cursor_tags DO
    BEGIN
        -- Haal regel definitie op
        SELECT function_name INTO :rule_function 
        FROM DQ_RULES 
        WHERE rule_code = tag_record.rule_code 
        AND active = TRUE;
        
        CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'RULE_FOUND', 
            'Processing: ' || tag_record.rule_code || ' -> ' || :rule_function || 
            ' on ' || tag_record.table_name || '.' || tag_record.column_name);
        
        -- Bouw dynamische SQL voor validatie
        validation_sql := 
            'INSERT INTO DQ_VALIDATION_RESULTS ' ||
            '(validation_id, table_name, column_name, rule_code, record_id, is_valid, validation_timestamp) ' ||
            'SELECT ' ||
            '''' || :validation_id || ''', ' ||
            '''' || tag_record.table_name || ''', ' ||
            '''' || tag_record.column_name || ''', ' ||
            '''' || tag_record.rule_code || ''', ' ||
            'ROW_NUMBER() OVER (ORDER BY 1)::STRING, ' ||
            :rule_function || '(' || tag_record.column_name || '), ' ||
            'CURRENT_TIMESTAMP() ' ||
            'FROM ' || :target_database || '.' || :target_schema || '.' || tag_record.table_name;
        
        CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'EXECUTE_SQL', 
            'SQL: ' || :validation_sql);
        
        -- Voer validatie uit
        EXECUTE IMMEDIATE :validation_sql;
        
        -- Tel resultaten
        SELECT COUNT(*) INTO :result_count 
        FROM DQ_VALIDATION_RESULTS 
        WHERE validation_id = :validation_id 
        AND rule_code = tag_record.rule_code;
        
        CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'RULE_COMPLETE', 
            'Rule ' || tag_record.rule_code || ' processed ' || :result_count || ' records');
            
    EXCEPTION
        WHEN OTHER THEN
            LET error_msg := 'Error processing rule ' || tag_record.rule_code || ': ' || SQLERRM;
            CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'RULE_ERROR', :error_msg, 'ERROR');
            
            -- Log error in results tabel
            INSERT INTO DQ_VALIDATION_RESULTS 
            (validation_id, table_name, column_name, rule_code, record_id, is_valid, error_message) 
            VALUES (:validation_id, tag_record.table_name, tag_record.column_name, 
                    tag_record.rule_code, 'ERROR', FALSE, :error_msg);
                    
            LET error_count := :error_count + 1;
    END;
END FOR;

-- Finale summary
CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'COMPLETE', 
    'Validation ' || :validation_id || ' completed. Errors: ' || :error_count);

RETURN 'Validation completed. ID: ' || :validation_id || ', Errors: ' || :error_count;

EXCEPTION
WHEN OTHER THEN
CALL LOG_DEBUG('VALIDATE_DATA_QUALITY', 'FATAL_ERROR',
'Fatal error: ' || SQLERRM, 'ERROR');
RETURN 'FAILED: ' || SQLERRM;
END;
$$;

-- =====================================================
-- HELPER PROCEDURES
-- =====================================================

-- Procedure om validatie resultaten te bekijken
CREATE OR REPLACE PROCEDURE GET_VALIDATION_SUMMARY(validation_id STRING)
RETURNS TABLE (
    table_name STRING,
    column_name STRING,
    rule_code STRING,
    total_records INT,
    valid_records INT,
    invalid_records INT,
    error_records INT
)
LANGUAGE SQL
AS
$$
DECLARE 
    res RESULTSET;
    query VARCHAR DEFAULT 'SELECT 
        r.table_name,
        r.column_name, 
        r.rule_code, 
        COUNT(*) as total_records,
        SUM(CASE WHEN r.is_valid = TRUE AND r.error_message IS NULL THEN 1 ELSE 0 END) as valid_records,
        SUM(CASE WHEN r.is_valid = FALSE AND r.error_message IS NULL THEN 1 ELSE 0 END) as invalid_records,
        SUM(CASE WHEN r.error_message IS NOT NULL THEN 1 ELSE 0 END) as error_records
    FROM DQ_VALIDATION_RESULTS r
    WHERE r.validation_id = ?
    GROUP BY r.table_name, r.column_name, r.rule_code
    ORDER BY r.table_name, r.column_name, r.rule_code';
BEGIN
    res := (EXECUTE IMMEDIATE :query USING(validation_id));
    RETURN TABLE(res);
END;
$$;


-- =====================================================
-- VOORBEELD VALIDATIE FUNCTIES
-- =====================================================

CREATE OR REPLACE FUNCTION GELDIG_EMAILADRES(email STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
SELECT email REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
$$;

CREATE OR REPLACE FUNCTION MAX_1_JAAR_OUD(datum DATE)
RETURNS BOOLEAN
LANGUAGE SQL  
AS
$$
SELECT datum >= DATEADD('year', -1, CURRENT_DATE())
$$;

CREATE OR REPLACE FUNCTION NIET_LEEG(waarde STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
SELECT waarde IS NOT NULL AND TRIM(waarde) != ''
$$;

-- =====================================================
-- VOORBEELD GEBRUIK
-- =====================================================

-- 1. Tags zetten op kolommen
-- ALTER TABLE MY_TABLE MODIFY COLUMN email SET TAG DATA_QUALITY_RULE = 'R001';
-- ALTER TABLE MY_TABLE MODIFY COLUMN created_date SET TAG DATA_QUALITY_RULE = 'R002';
-- ALTER TABLE MY_TABLE MODIFY COLUMN name SET TAG DATA_QUALITY_RULE = 'R003';

-- 2. Validatie uitvoeren
-- CALL VALIDATE_DATA_QUALITY('MY_DATABASE', 'MY_SCHEMA', 'MY_TABLE');

-- 3. Resultaten bekijken  
-- CALL GET_VALIDATION_SUMMARY('DQ_1735732800');

-- 4. Debug logs bekijken
-- SELECT * FROM DEBUG_LOG WHERE procedure_name = 'VALIDATE_DATA_QUALITY' ORDER BY timestamp DESC;
-- =====================================================
-- COMPLETE VOORBEELD: VALIDATE_DATA_QUALITY PROCEDURE
-- =====================================================

-- STAP 1: Voorbereidingen - Maak test tabel en data
-- =====================================================
CREATE OR REPLACE TABLE CUSTOMERS (
    id INT IDENTITY(1,1),
    email STRING,
    phone STRING,
    registration_date DATE,
    name STRING,
    status STRING
);

-- Test data invoegen (mix van geldige en ongeldige data)
INSERT INTO CUSTOMERS (email, phone, registration_date, name, status) VALUES
('jan@example.com', '0612345678', '2024-06-15', 'Jan Jansen', 'active'),
('invalid-email', '06-123-456', '2020-01-01', 'Piet Peters', 'active'),
('marie@test.nl', '', '2024-12-01', '', 'inactive'),
('', '0687654321', '2025-01-01', 'Lisa de Vries', 'pending'),
('admin@company.com', '0698765432', '2024-08-20', 'Admin User', 'active');

-- STAP 2: Regels definiëren in DQ_RULES tabel
-- =====================================================
INSERT INTO DQ_RULES (rule_code, function_name, description, active) VALUES
('R001', 'GELDIG_EMAILADRES', 'Controleert email formaat', TRUE),
('R002', 'MAX_1_JAAR_OUD', 'Controleert of datum max 1 jaar oud is', TRUE),
('R003', 'NIET_LEEG', 'Controleert of veld niet leeg is', TRUE),
('R004', 'GELDIG_TELEFOON', 'Controleert telefoon formaat', TRUE);

-- Extra validatie functie voor telefoon
CREATE OR REPLACE FUNCTION GELDIG_TELEFOON(telefoon STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
SELECT telefoon REGEXP '^06[0-9]{8}$'
$$;

-- STAP 3: Tags zetten op kolommen
-- =====================================================
-- Email kolom moet geldig email formaat hebben
create tag if not exists data_quality_rule;

ALTER TABLE CUSTOMERS MODIFY COLUMN email
SET TAG DATA_QUALITY_RULE = 'R001';

-- Email kolom mag ook niet leeg zijn (meerdere regels op 1 kolom)
-- Helaas kan Snowflake maar 1 tag waarde per tag naam hebben
-- Daarom gebruiken we verschillende tag namen:
ALTER TABLE CUSTOMERS MODIFY COLUMN email
SET TAG DATA_QUALITY_RULE = 'R001';

ALTER TABLE CUSTOMERS MODIFY COLUMN email
SET TAG DATA_QUALITY_RULE = 'R003';

-- Telefoon moet geldig Nederlands mobiel nummer zijn
ALTER TABLE CUSTOMERS MODIFY COLUMN phone
SET TAG DATA_QUALITY_RULE = 'R004';

-- Registratie datum mag niet ouder dan 1 jaar zijn
ALTER TABLE CUSTOMERS MODIFY COLUMN registration_date
SET TAG DATA_QUALITY_RULE = 'R002';

-- Naam mag niet leeg zijn
ALTER TABLE CUSTOMERS MODIFY COLUMN name
SET TAG DATA_QUALITY_RULE = 'R003';

-- STAP 4: Controleren welke tags zijn gezet
-- =====================================================
SELECT
    object_name,
    column_name,
    tag_name,
    tag_value as rule_code
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('CUSTOMERS', 'TABLE'))
WHERE tag_name LIKE 'DATA_QUALITY_RULE%'
ORDER BY object_name, column_name, tag_name
;

-- STAP 5: Validatie uitvoeren
-- =====================================================
CALL VALIDATE_DATA_QUALITY('MY_DB', 'BY_CLAUDE', 'CUSTOMERS');

select *
from debug_log
order by timestamp desc
limit 100;

-- STAP 6: Resultaten bekijken
-- =====================================================

-- A) Summary van laatste validatie
SELECT validation_id, COUNT(*) as total_checks
FROM DQ_VALIDATION_RESULTS
GROUP BY validation_id
ORDER BY validation_timestamp DESC
LIMIT 1;

-- B) Gedetailleerd overzicht per regel
WITH latest_validation AS (
SELECT validation_id
FROM DQ_VALIDATION_RESULTS
ORDER BY validation_timestamp DESC
LIMIT 1
)
SELECT
    r.table_name,
    r.column_name,
    r.rule_code,
    dr.description,
    COUNT(*) as total_records,
    SUM(CASE WHEN r.is_valid = TRUE THEN 1 ELSE 0 END) as valid_count,
    SUM(CASE WHEN r.is_valid = FALSE THEN 1 ELSE 0 END) as invalid_count,
    SUM(CASE WHEN r.error_message IS NOT NULL THEN 1 ELSE 0 END) as error_count,
    ROUND((SUM(CASE WHEN r.is_valid = TRUE THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) as success_percentage
FROM DQ_VALIDATION_RESULTS r
JOIN DQ_RULES dr ON r.rule_code = dr.rule_code
WHERE r.validation_id = (SELECT validation_id FROM latest_validation)
GROUP BY r.table_name, r.column_name, r.rule_code, dr.description
ORDER BY r.table_name, r.column_name, r.rule_code;

-- C) Ongeldige records opzoeken
WITH latest_validation AS (
SELECT validation_id
FROM DQ_VALIDATION_RESULTS
ORDER BY validation_timestamp DESC
LIMIT 1
)
SELECT
    r.table_name,
    r.column_name,
    r.rule_code,
    r.record_id,
    r.is_valid,
    r.error_message,
    -- Join met originele data om waarde te zien
    c.email,
    c.phone,
    c.name,
    c.registration_date
FROM DQ_VALIDATION_RESULTS r
JOIN CUSTOMERS c ON r.record_id = c.id::STRING
WHERE r.validation_id = (SELECT validation_id FROM latest_validation)
AND r.is_valid = FALSE
ORDER BY r.table_name, r.column_name, r.rule_code;

-- STAP 7: Debug informatie bekijken
-- =====================================================

-- Bekijk laatste validatie logs
SELECT
    step_name,
    message,
    log_level,
    timestamp
FROM DEBUG_LOG
WHERE procedure_name = 'VALIDATE_DATA_QUALITY'
AND timestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
ORDER BY timestamp;

-- Performance analyse
SELECT
    step_name,
    COUNT(*) as frequency,
    AVG(DATEDIFF('millisecond', LAG(timestamp) OVER (ORDER BY timestamp), timestamp)) as avg_duration_ms
FROM DEBUG_LOG
WHERE procedure_name = 'VALIDATE_DATA_QUALITY'
AND timestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
AND step_name IN ('RULE_FOUND', 'RULE_COMPLETE')
GROUP BY step_name;

-- STAP 8: Alternatieve uitvoering - Hele schema valideren
-- =====================================================

-- Valideer alle tabellen in een schema (niet alleen CUSTOMERS)
CALL VALIDATE_DATA_QUALITY('TESTDB', 'PUBLIC', NULL);

-- STAP 9: Helper procedure gebruiken voor mooie summary
-- =====================================================

-- Gebruik de helper procedure voor een nette summary
WITH latest_validation AS (
SELECT validation_id
FROM DQ_VALIDATION_RESULTS
ORDER BY validation_timestamp DESC
LIMIT 1
)
CALL GET_VALIDATION_SUMMARY((SELECT validation_id FROM latest_validation));

-- STAP 10: Opruimen oude validaties (optioneel)
-- =====================================================

-- Verwijder validatie resultaten ouder dan 30 dagen
DELETE FROM DQ_VALIDATION_RESULTS
WHERE validation_timestamp < DATEADD('day', -30, CURRENT_TIMESTAMP());

-- Verwijder debug logs ouder dan 7 dagen
DELETE FROM DEBUG_LOG
WHERE timestamp < DATEADD('day', -7, CURRENT_TIMESTAMP())
AND procedure_name = 'VALIDATE_DATA_QUALITY';

-- =====================================================
-- VERWACHTE RESULTATEN VAN DIT VOORBEELD:
-- =====================================================
/*
Voor de test data hierboven verwacht je ongeveer deze resultaten:

CUSTOMERS tabel (5 records):

- email kolom met R001 (email formaat): 3 valid, 2 invalid
- email kolom met R003 (niet leeg): 4 valid, 1 invalid
- phone kolom met R004 (telefoon formaat): 2 valid, 3 invalid
- registration_date kolom met R002 (max 1 jaar oud): 4 valid, 1 invalid
- name kolom met R003 (niet leeg): 4 valid, 1 invalid

Totaal: ~25 validatie checks uitgevoerd
*/

