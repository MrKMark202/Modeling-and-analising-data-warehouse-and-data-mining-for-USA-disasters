SELECT * FROM "State"
SELECT * FROM "Declaration"
SELECT * FROM "Disaster"
SELECT * FROM "Disastered_states"

SELECT * 
FROM "Disastered_states"
ORDER BY state_fk ASC;

SELECT * 
FROM "Declaration"
ORDER BY disaster_fk ASC;

SELECT * 
FROM "Disaster"
ORDER BY disaster_number ASC;

SELECT COUNT(*) 
FROM (
    SELECT country_name
    FROM "State"
    GROUP BY country_name
    HAVING COUNT(*) > 1
) AS duplikati;

SELECT COUNT(*) 
FROM (
    SELECT declaration_request_number
    FROM "Declaration"
    GROUP BY declaration_request_number
    HAVING COUNT(*) > 1
) AS duplikati;

SELECT COUNT(DISTINCT incident_begin_date) AS distinct_begin_dates
FROM   "Disaster";


SELECT * FROM "state_dim"
SELECT * FROM "incident_datesdim"
SELECT * FROM "disaster_dim"
SELECT * FROM "declaration_dim"

SELECT * FROM "disaster_fact"

SELECT COUNT(*) 
FROM (
    SELECT country_name
    FROM "state_dim"
    GROUP BY country_name
    HAVING COUNT(*) > 1
) AS duplikati;

SELECT COUNT(*) 
FROM (
    SELECT state_tk
    FROM "disaster_fact"
    GROUP BY state_tk
    HAVING COUNT(*) > 1
) AS duplikati;

SELECT COUNT(*) 
FROM (
    SELECT declaration_date
    FROM "declaration_dim"
    GROUP BY declaration_date
    HAVING COUNT(*) > 1
) AS duplikati;

SELECT COUNT(*) 
FROM (
    SELECT declaration_request_number
    FROM "declaration_dim"
    GROUP BY declaration_request_number
    HAVING COUNT(*) > 1
) AS duplikati;



SELECT 
    kcu.column_name
FROM 
    information_schema.table_constraints tc
JOIN 
    information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
WHERE 
    tc.table_name = 'state_dim'
    AND tc.constraint_type = 'PRIMARY KEY';

ALTER TABLE state_dim
ADD PRIMARY KEY (state_tk);

ALTER TABLE disaster_dim
ADD PRIMARY KEY (disaster_tk);

ALTER TABLE declaration_dim
ADD PRIMARY KEY (declaration_tk);

ALTER TABLE incident_datesdim
ADD PRIMARY KEY (incident_dates_tk);



SELECT 'state_dim' AS t, COUNT(*) FROM state_dim
UNION ALL
SELECT 'disaster_dim' , COUNT(*) FROM disaster_dim
UNION ALL
SELECT 'declaration_dim', COUNT(*) FROM declaration_dim
UNION ALL
SELECT 'incident_dates_dim', COUNT(*) FROM incident_datesdim;

SELECT 'state_dim', COUNT(*) FROM state_dim UNION ALL
SELECT 'declaration_dim', COUNT(*) FROM declaration_dim;

SELECT * FROM disaster_dim WHERE disaster_tk = 0;
SELECT * FROM disaster_fact WHERE state_tk = 1;

DELETE FROM state_dim WHERE state_tk = 0;
DELETE FROM incident_datesdim WHERE incident_dates_tk = 0;
DELETE FROM disaster_dim WHERE disaster_tk = 0;
DELETE FROM declaration_dim WHERE declaration_tk = 0;