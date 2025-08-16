CREATE DATABASE US_DISASTERS_FROM_1953;
USE US_DISASTERS_FROM_1953;
DROP DATABASE US_DISASTERS_FROM_1953;

SELECT COUNT(*) FROM state;
SELECT COUNT(*) FROM declaration;
SELECT COUNT(*) FROM disaster;

SELECT * FROM state;
DESCRIBE state;
SELECT * FROM county;
SELECT * FROM declaration;
SELECT * FROM disaster;

CREATE DATABASE US_DISASTERS_DIMENSION;
USE US_DISASTERS_DIMENSION;
DROP DATABASE US_DISASTERS_DIMENSION;

SET SQL_SAFE_UPDATES = 0;
SHOW TABLES FROM us_disasters_dimension;

CREATE TABLE States_DIM(
state_tk INT,
version INT,
date_from DATE,
date_to DATE,
state_name VARCHAR(100)
);
DELETE FROM States_DIM WHERE state_tk = 0;
DROP TABLE States_DIM;

CREATE TABLE County_DIM(
county_tk INT,
version INT,
date_from DATE,
date_to DATE,
county_name VARCHAR(100)
);
DELETE FROM County_DIM WHERE county_tk = 0;
DROP TABLE County_DIM;

CREATE TABLE Disaster_DIM(
disaster_tk INT,
version INT,
date_from DATE,
date_to DATE,
incident_type VARCHAR(100)
);
DELETE FROM Disaster_DIM WHERE disaster_tk = 0;
DROP TABLE Disaster_DIM;

CREATE TABLE Declaration_DIM(
declaration_tk INT,
version INT,
date_from DATE,
date_to DATE,
declaration_title VARCHAR(100),
declaration_type VARCHAR(100),
declaration_date Date,
declaration_request_number Int,
ih_program_declared boolean,
ia_program_declared boolean,
pa_program_declared boolean,
hm_program_declared boolean
);
DELETE FROM Declaration_DIM WHERE declaration_tk = 0;
DROP TABLE Declaration_DIM;

CREATE TABLE Incident_dates_DIM(
incident_dates_tk INT,
version INT,
date_from DATE,
date_to DATE,
incident_begin_date Date,
incident_end_date Date,
incident_duration INT
);
DELETE FROM Incident_dates_DIM WHERE incident_dates_tk = 0;
DROP TABLE Incident_dates_DIM;

CREATE TABLE DISASTER_FACTS_TABLE(
	disaster_fact_tk INT,
    state_tk INT,
    county_tk INT,
    incident_dates_tk INT,
    declaration_tk INT,
    disaster_tk INT,
    deaths INT NULL
);
DROP TABLE DISASTER_FACTS_TABLE;

SELECT * FROM States_DIM;
SELECT COUNT(*) FROM States_DIM;

SELECT * FROM County_DIM;
SELECT COUNT(*) FROM County_DIM;

SELECT * FROM Disaster_DIM;
SELECT COUNT(*) FROM Disaster_DIM;

SELECT * FROM Declaration_DIM;
SELECT COUNT(*) FROM Declaration_DIM;

SELECT * FROM Incident_dates_DIM;
SELECT COUNT(*) FROM Incident_dates_DIM;

SELECT * FROM DISASTER_FACTS_TABLE;
SELECT COUNT(*) FROM DISASTER_FACTS_TABLE;