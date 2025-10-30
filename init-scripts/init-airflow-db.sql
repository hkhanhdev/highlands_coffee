-- init-airflow-db.sql
-- This script initializes the Airflow metadata database in Postgres

-- Create Airflow user (if not exists)
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow'
   ) THEN
      CREATE ROLE airflow LOGIN PASSWORD 'airflow';
   END IF;
END
$do$;

-- Create Airflow database (if not exists)
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database WHERE datname = 'airflow'
   ) THEN
      CREATE DATABASE airflow OWNER airflow;
   END IF;
END
$do$;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
