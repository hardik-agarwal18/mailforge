
\set ON_ERROR_STOP on

-- Create the monitoring role if it doesn't exist.
SELECT format(
    'CREATE ROLE %I LOGIN PASSWORD %L',
    :'exporter_user',
    :'exporter_password'
)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = :'exporter_user'
)
\gexec

-- Ensure its password matches the value in .env.
SELECT format(
    'ALTER ROLE %I WITH LOGIN PASSWORD %L',
    :'exporter_user',
    :'exporter_password'
)
\gexec

-- Grant PostgreSQL monitoring privileges.
SELECT format(
    'GRANT pg_monitor TO %I',
    :'exporter_user'
)
\gexec

-- Allow connections to the Mailforge database.
SELECT format(
    'GRANT CONNECT ON DATABASE %I TO %I',
    :'database_name',
    :'exporter_user'
)
\gexec
