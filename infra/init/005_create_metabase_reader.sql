CREATE SCHEMA IF NOT EXISTS analytics_staging;
CREATE SCHEMA IF NOT EXISTS analytics_intermediate;
CREATE SCHEMA IF NOT EXISTS analytics_marts;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'metabase_reader') THEN
        CREATE ROLE metabase_reader
            LOGIN
            PASSWORD 'local_only_read_only'
            NOSUPERUSER
            NOCREATEDB
            NOCREATEROLE
            NOINHERIT
            NOREPLICATION;
    ELSE
        ALTER ROLE metabase_reader
            WITH LOGIN
            PASSWORD 'local_only_read_only'
            NOSUPERUSER
            NOCREATEDB
            NOCREATEROLE
            NOINHERIT
            NOREPLICATION;
    END IF;
END;
$$;

REVOKE ALL ON SCHEMA raw, serving, ops, analytics FROM metabase_reader;
DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO metabase_reader', current_database());
END;
$$;
GRANT USAGE ON SCHEMA analytics_staging, analytics_intermediate, analytics_marts
    TO metabase_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics_staging, analytics_intermediate, analytics_marts
    TO metabase_reader;

ALTER DEFAULT PRIVILEGES FOR ROLE github_analytics IN SCHEMA analytics_staging
    GRANT SELECT ON TABLES TO metabase_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE github_analytics IN SCHEMA analytics_intermediate
    GRANT SELECT ON TABLES TO metabase_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE github_analytics IN SCHEMA analytics_marts
    GRANT SELECT ON TABLES TO metabase_reader;
