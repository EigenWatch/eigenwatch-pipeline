-- Initialize databases for EigenWatch Pipeline
-- This runs automatically when PostgreSQL container starts for the first time

-- Create databases
CREATE DATABASE eigenwatch_staging_db;
CREATE DATABASE eigenwatch_analytics;

-- Grant permissions (already owner, but explicit)
GRANT ALL PRIVILEGES ON DATABASE dagster TO postgres;
GRANT ALL PRIVILEGES ON DATABASE eigenwatch_staging_db TO postgres;
GRANT ALL PRIVILEGES ON DATABASE eigenwatch_analytics TO postgres;
