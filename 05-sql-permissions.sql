-- restore sql permissions
-- sudo -i -u postgres
-- psql -d tradestatistics
CREATE ROLE tradestatistics WITH PASSWORD 'blablabla';
ALTER ROLE tradestatistics WITH LOGIN;
GRANT CONNECT ON DATABASE tradestatistics TO tradestatistics;
GRANT USAGE ON SCHEMA public TO tradestatistics;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO tradestatistics;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO tradestatistics;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO tradestatistics;
REVOKE CREATE ON SCHEMA public FROM public;
