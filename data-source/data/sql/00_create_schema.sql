CREATE SCHEMA demo;
GRANT ALL ON SCHEMA demo TO devuser;

-- change search_path on a connection-level
SET search_path TO demo;

-- change search_path on a database-level
ALTER database "devdb" SET search_path TO demo;
