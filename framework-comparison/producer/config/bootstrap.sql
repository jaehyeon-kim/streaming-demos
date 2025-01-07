CREATE SCHEMA ecommerce;
GRANT ALL ON SCHEMA ecommerce TO develop;

-- change search_path on a connection-level
SET search_path TO ecommerce;

-- change search_path on a database-level
ALTER database "develop" SET search_path TO ecommerce;
