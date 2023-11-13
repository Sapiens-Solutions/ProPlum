CREATE SCHEMA ${target_schema} AUTHORIZATION "${owner}";

-- Permissions

GRANT ALL ON SCHEMA ${target_schema} TO "${owner}";