DROP SCHEMA IF EXISTS raw_data_import CASCADE;
DROP USER MAPPING IF EXISTS FOR {{ target.user }} SERVER raw_data_server;
DROP SERVER IF EXISTS raw_data_server CASCADE;