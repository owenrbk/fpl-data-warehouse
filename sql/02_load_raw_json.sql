-- 02_load_raw_json.sql
-- Loads raw JSON data into raw_fpl
-- This can be run after placing your JSON files in the local data folder

INSERT INTO raw_fpl (source_filename, json_data)
VALUES ('bootstrap-static.json', '<replace_with_JSON_data_here>');
