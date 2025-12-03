-- 01_create_raw_table.sql
-- Creates a table to store raw JSON data from the FPL API
-- Each row contains a full JSON payload, filename, and timestamp

CREATE TABLE raw.raw_fpl (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT NOW(),
    source_filename TEXT,
    json_data JSONB
);
