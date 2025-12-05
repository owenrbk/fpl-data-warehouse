-- 00_create_schemas.sql
-- Creates separate schemas for different stages of the data pipeline

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;
