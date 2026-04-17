-- ============================================================
-- 00_bootstrap_dog_db.sql
-- Verifies schemas exist in DOG_DB for Andrew's streaming &
-- anomaly detection tables.
--
-- DOG_SCHEMA  = raw ingested data
-- DOG_TRANSFORM = curated / aggregated data
--
-- These schemas already exist in DOG_DB (created by admin).
-- ============================================================

USE ROLE TRAINING_ROLE;
USE DATABASE DOG_DB;
USE WAREHOUSE PANTHER_WH;

-- Verify schemas are accessible
USE SCHEMA DOG_DB.DOG_SCHEMA;
USE SCHEMA DOG_DB.DOG_TRANSFORM;
