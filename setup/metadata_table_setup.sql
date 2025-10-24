%sql
-- =====================================================
-- Healthcare Data Pipeline - Metadata Table Setup
-- =====================================================
-- This creates a static metadata table for file processing configuration
-- Data stewards can enable/disable files via simple UPDATE statements

-- Create the metadata table in Unity Catalog
-- Note: Make sure 'healthcare' catalog and 'metadata' schema exist
CREATE SCHEMA IF NOT EXISTS healthcare.metadata;
CREATE TABLE IF NOT EXISTS healthcare.metadata.file_metadata (
    table_name STRING,
    domain STRING,
    source_path STRING,
    file_pattern STRING,
    file_format STRING,
    delimiter STRING,
    key_columns ARRAY<STRING>,
    required_columns ARRAY<STRING>,
    phi_columns ARRAY<STRING>,
    quality_threshold DOUBLE,
    retention_days INT,
    vacuum_days INT,
    is_enabled BOOLEAN,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Clear existing data (for demo purposes)
TRUNCATE TABLE healthcare.metadata.file_metadata;

-- Insert sample healthcare file configurations
INSERT INTO healthcare.metadata.file_metadata VALUES
-- Claims 837 (EDI format)
('claims_837', 'payer', '/mnt/adls/payer/claims', 'claims_837_*.csv', 'csv', '|',
 ARRAY('claim_id', 'member_id', 'service_date'),
 ARRAY('claim_id', 'member_id', 'provider_id', 'service_date', 'billed_amount', 'paid_amount'),
 ARRAY('member_id', 'provider_id', 'subscriber_name', 'date_of_birth'),
 99.5, 2555, 30, true, current_timestamp(), current_timestamp()),

-- Claims 835 (Payment format)
('claims_835', 'payer', '/mnt/adls/payer/payments', 'claims_835_*.csv', 'csv', '|',
 ARRAY('payment_id', 'claim_id'),
 ARRAY('payment_id', 'claim_id', 'payment_amount', 'payment_date', 'adjustment_reason'),
 ARRAY('member_id', 'provider_id'),
 99.5, 2555, 30, true, current_timestamp(), current_timestamp()),

-- HL7 Messages (Clinical data)
('hl7_messages', 'clinical', '/mnt/adls/clinical/hl7', 'hl7_*.json', 'json', null,
 ARRAY('message_id', 'patient_id', 'message_date'),
 ARRAY('message_id', 'patient_id', 'message_type', 'message_date', 'facility_id'),
 ARRAY('patient_id', 'provider_npi', 'patient_name', 'dob'),
 99.5, 2555, 30, true, current_timestamp(), current_timestamp()),

-- Eligibility Verification
('eligibility_verification', 'eligibility', '/mnt/adls/eligibility/verification', 'eligibility_*.csv', 'csv', ',',
 ARRAY('verification_id', 'member_id', 'verification_date'),
 ARRAY('verification_id', 'member_id', 'verification_status', 'verification_date', 'coverage_type'),
 ARRAY('member_id', 'subscriber_id', 'member_name'),
 99.5, 2555, 30, false, current_timestamp(), current_timestamp()),

-- Lab Results (Clinical data)
('lab_results', 'clinical', '/mnt/adls/clinical/lab', 'lab_*.parquet', 'parquet', null,
 ARRAY('lab_id', 'patient_id', 'test_date'),
 ARRAY('lab_id', 'patient_id', 'test_code', 'test_date', 'result_value', 'normal_range'),
 ARRAY('patient_id', 'patient_name', 'dob'),
 99.5, 2555, 30, false, current_timestamp(), current_timestamp());

-- =====================================================
-- Example Queries for Data Stewards
-- =====================================================

-- View all file configurations
-- SELECT * FROM healthcare.metadata.file_metadata ORDER BY domain, table_name;

-- View only enabled files
-- SELECT table_name, domain, file_pattern, is_enabled 
-- FROM healthcare.metadata.file_metadata 
-- WHERE is_enabled = true;

-- Enable a file for processing
-- UPDATE healthcare.metadata.file_metadata 
-- SET is_enabled = true, updated_date = current_timestamp()
-- WHERE table_name = 'eligibility_verification';

-- Disable a file from processing
-- UPDATE healthcare.metadata.file_metadata 
-- SET is_enabled = false, updated_date = current_timestamp()
-- WHERE table_name = 'claims_835';

-- Add a new file type
-- INSERT INTO healthcare.metadata.file_metadata VALUES
-- ('pharmacy_claims', 'pharmacy', '/mnt/adls/pharmacy/claims', 'pharmacy_*.csv', 'csv', ',',
--  ARRAY('claim_id', 'member_id', 'fill_date'),
--  ARRAY('claim_id', 'member_id', 'ndc_code', 'fill_date', 'quantity', 'cost'),
--  ARRAY('member_id', 'patient_name'),
--  99.5, 2555, 30, false, current_timestamp(), current_timestamp());

-- =====================================================
-- Verification Queries
-- =====================================================

-- Check table was created successfully
SELECT 'Metadata table created successfully' as status;

-- Show sample data
SELECT 
    table_name,
    domain,
    file_pattern,
    is_enabled,
    size(key_columns) as key_column_count,
    size(phi_columns) as phi_column_count
FROM healthcare.metadata.file_metadata
ORDER BY domain, table_name;
