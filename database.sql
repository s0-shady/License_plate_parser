-- Database and user creation for license plates analysis project
-- a) User and table creation instructions

-- Create user (run as superuser)
CREATE USER license_plate_analyzer WITH 
    SUPERUSER 
    CREATEDB 
    CREATEROLE 
    REPLICATION 
    PASSWORD 'secure_password_123';

-- Create database
CREATE DATABASE license_plate_reports_db OWNER license_plate_analyzer;

-- Switch to project database
\c license_plate_reports_db;

-- Grant privileges to user
GRANT ALL PRIVILEGES ON DATABASE license_plate_reports_db TO license_plate_analyzer;
GRANT ALL PRIVILEGES ON SCHEMA public TO license_plate_analyzer;

-- Create main table for storing reports
CREATE TABLE reports (
    id SERIAL PRIMARY KEY,
    license_plate VARCHAR(20) NOT NULL,
    region_code VARCHAR(10) NOT NULL,
    report_date TIMESTAMP NOT NULL,
    comment_text TEXT,
    author_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_license_plate ON reports (license_plate);
CREATE INDEX idx_region_code ON reports (region_code);
CREATE INDEX idx_report_date ON reports (report_date);

-- Grant privileges to table
GRANT ALL PRIVILEGES ON TABLE reports TO license_plate_analyzer;
GRANT ALL PRIVILEGES ON SEQUENCE reports_id_seq TO license_plate_analyzer;

-- Create regional statistics table for optimization
CREATE TABLE regional_statistics (
    region_code VARCHAR(10) PRIMARY KEY,
    report_count INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

GRANT ALL PRIVILEGES ON TABLE regional_statistics TO license_plate_analyzer;

-- c) Queries showing number of reports by regional part (from highest count)
-- Analysis of regional "bad behavior density" :)

-- Basic query - number of reports by regional part
SELECT 
    region_code,
    COUNT(*) as report_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage_of_total
FROM reports 
GROUP BY region_code 
ORDER BY report_count DESC, region_code ASC;

-- Extended query with additional statistics
SELECT 
    region_code,
    COUNT(*) as report_count,
    COUNT(DISTINCT license_plate) as unique_plates,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT license_plate), 2) as avg_reports_per_plate,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage_of_total,
    MIN(report_date) as first_report,
    MAX(report_date) as last_report
FROM reports 
GROUP BY region_code 
HAVING COUNT(*) >= 5  -- Only regions with at least 5 reports
ORDER BY report_count DESC, region_code ASC;

-- Top 20 regions with highest "bad behavior density"
SELECT 
    region_code,
    COUNT(*) as report_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage_of_total,
    -- Additional column with interpretation
    CASE 
        WHEN COUNT(*) > 1000 THEN 'EXTREME BAD BEHAVIOR'
        WHEN COUNT(*) > 500 THEN 'HIGH BAD BEHAVIOR'
        WHEN COUNT(*) > 200 THEN 'MEDIUM BAD BEHAVIOR'
        WHEN COUNT(*) > 50 THEN 'LOW BAD BEHAVIOR'
        ELSE 'SPORADIC BAD BEHAVIOR'
    END as behavior_level
FROM reports 
GROUP BY region_code 
ORDER BY report_count DESC 
LIMIT 20;

-- Time analysis - bad behavior over time
SELECT 
    DATE_TRUNC('month', report_date) as month_period,
    region_code,
    COUNT(*) as report_count
FROM reports 
WHERE report_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY month_period, region_code
HAVING COUNT(*) >= 10
ORDER BY month_period DESC, report_count DESC;

-- Ranking worst vs national average
WITH national_average AS (
    SELECT AVG(report_count) as average_count
    FROM (
        SELECT COUNT(*) as report_count 
        FROM reports 
        GROUP BY region_code
    ) sub
)
SELECT 
    r.region_code,
    COUNT(*) as report_count,
    na.average_count,
    ROUND(COUNT(*) / na.average_count, 2) as bad_behavior_coefficient
FROM reports r
CROSS JOIN national_average na
GROUP BY r.region_code, na.average_count
HAVING COUNT(*) > na.average_count
ORDER BY bad_behavior_coefficient DESC;

-- Daily statistics
SELECT 
    DATE(report_date) as report_day,
    COUNT(*) as daily_reports,
    COUNT(DISTINCT region_code) as regions_active,
    COUNT(DISTINCT license_plate) as unique_plates
FROM reports 
WHERE report_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(report_date)
ORDER BY report_day DESC;

-- Most frequently reported license plates
SELECT 
    license_plate,
    region_code,
    COUNT(*) as report_count,
    MIN(report_date) as first_report,
    MAX(report_date) as last_report,
    COUNT(DISTINCT DATE(report_date)) as days_reported
FROM reports 
GROUP BY license_plate, region_code
HAVING COUNT(*) > 1
ORDER BY report_count DESC, last_report DESC
LIMIT 50;

-- Hourly analysis - when are most reports submitted
SELECT 
    EXTRACT(HOUR FROM report_date) as hour_of_day,
    COUNT(*) as report_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM reports 
GROUP BY EXTRACT(HOUR FROM report_date)
ORDER BY hour_of_day;

-- Regional analysis with geographical insights
SELECT 
    region_code,
    COUNT(*) as total_reports,
    COUNT(DISTINCT license_plate) as unique_vehicles,
    ROUND(AVG(LENGTH(comment_text)), 1) as avg_comment_length,
    COUNT(*) FILTER (WHERE comment_text IS NOT NULL AND LENGTH(comment_text) > 50) as detailed_reports,
    MIN(report_date) as first_report,
    MAX(report_date) as latest_report,
    EXTRACT(DAYS FROM (MAX(report_date) - MIN(report_date))) as reporting_period_days
FROM reports 
GROUP BY region_code
HAVING COUNT(*) >= 10
ORDER BY total_reports DESC;

-- Author analysis (anonymous vs named reports)
SELECT 
    CASE 
        WHEN author_name = 'Anonymous' OR author_name = 'Anonim' THEN 'Anonymous'
        WHEN LENGTH(author_name) <= 3 THEN 'Short Name/Initials'
        WHEN author_name ~ '^[A-Z][a-z]+ [A-Z][a-z]+$' THEN 'Full Name'
        ELSE 'Other'
    END as author_type,
    COUNT(*) as report_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
    ROUND(AVG(LENGTH(comment_text)), 1) as avg_comment_length
FROM reports 
WHERE author_name IS NOT NULL
GROUP BY author_type
ORDER BY report_count DESC;

-- Comprehensive dashboard query
SELECT 
    'Total Reports' as metric,
    COUNT(*)::text as value,
    'All time' as period
FROM reports
UNION ALL
SELECT 
    'Unique License Plates',
    COUNT(DISTINCT license_plate)::text,
    'All time'
FROM reports
UNION ALL
SELECT 
    'Active Regions',
    COUNT(DISTINCT region_code)::text,
    'All time'
FROM reports
UNION ALL
SELECT 
    'Reports Last 24h',
    COUNT(*)::text,
    'Last 24 hours'
FROM reports 
WHERE report_date >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
UNION ALL
SELECT 
    'Most Active Region',
    region_code,
    COUNT(*)::text || ' reports'
FROM reports 
GROUP BY region_code
ORDER BY COUNT(*) DESC
LIMIT 1;

-- Data quality checks
SELECT 
    'Null license plates' as check_type,
    COUNT(*) as count
FROM reports 
WHERE license_plate IS NULL
UNION ALL
SELECT 
    'Null region codes',
    COUNT(*)
FROM reports 
WHERE region_code IS NULL
UNION ALL
SELECT 
    'Future dates',
    COUNT(*)
FROM reports 
WHERE report_date > CURRENT_TIMESTAMP
UNION ALL
SELECT 
    'Very old dates (before 2020)',
    COUNT(*)
FROM reports 
WHERE report_date < '2020-01-01'
UNION ALL
SELECT 
    'Unknown region codes',
    COUNT(*)
FROM reports 
WHERE region_code = 'UNK';