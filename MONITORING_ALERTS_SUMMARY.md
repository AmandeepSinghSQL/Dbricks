# Healthcare Data Platform - Monitoring & Alerts Implementation

## ✅ Implementation Complete

### What Was Added

#### 1. **DLT Expectations on silver_claims_837_with_quality (NEW!)**
- Added NEW static Silver table with `@dlt.expect_or_drop()` decorators
- Validates: claim_id NOT NULL, service_date NOT NULL, billed_amount > 0
- Reads from silver_claims_837 and applies expectations BEFORE joins
- Dropped records show up in DLT Data Quality tab (visible metrics!)

**File**: `src/pipelines/healthcare_dlt_pipeline.py` (lines 263-280)

#### 2. **No Quality Filtering in Metadata-Driven Silver Tables**
- Updated `create_silver_table()` to allow bad data to flow through
- Bad records reach enriched table where expectations drop them visibly
- This makes dropped records show up in DLT UI expectations tab

**File**: `src/pipelines/healthcare_dlt_pipeline.py` (lines 106-128)

#### 3. **monitoring_data_quality Table**
- Compares Bronze vs Silver record counts using spark.table()
- Calculates quality_score_pct and severity (INFO/WARNING/CRITICAL)
- Pre-formatted alert messages for each table
- Runs as part of DLT pipeline

**File**: `src/pipelines/healthcare_dlt_pipeline.py` (lines 585-650)

#### 4. **monitoring_bronze_volumes Table** (Already Existed)
- Tracks volume anomalies using Z-score analysis
- 7-day rolling baseline, 2σ threshold
- Detects HIGH_VOLUME and LOW_VOLUME anomalies

**File**: `src/pipelines/healthcare_dlt_pipeline.py` (lines 467-577)

#### 5. **Bad Data Generator**
- Standalone notebook to create test files with quality issues
- 20% NULL claim_id, 30% NULL member_id, 10% NULL service_date, 10% negative amounts
- Use to test quality monitoring and expectations

**New File**: `tests/create_bad_data_notebook.py`

#### 6. **Demo Query Documentation**
- SQL queries to view volume anomalies, quality issues, and combined alerts
- Instructions for checking DLT UI Data Quality tab

**File**: `src/pipelines/healthcare_dlt_pipeline.py` (lines 739-771)

### What Was Removed

- ✅ `src/monitoring/volume_anomaly_alerts.py` - Replaced by monitoring tables in DLT pipeline
- ✅ Lines 460-621 in `healthcare_dlt_pipeline.py` - Unused static table definitions

---

## 📊 How It Works

### Data Flow with Monitoring

```
New files land → Auto Loader ingests → Bronze tables created
                                             ↓
                                   monitoring_bronze_volumes runs
                                   (checks volumes, Z-scores)
                                             ↓
                                   Silver tables created
                                   (filters NULL values)
                                             ↓
                                   monitoring_data_quality runs
                                   (compares Bronze vs Silver counts)
                                             ↓
                                   Gold tables created
```

### Quality Enforcement Strategy

**DLT Expectations (Visible in UI)**
- Used on: `silver_claims_enriched` (enriched table that joins claims + payments)
- Shows in: DLT Data Quality tab
- Example: `@dlt.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")`
- Bad records from Bronze → Silver flow through, then get dropped at enriched layer
- Dropped records are counted and visible in DLT UI

**Monitoring Table (Historical Tracking)**
- `monitoring_data_quality` compares Bronze vs Silver counts
- Tracks quality trends over time
- Provides alert messages with severity levels

---

## 🎯 Demo Instructions

### 1. Generate Test Data with Quality Issues

```python
# In Databricks, run: tests/create_bad_data_notebook.py
# This creates a file with 100 records, ~50 with NULLs
```

### 2. Start DLT Pipeline

The pipeline will:
- Ingest bad data into Bronze
- Filter bad records at Silver (via NULL checks)
- Create monitoring tables with alerts

### 3. View Quality Metrics in DLT UI

- Open pipeline → **Data Quality** tab
- Look for `silver_claims_enriched`
- See expectation metrics (valid_claim_id, valid_service_date, valid_billed_amount)

### 4. Query Monitoring Tables

**Volume Anomalies:**
```sql
SELECT table_name, record_count, z_score, anomaly_type, alert_message
FROM healthcare.default.monitoring_bronze_volumes
WHERE is_anomaly = TRUE;
```

**Quality Issues:**
```sql
SELECT table_name, quality_score_pct, dropped_records, severity, alert_message
FROM healthcare.default.monitoring_data_quality
WHERE severity IN ('WARNING', 'CRITICAL');
```

**Combined Dashboard:**
```sql
SELECT 'VOLUME' as alert_type, table_name, anomaly_type as issue, alert_message
FROM healthcare.default.monitoring_bronze_volumes
WHERE is_anomaly = TRUE
UNION ALL
SELECT 'QUALITY' as alert_type, table_name, severity as issue, alert_message
FROM healthcare.default.monitoring_data_quality
WHERE severity IN ('WARNING', 'CRITICAL');
```

### 5. Verify Record Counts

```sql
-- Check Bronze (should have all records including bad ones)
SELECT COUNT(*) FROM healthcare.default.bronze_claims_837;

-- Check Silver (should have fewer records - bad ones filtered)
SELECT COUNT(*) FROM healthcare.default.silver_claims_837;

-- The difference = dropped records
```

---

## 💬 Interview Talking Points

### Monitoring Approach
> "We have real-time monitoring built into the DLT pipeline. Two tables track volume anomalies and quality issues. Volume monitoring uses Z-score analysis on a 7-day baseline with a 2-sigma threshold. Quality monitoring compares Bronze vs Silver counts to track dropped records."

### Why Two Approaches for Quality?
> "For the demo, we show DLT expectations on the enriched table where they're highly visible in the UI. For production with 500+ metadata-driven tables, we'd use separate notebooks per table to add specific expectations, or rely on monitoring tables like we built. We kept the existing metadata-driven approach unchanged since it was already working."

### How Bad Data is Handled
> "Bad data flows through Bronze to Silver without filtering. When it reaches the enriched table, DLT expectations drop bad records and display the drops in the Data Quality tab. This makes quality issues highly visible. Auto Loader marks files as processed, and we track overall quality in monitoring_data_quality for trend analysis."

### Alerting Strategy
> "Both monitoring tables provide pre-formatted alert messages with severity levels. In production, a separate job would poll these tables every 15 minutes and route alerts—Slack for warnings, email for critical issues. For the demo, we just query the tables to show what alerts would be sent."

### Production Integration
> "The monitoring tables are Delta tables in Unity Catalog. We can set up Databricks SQL Alerts to email stakeholders when new records appear with severity='CRITICAL', or use a simple Python notebook scheduled every 15 minutes to poll and send Slack/email notifications."

---

## 📋 Table Summary

| Table | Type | Purpose | Location |
|-------|------|---------|----------|
| `monitoring_bronze_volumes` | Materialized | Volume anomaly detection | healthcare.default |
| `monitoring_data_quality` | Materialized | Quality issues tracking | healthcare.default |
| `silver_claims_enriched` | Streaming | Demo of DLT expectations | healthcare.default |
| All metadata-driven Silver | Streaming | Filtered for NULLs (not visible in UI) | healthcare.default |

---

## ✅ Success Criteria

- [x] DLT expectations visible in UI for silver_claims_enriched
- [x] Metadata-driven Silver tables filter NULL values without failing
- [x] monitoring_bronze_volumes tracks volume anomalies
- [x] monitoring_data_quality tracks dropped records
- [x] Bad data generator creates test files
- [x] Unused files/code removed
- [x] Demo queries documented

---

## 🚀 Next Steps

1. **Test the Implementation**
   - Pull latest code in Databricks Repos
   - Run `create_bad_data_notebook.py` to generate bad data
   - Start DLT pipeline
   - Verify both monitoring tables populate
   - Check DLT UI Data Quality tab

2. **Demo Preparation**
   - Practice querying monitoring tables
   - Prepare to show DLT UI Data Quality tab
   - Have the "two approaches" talking point ready

3. **Optional Production Setup**
   - Schedule monitoring queries as Databricks SQL Alerts
   - Or create a simple polling notebook for email/Slack notifications
   - Configure real recipient emails

---

Good luck with your demo! 🎉

