# Databricks Case Study: Healthcare Data Platform
## Monitoring & Cost Optimization for Payer/Clinical Data

**Combined 20-minute presentation for Healthcare Data Engineering role**  
**150+ daily files: Claims, Clinical, Medical Records**

---

## 📁 Project Structure

This project follows Databricks Asset Bundle conventions:

```
DBX_Test/
├── src/              # Production code for deployment
│   ├── pipelines/    # DLT pipeline notebooks
│   └── schemas/      # Schema definitions and utilities
├── setup/            # One-time setup scripts (SQL, config)
├── tests/            # Test data generators and validation
├── docs/             # Architecture and implementation documentation
└── presentation/     # Demo materials (HTML presentations)
```

### Quick Navigation

- **Main Pipeline:** `src/pipelines/healthcare_dlt_pipeline.py`
- **Schema Config:** `src/schemas/schemas_config.py`
- **Setup SQL:** `setup/metadata_table_setup.sql`
- **Test Data Generator:** `tests/create_test_files.py`
- **Architecture Docs:** `docs/architecture.md`

---

## 📚 Documentation (Read These First)

1. **docs/architecture.md** - Complete medallion architecture with industry standards
2. **docs/healthcare_context.md** - ⭐ Healthcare-specific requirements (HIPAA, PHI protection)
3. **docs/hybrid_pipeline_guide.md** - ⭐ Hybrid approach: metadata-driven + cost optimization

---

## 💻 Production Code

### Main Pipeline
- **src/pipelines/healthcare_dlt_pipeline.py** - ⭐ Metadata-driven DLT pipeline with:
  - Bronze: Autoloader for incremental ingestion (3 file types)
  - Silver: Automated deduplication + PHI masking + custom join logic
  - Gold: Automated aggregations + cost optimization demos (skewed join vs optimized)

### Supporting Files
- **src/schemas/schemas_config.py** - Centralized schema definitions for all healthcare file types
- **setup/metadata_table_setup.sql** - Unity Catalog metadata table setup

---

## 🧪 Test Utilities

- **tests/create_test_files.py** - Generate skewed test data (80% to one provider for optimization demo)
- **tests/test_centralized_schemas.py** - Validate schema configurations

---

## 🎨 Presentation Materials

- **presentation/architecture_diagram.html** - Interactive architecture visualization
- **presentation/platform_presentation.html** - Full demo presentation with dynamic dashboard

---

## 🚀 Quick Start

### Local Development
1. Review architecture: `docs/architecture.md`
2. Understand healthcare context: `docs/healthcare_context.md`
3. Study hybrid pipeline approach: `docs/hybrid_pipeline_guide.md`

### Databricks Deployment
1. **Setup:** Run `setup/metadata_table_setup.sql` in Databricks SQL editor
2. **Upload:** Upload `src/` folder to Databricks workspace
3. **Generate Test Data:** Run `tests/create_test_files.py`
4. **Create DLT Pipeline:** Point to `src/pipelines/healthcare_dlt_pipeline.py`
5. **Run Pipeline:** Start and verify 9 tables created (3 Bronze, 4 Silver, 5 Gold)

---

## 🎯 Case Study Overview

**Healthcare Data Platform** (Payer/Provider Organization)

### Problem
- **Monitoring:** 150+ healthcare files/day (claims, clinical, eligibility)
  - Delayed claims processing affects revenue cycle ($millions)
  - Missing clinical data impacts patient care decisions
  - Compliance risk (HIPAA, audit requirements)
  
- **Cost:** 40% cost increase vs only 15% workload growth
  - Healthcare margins are thin (3-5%)
  - Cost efficiency critical for sustainability

### Solution
- **DLT Pipeline** - Handles complex healthcare formats (837/835 EDI, HL7, FHIR)
- **PHI Protection** - Deterministic hashing for HIPAA compliance
- **Custom Monitoring** - Volume anomaly detection, proactive stakeholder alerts
- **Cost Analysis** - Identify waste in claims processing infrastructure
- **99.5%+ Quality** - Healthcare-grade data quality standards

### Expected Outcomes
- **MTTR:** 2-4 hours → 15-30 min (80% reduction)
- **Cost Savings:** $480k/year (meaningful for healthcare margins)
- **Data Quality:** 99.7% (exceeds healthcare 99.5% standard)
- **Compliance:** Zero PHI exposure incidents, full audit trail
- **Code Reduction:** 70% less code using DLT

---

## 🚀 Presentation Flow (20 min)

### Part 1: Monitoring & Alerting (10 min)
1. **Architecture** (2 min) - ADLS → DLT → UC metrics
2. **Volume Anomaly Detection** (4 min) - Z-score, proactive alerts
3. **Demo** (3 min) - Show DLT code + monitoring layer
4. **Transition** (1 min) - "These metrics feed into cost analysis..."

### Part 2: Cost Optimization (10 min)
1. **Problem Analysis** (2 min) - 40% vs 15% gap
2. **Cost Analysis** (4 min) - System Tables + correlation
3. **Optimization Roadmap** (3 min) - Quick wins + savings
4. **Q&A** (1 min)

---

## 💡 Key Talking Points

**Why DLT?**
- 90% less code to maintain
- Built-in data quality, lineage, retries
- Focus on business logic, not infrastructure

**Proactive Monitoring:**
- Statistical anomaly detection (3σ threshold)
- Alert stakeholders BEFORE they complain
- "High volume ≠ failure, just needs more time"

**Cost Efficiency:**
- Data-driven optimization (not guessing)
- Quick wins: 30% savings in 2 weeks
- Integration: Same metrics for monitoring AND cost

---

## 📊 Technical Highlights

**Modern Stack:**
- Delta Live Tables (DLT) for pipelines
- **Streaming tables** for Bronze → Silver (incremental processing)
- **Batch aggregations** for Silver → Gold (complete dataset)
- Unity Catalog for metrics storage
- System Tables for cost analysis
- Autoloader (via DLT) for incremental ingestion

**Best Practices:**
- Declarative pipelines (not imperative)
- Streaming for transformations, batch for aggregations
- Statistical thresholds (not arbitrary)
- Metrics-driven decisions
- Proactive communication

---

## 🎓 What Makes This Stand Out

1. ✅ **Modern Approach** - DLT, not custom Autoloader code
2. ✅ **Production-Ready** - Real PySpark, not pseudocode
3. ✅ **Integrated Solution** - Not two separate case studies
4. ✅ **Quantified Impact** - $480k savings, 80% MTTR reduction
5. ✅ **Business Focus** - Proactive stakeholder communication

---

Good luck with your presentation! 🚀

