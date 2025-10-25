# PII Incident Response - Interview Talking Points

## Overview
This document provides talking points for the PII incident response presentation. Use this to prepare for interview questions about PHI leak scenarios, investigation, containment, and prevention.

---

## SCENARIO: A user accidentally accessed PII data they shouldn't have

---

## SECTION 1: Reasons for PII Leak - "How Could This Happen?"

### Talking Point 1: Schema Drift (Without Rescue Mode)

**What to Say:**
> "The most insidious risk is schema drift without rescue mode. Here's what happens:
> 
> A source system adds a new PHI field like 'patient_ssn'. With Auto Loader's default 'addNewColumns' mode, it auto-ingests to Bronze **unmasked** with **no alert**. It sits undetected for weeks or months until an audit catches it.
> 
> That's why we use **rescue mode** instead. With rescue mode, new columns go to `_rescued_data` and we can monitor for them. The stream doesn't fail, but new columns don't auto-add either."

**If They Ask: "Why not just use predefined schemas?"**
> "Rescue mode gives us the best of both worlds. We define schemas in `schemas_config.py` as hints, but rescue mode ensures new columns can't sneak in. Less maintenance than strict schemas, more security than schema inference."

---

### Talking Point 2: Incorrect RBAC Setup

**What to Say:**
> "The second scenario is Bronze tables granted to broad groups - maybe 50+ users in 'data-analysts' group. No Unity Catalog access controls configured. You're relying on 'trust' instead of technical controls.
> 
> Bronze should be **Red Zone** - only 5-8 authorized users: compliance team and data engineering leads. We use Unity Catalog RBAC with Azure AD integration via SCIM. Groups are managed in Azure AD and auto-sync to Databricks."

---

### Talking Point 3: No Column Masking

**What to Say:**
> "PHI flows from Bronze to Silver unmasked. Analysts are granted Silver access because they need it for joins. Result: 20+ analysts see raw patient_id, provider_id.
> 
> Our approach: **SHA256 masking at Silver layer**. Analysts get Silver access but only see hashed PHI. If they need to join on patient_id, the hash works perfectly. They never see the raw value."

---

### Talking Point 4: Materialized View Exposure

**What to Say:**
> "An admin creates a materialized view from Bronze with raw PHI and grants access to analysts. Users query the copy, bypassing Bronze permissions entirely.
> 
> Prevention: Unity Catalog tracks data lineage. We can query `system.access.audit` to find any views or tables derived from Bronze and audit their permissions quarterly."

---

## SECTION 2: Ideal Architecture - "What Should Exist"

### Talking Point 5: Three-Zone Access Control

**What to Say:**
> "We implement a **three-zone model**:
> 
> **🔴 RED ZONE (Bronze)** - Raw PHI, unmasked
> - Access: Only compliance team + DE leads (5-8 users)
> - Requirements: Manager approval + HIPAA training
> 
> **🟡 YELLOW ZONE (Silver)** - Masked/Hashed PHI
> - Access: Authorized analysts (20-30 users)
> - PHI is SHA256 hashed, functional for joins but not readable
> - Requirements: Manager approval + training
> 
> **🟢 GREEN ZONE (Gold)** - Fully anonymized
> - Access: All analysts (100+), BI tools, self-service
> - Requirements: None - open access
> 
> **Key principle:** As PHI risk decreases (Bronze → Gold), access permissions expand. This is **principle of least privilege** in action."

---

## SECTION 3: Investigation - "Accessing Audit Logs"

### Talking Point 6: Unity Catalog Audit Logs

**What to Say:**
> "First step when we detect unauthorized access: query `system.access.audit` table. This tracks every data access in Databricks - who, what, when, which query.
> 
> Key query: Find anyone who accessed Bronze tables who shouldn't have:
> ```sql
> SELECT event_time, user_identity.email, 
>        request_params.full_name_arg AS table_accessed
> FROM system.access.audit
> WHERE request_params.full_name_arg LIKE '%bronze%'
>   AND user_identity.email NOT IN ('compliance-team')
> ORDER BY event_time DESC
> ```
> 
> This gives us the scope of exposure within minutes, not weeks."

**If They Ask: "How far back does audit go?"**
> "Unity Catalog audit logs retain 1 year by default. For compliance, we also export critical audit events to long-term storage (7 years for HIPAA)."

---

## SECTION 4: Immediate Steps - "Containment Actions"

### Talking Point 7: Revoke, Notify, Document

**What to Say:**
> "We have a three-step containment process:
> 
> **Step 1: REVOKE ACCESS** (Immediate - 5 minutes)
> ```sql
> REVOKE SELECT ON SCHEMA healthcare.bronze 
> FROM `data-analysts@company.com`;
> ```
> 
> **Step 2: NOTIFY STAKEHOLDERS** (Within 15 minutes)
> - Compliance officer
> - Security team  
> - User's manager
> - HIPAA privacy officer if PHI exposed
> 
> **Step 3: DOCUMENT INCIDENT** (Within 1 hour)
> - Who accessed what data
> - Time range of exposure
> - Which PHI fields were exposed
> - Actions taken to remediate
> - Whether breach reporting required (depends on scope)
> 
> This is our **1-hour incident response SLA**."

---

## SECTION 5: Prevention & Deletion - "Fix Root Causes"

### Talking Point 8: Four-Pillar Prevention Strategy

**What to Say:**
> "We implement four preventive controls:
> 
> **1. Harden RBAC** - Audit all Bronze GRANTs, remove broad groups, quarterly access reviews
> 
> **2. Implement Column Masking** - SHA256 at Silver layer, analysts only see hashed PHI
> 
> **3. Rescue Mode Schema Control** - Set `cloudFiles.schemaEvolutionMode = "rescue"`, monitor `_rescued_data` for schema drift, alert within 15 minutes
> 
> **4. Real-Time Monitoring** - Query `system.access.audit` every 15 minutes for unauthorized Bronze access, automatic security alerts
> 
> These shift us from **reactive** (finding issues in audits) to **proactive** (preventing issues at ingestion)."

---

### Talking Point 9: Data Deletion (If Required)

**What to Say:**
> "If we need to delete exposed PHI, we have a multi-step process:
> 
> **Immediate (Soft Delete):**
> ```sql
> UPDATE healthcare.bronze.claims_837
> SET patient_id = 'REDACTED', provider_id = 'REDACTED'
> WHERE patient_id = 'PAT-12345';
> ```
> 
> **Silver Layer (Hashed match):**
> ```sql
> DELETE FROM healthcare.silver.claims_837
> WHERE SHA2(patient_id, 256) = SHA2('PAT-12345', 256);
> ```
> 
> **Gold Layer:** Should be anonymized already, but re-aggregate if needed
> 
> **After Audit Retention (e.g., 30 days):**
> ```sql
> VACUUM healthcare.bronze.claims_837 RETAIN 0 HOURS;
> ```
> 
> Important: Even after DELETE, historical versions exist in Delta Lake. VACUUM physically removes them, but you lose time-travel. We balance compliance (immediate deletion) with audit requirements (retain 30 days)."

**If They Ask: "What about downstream systems?"**
> "Great question. We maintain a data lineage registry. If PHI needs deletion, we identify all downstream consumers (ML models, reports, exports) and coordinate deletion there too. This is where Unity Catalog's lineage tracking is invaluable."

---

## SECTION 6: Long-Term Solution - "Architecture Changes"

### Talking Point 10: Enterprise-Grade Security Model

**What to Say:**
> "Long-term, we're implementing a comprehensive security architecture:
> 
> **🔐 Unity Catalog RBAC** - Integrate with Azure AD via SCIM, groups auto-sync
> 
> **🛡️ Rescue Mode Schema Control** - Auto Loader with rescue mode, `_rescued_data` monitoring, 15-minute alerts
> 
> **🎭 Dynamic Masking** - Unity Catalog column-level masking, automatic based on group membership
> 
> **📊 Real-Time Monitoring** - Automated audit log queries every 15 minutes, 1-hour incident response SLA
> 
> **🔍 Quarterly Access Reviews** - Automated reports, manager approval required, stale permissions auto-expire after 90 days
> 
> **📝 Compliance Automation** - Automatic incident logging, deletion tracking, HIPAA-ready audit certificates
> 
> This gives us **Detection in 15 minutes**, **Containment in 1 hour**, **Prevention in 24 hours**. Compare that to the old model: discover issues in audits 6 months later."

---

## KEY INTERVIEW RESPONSES

### "How do you handle schema drift?"
> "We use Auto Loader's rescue mode. New columns automatically go to `_rescued_data` instead of being auto-added. We can monitor for non-null values in that column. This gives us schema control without maintaining complex schema configs."

### "What's your incident response time?"
> "Detection: 15 minutes via automated audit log monitoring. Containment: 1 hour (revoke access, notify stakeholders, document). Prevention: 24 hours (harden RBAC, implement masking). This is our SLA."

### "How do you balance data access with security?"
> "Three-zone model. Bronze is locked down (5-8 users), Silver has masked PHI for authorized analysts (20-30 users), Gold is open for all (100+). As risk decreases, access expands. Principle of least privilege."

### "What if someone needs to see raw PHI legitimately?"
> "They need Bronze access. This requires manager approval + HIPAA training + compliance team review. We grant it via Unity Catalog groups synced from Azure AD. All access is logged in `system.access.audit` for compliance tracking."

### "How do you prove compliance to auditors?"
> "Unity Catalog audit logs show every access with user, timestamp, query, and result. We can generate reports showing: who accessed what PHI, when, and why (approved access vs. incident). We export these logs to long-term storage for 7-year HIPAA retention."

---

## PRACTICE FLOW

Walk through the presentation in this order:
1. **Problem** (Section 1) - 4 ways PII leaks happen
2. **Ideal State** (Section 2) - 3-zone access model
3. **Investigation** (Section 3) - Audit log queries
4. **Containment** (Section 4) - 3 immediate steps
5. **Prevention** (Section 5) - 4 preventive controls + deletion process
6. **Long-term** (Section 6) - Enterprise architecture

**Time:** 10-15 minutes for full walkthrough, 5 minutes for executive summary

---

## FINAL TIPS

- **Be confident about rescue mode** - It's simpler and just as effective as predefined schemas
- **Emphasize proactive vs. reactive** - This is the key differentiator
- **Know the SQL queries** - They're simple but impressive in an interview
- **Time matters** - 15 min detection, 1 hour containment, 24 hour prevention
- **Three zones** - Red (Bronze), Yellow (Silver), Green (Gold) - easy to remember

**You've got this!** 🎯🔒🌿
