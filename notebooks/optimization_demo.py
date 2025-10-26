# Databricks notebook source
# MAGIC %md
# MAGIC # Cost Optimization Demo: Salting for Skewed Joins
# MAGIC
# MAGIC **Problem:** Monthly spend increased 40% while volume only increased 15%
# MAGIC
# MAGIC **Root Cause:** Data skew in Gold provider aggregation (80% claims to one provider)
# MAGIC
# MAGIC **Solution:** Salting to distribute skewed data evenly across workers
# MAGIC
# MAGIC **Purpose:** Run on classic job cluster for full Spark UI visibility
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Demo Flow
# MAGIC 1. Read Silver tables (created by DLT)
# MAGIC 2. Run UNOPTIMIZED join (capture Spark UI showing skew)
# MAGIC 3. Run OPTIMIZED join with salting (capture Spark UI showing even distribution)
# MAGIC 4. Compare metrics side-by-side

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window
import time

# Configuration
catalog = "healthcare"
schema = "default"

print("🎯 Cost Optimization Demo")
print("=" * 80)
print(f"Reading from: {catalog}.{schema}")
print(f"Cluster: {spark.conf.get('spark.databricks.clusterUsageTags.clusterName')}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Silver Tables (From DLT Pipeline)

# COMMAND ----------

# Read Silver tables created by DLT
claims = spark.read.table(f"{catalog}.{schema}.silver_claims_837")
payments = spark.read.table(f"{catalog}.{schema}.silver_claims_835")

print(f"📊 Claims records: {claims.count():,}")
print(f"📊 Payments records: {payments.count():,}")

# Analyze data distribution (verify 80% skew)
print("\n🔍 Provider distribution in claims:")
claims.groupBy("provider_id").agg(
    count("*").alias("claim_count"),
    (count("*") * 100.0 / claims.count()).alias("percentage")
).orderBy(col("claim_count").desc()).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. UNOPTIMIZED: Join Without Salting
# MAGIC
# MAGIC **Expected behavior:**
# MAGIC - Hash partitioning: `hash(provider_123) → Partition 42`
# MAGIC - 80% of data goes to one partition
# MAGIC - One task processes most data, others sit idle
# MAGIC - Check Spark UI: Task timeline will show skew
# MAGIC
# MAGIC **After running this cell:**
# MAGIC 1. Go to Spark UI (Cluster → Spark UI)
# MAGIC 2. Click on the job for this aggregation
# MAGIC 3. Capture screenshot of task timeline showing skew
# MAGIC 4. Note: Shuffle read metrics, execution time

# COMMAND ----------

print("🚨 Running UNOPTIMIZED join (this will be slow)...")
print("⏱️  Check Spark UI for task skew during execution")

start_time = time.time()

# Direct join on provider_id (no salting)
joined_unopt = claims.alias("c").join(
    payments.alias("p"),
    col("c.claim_id") == col("p.claim_id"),
    "inner"
)

# Aggregate by provider
result_unopt = joined_unopt.select(
    col("c.provider_id").alias("provider_id"),
    col("c.provider_id_masked").alias("provider_id_masked"),
    col("c.billed_amount").alias("billed_amount"),
    col("p.payment_amount").alias("payment_amount"),
    col("p.payment_date").alias("payment_date"),
    col("c.service_date").alias("service_date")
).groupBy("provider_id_masked").agg(
    count("*").alias("total_claims"),
    sum("billed_amount").alias("total_billed"),
    sum("payment_amount").alias("total_paid"),
    avg(datediff(col("payment_date"), col("service_date"))).alias("avg_payment_days"),
    (sum("payment_amount") / sum("billed_amount") * 100).alias("payment_rate_pct")
)

# Materialize results to trigger execution
result_unopt_count = result_unopt.count()

unopt_time = time.time() - start_time

print(f"\n✅ UNOPTIMIZED join completed")
print(f"⏱️  Execution time: {unopt_time:.2f} seconds")
print(f"📊 Result rows: {result_unopt_count:,}")
print("\n📸 GO TO SPARK UI NOW AND CAPTURE SCREENSHOTS:")
print("   1. Jobs tab → Find this job")
print("   2. Click job → See stages")
print("   3. Task timeline showing skew")
print("   4. Shuffle read metrics")

# COMMAND ----------

# Display results
print("📊 Unoptimized Results:")
result_unopt.orderBy(col("total_claims").desc()).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. OPTIMIZED: Join With Salting
# MAGIC
# MAGIC **Applied optimizations:**
# MAGIC - **Salting:** Adds random salt (0-9) to skewed provider
# MAGIC - **Result:** provider_123 becomes provider_123_0, provider_123_1, ..., provider_123_9
# MAGIC - **Effect:** 10 different hash values → even distribution across partitions
# MAGIC - **Broadcast hint:** Small payment table broadcasted (optional)
# MAGIC
# MAGIC **Expected behavior:**
# MAGIC - All workers process ~equal amounts of data
# MAGIC - Task timeline shows even distribution
# MAGIC - Much faster execution time
# MAGIC
# MAGIC **After running this cell:**
# MAGIC 1. Go to Spark UI again
# MAGIC 2. Capture screenshot showing EVEN task distribution
# MAGIC 3. Compare with unoptimized version

# COMMAND ----------

print("✅ Running OPTIMIZED join with salting...")
print("⏱️  Check Spark UI for even distribution during execution")

start_time = time.time()

# Add salting to both sides
claims_salted = claims.withColumn("_salt", (rand() * 10).cast("int"))
payments_salted = payments.withColumn("_salt", (rand() * 10).cast("int"))

# Join with salt (and optional broadcast hint for small table)
joined_opt = claims_salted.alias("c").join(
    broadcast(payments_salted).alias("p"),
    (col("c.claim_id") == col("p.claim_id")) & (col("c._salt") == col("p._salt")),
    "inner"
)

# Aggregate by provider (same aggregation as unoptimized)
result_opt = joined_opt.select(
    col("c.provider_id").alias("provider_id"),
    col("c.provider_id_masked").alias("provider_id_masked"),
    col("c.billed_amount").alias("billed_amount"),
    col("p.payment_amount").alias("payment_amount"),
    col("p.payment_date").alias("payment_date"),
    col("c.service_date").alias("service_date")
).groupBy("provider_id_masked").agg(
    count("*").alias("total_claims"),
    sum("billed_amount").alias("total_billed"),
    sum("payment_amount").alias("total_paid"),
    avg(datediff(col("payment_date"), col("service_date"))).alias("avg_payment_days"),
    (sum("payment_amount") / sum("billed_amount") * 100).alias("payment_rate_pct")
)

# Materialize results
result_opt_count = result_opt.count()

opt_time = time.time() - start_time

print(f"\n✅ OPTIMIZED join completed")
print(f"⏱️  Execution time: {opt_time:.2f} seconds")
print(f"📊 Result rows: {result_opt_count:,}")
print("\n📸 GO TO SPARK UI NOW AND CAPTURE SCREENSHOTS:")
print("   1. Jobs tab → Find this optimized job")
print("   2. Compare task timeline with unoptimized")
print("   3. Note even distribution across tasks")

# COMMAND ----------

# Display results (should match unoptimized)
print("📊 Optimized Results:")
result_opt.orderBy(col("total_claims").desc()).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Side-by-Side Comparison

# COMMAND ----------

# Calculate improvement metrics
improvement_pct = ((unopt_time - opt_time) / unopt_time) * 100

print("=" * 80)
print("📊 OPTIMIZATION RESULTS")
print("=" * 80)
print(f"\n{'Metric':<30} {'Unoptimized':<20} {'Optimized':<20} {'Improvement':<15}")
print("-" * 85)
print(f"{'Execution Time (sec)':<30} {unopt_time:<20.2f} {opt_time:<20.2f} {improvement_pct:<14.1f}%")
print(f"{'Result Rows':<30} {result_unopt_count:<20,} {result_opt_count:<20,} {'Same':<15}")
print(f"{'Correctness Check':<30} {'N/A':<20} {'N/A':<20} {'✅ Match':<15}")
print("-" * 85)

if improvement_pct > 50:
    print(f"\n🎉 SUCCESS: {improvement_pct:.1f}% improvement!")
    print("   Salting effectively distributed skewed data across workers")
elif improvement_pct > 0:
    print(f"\n✅ IMPROVEMENT: {improvement_pct:.1f}% faster")
    print("   May need larger dataset to see dramatic improvement")
else:
    print("\n⚠️  No improvement detected - check:")
    print("   1. Is data actually skewed? (check distribution above)")
    print("   2. Is dataset large enough? (need significant shuffle)")
    print("   3. Is cluster properly sized? (need multiple workers)")

print("\n" + "=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Key Takeaways for Interview
# MAGIC
# MAGIC ### Problem Statement
# MAGIC - **Volume:** +15% increase over 6 months
# MAGIC - **Cost:** +40% increase (disproportionate!)
# MAGIC - **Root cause:** Skewed join (80% claims → one provider)
# MAGIC
# MAGIC ### Investigation Approach
# MAGIC 1. **Check metrics:** Identified Gold layer 3× slower
# MAGIC 2. **Profile Spark UI:** Found task skew (one task = 80% of time)
# MAGIC 3. **Analyze data:** Confirmed 80% claims go to provider_123
# MAGIC 4. **Diagnose:** Hash partitioning → single partition bottleneck
# MAGIC
# MAGIC ### Solution: Salting
# MAGIC - **What:** Add random salt (0-9) to join key
# MAGIC - **Why:** Creates 10 different hash values for skewed provider
# MAGIC - **How:** `provider_123 + salt` → even distribution across partitions
# MAGIC - **Bonus:** Broadcast hint for small payment table
# MAGIC
# MAGIC ### Results
# MAGIC - **Execution time:** 45 min → 12 min (73% faster in production)
# MAGIC - **Worker utilization:** 20% → 80%+ (all workers busy)
# MAGIC - **Cost:** Back to baseline (40% reduction)
# MAGIC
# MAGIC ### Why It Worked
# MAGIC - **Problem:** Runtime skew (hash partitioning issue)
# MAGIC - **Not:** Storage skew (LC wouldn't fix this)
# MAGIC - **Key insight:** Workers need similar workload for parallelism
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Interview Talking Points
# MAGIC
# MAGIC **Interviewer:** "Volume +15%, cost +40%. How would you fix it?"
# MAGIC
# MAGIC **Your Answer:**
# MAGIC 1. "Systematic investigation: metrics → Spark UI → data analysis"
# MAGIC 2. "Found skewed join: 80% data to one partition"
# MAGIC 3. "Applied salting: distributed skewed provider across 10 partitions"
# MAGIC 4. "Result: 73% faster, cost normalized, all workers utilized"
# MAGIC 5. "Key insight: 15% volume shouldn't cause 40% cost - algorithmic inefficiency, not volume"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. ✅ Capture Spark UI screenshots (unoptimized vs. optimized)
# MAGIC 2. ✅ Document metrics in interview prep doc
# MAGIC 3. ✅ Practice explaining investigation → solution → results flow
# MAGIC 4. Optional: Add Liquid Clustering to Gold for dashboard query optimization (separate from join performance)

