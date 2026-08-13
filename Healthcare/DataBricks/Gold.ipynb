# Databricks notebook source
# MAGIC %run /Workspace/Users/nk1956663@gmail.com/Retail_Project/Functions

# COMMAND ----------

connect()

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from datetime import datetime

# ==============================================================================
# CONFIG
# ==============================================================================

SILVER_BASE_PATH = "/Volumes/proj_databricks/healthcare/silver"
GOLD_BASE_PATH = "/Volumes/proj_databricks/healthcare/gold"

print("=" * 90)
print("🚀 GOLD LAYER STARTED")
print("=" * 90)

print(f"Silver Path : {SILVER_BASE_PATH}")
print(f"Gold Path   : {GOLD_BASE_PATH}")


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def delta_exists(path):
    return DeltaTable.isDeltaTable(spark, path)


def validate_null_fk(df, column_name, table_name):
    """
    Validate foreign-key completeness.
    """
    null_count = (
        df.filter(F.col(column_name).isNull())
        .limit(1)
        .count()
    )

    if null_count > 0:
        print(
            f"⚠️ [DQ WARNING] {table_name}: "
            f"NULL values found in FK '{column_name}'"
        )
    else:
        print(
            f"✅ [DQ] {table_name}: "
            f"FK '{column_name}' passed"
        )


# ==============================================================================
# 1. DIM DIAGNOSTICS
# ==============================================================================

def process_dim_diagnostics():

    print("\n" + "-" * 80)
    print("[1] DIM_DIAGNOSTICS")
    print("-" * 80)

    source = (
        spark.read
        .format("delta")
        .load(f"{SILVER_BASE_PATH}/diagnostics")
    )

    staging = (
        source
        .select(
            "icd_code",
            "diagnosis_category",
            "disease_sub_category",
            "icd_description",
            "specialty_department",
            "disease_type",
            "severity_level",
            "benchmark_treatment_days",
            "estimated_treatment_cost",
            "is_active"
        )
        .filter(F.col("icd_code").isNotNull())
        .dropDuplicates(["icd_code"])
        .withColumn(
            "diagnostics_key",
            F.sha2(F.col("icd_code"), 256)
        )
        .withColumn(
            "_gold_updated_at",
            F.current_timestamp()
        )
    )

    target_path = f"{GOLD_BASE_PATH}/dim_diagnostics"

    if not delta_exists(target_path):

        (
            staging.write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
        )

        print("✅ Initial diagnostics dimension created")

    else:

        target = DeltaTable.forPath(spark, target_path)

        (
            target.alias("t")
            .merge(
                staging.alias("s"),
                "t.icd_code = s.icd_code"
            )
            .whenMatchedUpdate(
                set={
                    "diagnosis_category": "s.diagnosis_category",
                    "disease_sub_category": "s.disease_sub_category",
                    "icd_description": "s.icd_description",
                    "specialty_department": "s.specialty_department",
                    "disease_type": "s.disease_type",
                    "severity_level": "s.severity_level",
                    "benchmark_treatment_days": "s.benchmark_treatment_days",
                    "estimated_treatment_cost": "s.estimated_treatment_cost",
                    "is_active": "s.is_active",
                    "_gold_updated_at": "s._gold_updated_at"
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("🔄 Diagnostics MERGE completed")


# ==============================================================================
# 2. DIM PATIENT - SCD TYPE 2
# ==============================================================================

def process_dim_patient_scd2():

    print("\n" + "-" * 80)
    print("[2] DIM_PATIENT - SCD TYPE 2")
    print("-" * 80)

    source = (
        spark.read
        .format("delta")
        .load(f"{SILVER_BASE_PATH}/patients")
    )

    staging = (
        source
        .select(
            "patient_id",
            "first_name_masked",
            "last_name_masked",
            "email_masked",
            "phone_masked",
            "ssn_masked",
            "gender",
            "dob",
            "age",
            "blood_group",
            "city",
            "state",
            "zip_code",
            "insurance_policy_number",
            "insurance_provider",
            "policy_type",
            "created_at",
            "updated_at"
        )
        .filter(F.col("patient_id").isNotNull())
        .dropDuplicates(["patient_id"])
    )

    # --------------------------------------------------------------------------
    # Columns tracked for SCD Type 2
    # --------------------------------------------------------------------------

    tracked_columns = [
        "first_name_masked",
        "last_name_masked",
        "email_masked",
        "phone_masked",
        "ssn_masked",
        "gender",
        "dob",
        "age",
        "blood_group",
        "city",
        "state",
        "zip_code",
        "insurance_policy_number",
        "insurance_provider",
        "policy_type"
    ]

    staging = staging.withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(
                        F.col(c).cast("string"),
                        F.lit("")
                    )
                    for c in tracked_columns
                ]
            ),
            256
        )
    )

    target_path = f"{GOLD_BASE_PATH}/dim_patient"

    # --------------------------------------------------------------------------
    # INITIAL LOAD
    # --------------------------------------------------------------------------

    if not delta_exists(target_path):

        initial = (
            staging
            .withColumn(
                "patient_key",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("patient_id"),
                        F.coalesce(
                            F.col("updated_at").cast("string"),
                            F.lit("initial")
                        )
                    ),
                    256
                )
            )
            .withColumn(
                "effective_date",
                F.current_date()
            )
            .withColumn(
                "end_date",
                F.lit(None).cast("date")
            )
            .withColumn(
                "is_current",
                F.lit(True)
            )
            .withColumn(
                "_gold_updated_at",
                F.current_timestamp()
            )
        )

        (
            initial.write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
        )

        print("✅ Initial Patient SCD2 load completed")

        return

    # --------------------------------------------------------------------------
    # EXISTING TARGET
    # --------------------------------------------------------------------------

    target = DeltaTable.forPath(spark, target_path)

    current_target = (
        target.toDF()
        .filter(F.col("is_current") == True)
        .select(
            "patient_id",
            "record_hash"
        )
    )

    # --------------------------------------------------------------------------
    # DETECT NEW / CHANGED
    # --------------------------------------------------------------------------

    changes = (
        staging.alias("s")
        .join(
            current_target.alias("t"),
            "patient_id",
            "left"
        )
        .filter(
            F.col("t.patient_id").isNull()
            |
            (
                F.col("s.record_hash")
                !=
                F.col("t.record_hash")
            )
        )
        .select("s.*")
    )

    # --------------------------------------------------------------------------
    # EXPIRE CURRENT RECORDS
    # --------------------------------------------------------------------------

    changed_ids = (
        changes
        .select("patient_id")
        .distinct()
    )

    (
        target.alias("t")
        .merge(
            changed_ids.alias("s"),
            """
            t.patient_id = s.patient_id
            AND t.is_current = true
            """
        )
        .whenMatchedUpdate(
            set={
                "is_current": "false",
                "end_date": "current_date()",
                "_gold_updated_at": "current_timestamp()"
            }
        )
        .execute()
    )

    # --------------------------------------------------------------------------
    # INSERT NEW VERSION
    # --------------------------------------------------------------------------

    new_versions = (
        changes
        .withColumn(
            "patient_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("patient_id"),
                    F.coalesce(
                        F.col("updated_at").cast("string"),
                        F.current_timestamp().cast("string")
                    )
                ),
                256
            )
        )
        .withColumn(
            "effective_date",
            F.current_date()
        )
        .withColumn(
            "end_date",
            F.lit(None).cast("date")
        )
        .withColumn(
            "is_current",
            F.lit(True)
        )
        .withColumn(
            "_gold_updated_at",
            F.current_timestamp()
        )
    )

    (
        new_versions.write
        .format("delta")
        .mode("append")
        .save(target_path)
    )

    print("🔄 Patient SCD2 changes processed")


# ==============================================================================
# 3. DIM HOSPITAL
# ==============================================================================

def process_dim_hospital():

    print("\n" + "-" * 80)
    print("[3] DIM_HOSPITAL")
    print("-" * 80)

    source = (
        spark.read
        .format("delta")
        .load(f"{SILVER_BASE_PATH}/admission")
    )

    staging = (
        source
        .select(
            "hospital_name",
            "department"
        )
        .filter(
            F.col("hospital_name").isNotNull()
        )
        .dropDuplicates()
        .withColumn(
            "hospital_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("hospital_name"),
                    F.col("department")
                ),
                256
            )
        )
        .withColumn(
            "_gold_updated_at",
            F.current_timestamp()
        )
    )

    target_path = f"{GOLD_BASE_PATH}/dim_hospital"

    if not delta_exists(target_path):

        (
            staging.write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
        )

    else:

        target = DeltaTable.forPath(spark, target_path)

        (
            target.alias("t")
            .merge(
                staging.alias("s"),
                """
                t.hospital_name = s.hospital_name
                AND t.department = s.department
                """
            )
            .whenMatchedUpdate(
                set={
                    "_gold_updated_at": "s._gold_updated_at"
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    print("✅ dim_hospital completed")


# ==============================================================================
# 4. DIM DOCTOR
# ==============================================================================

def process_dim_doctor():

    print("\n" + "-" * 80)
    print("[4] DIM_DOCTOR")
    print("-" * 80)

    source = (
        spark.read
        .format("delta")
        .load(f"{SILVER_BASE_PATH}/admission")
    )

    staging = (
        source
        .select("doctor_id")
        .filter(F.col("doctor_id").isNotNull())
        .dropDuplicates()
        .withColumn(
            "doctor_key",
            F.sha2(
                F.col("doctor_id"),
                256
            )
        )
        .withColumn(
            "_gold_updated_at",
            F.current_timestamp()
        )
    )

    target_path = f"{GOLD_BASE_PATH}/dim_doctor"

    if not delta_exists(target_path):

        (
            staging.write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
        )

    else:

        target = DeltaTable.forPath(spark, target_path)

        (
            target.alias("t")
            .merge(
                staging.alias("s"),
                "t.doctor_id = s.doctor_id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    print("✅ dim_doctor completed")


# ==============================================================================
# 5. DIM INSURANCE
# ==============================================================================

def process_dim_insurance():

    print("\n" + "-" * 80)
    print("[5] DIM_INSURANCE")
    print("-" * 80)

    # --------------------------------------------------------------------------
    # Patient insurance master
    # --------------------------------------------------------------------------

    patients = (
        spark.read
        .format("delta")
        .load(f"{SILVER_BASE_PATH}/patients")
        .select(
            F.col("insurance_policy_number")
             .alias("policy_number"),
            "insurance_provider",
            "policy_type"
        )
        .filter(
            F.col("policy_number").isNotNull()
        )
        .dropDuplicates(["policy_number"])
    )

    # --------------------------------------------------------------------------
    # Claims insurance information
    # --------------------------------------------------------------------------

    claims = (
        spark.read
        .format("delta")
        .load(f"{SILVER_BASE_PATH}/claims")
        .select(
            "policy_number",
            "insurer_id",
            "insurer_name"
        )
        .filter(
            F.col("policy_number").isNotNull()
        )
        .dropDuplicates(["policy_number"])
    )

    # --------------------------------------------------------------------------
    # Combine without creating claim × patient duplication
    # --------------------------------------------------------------------------

    staging = (
        patients
        .join(
            claims,
            "policy_number",
            "full"
        )
        .filter(
            F.col("policy_number").isNotNull()
        )
        .dropDuplicates(["policy_number"])
        .withColumn(
            "insurance_key",
            F.sha2(
                F.col("policy_number"),
                256
            )
        )
        .withColumn(
            "_gold_updated_at",
            F.current_timestamp()
        )
    )

    target_path = f"{GOLD_BASE_PATH}/dim_insurance"

    if not delta_exists(target_path):

        (
            staging.write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
        )

    else:

        target = DeltaTable.forPath(spark, target_path)

        (
            target.alias("t")
            .merge(
                staging.alias("s"),
                "t.policy_number = s.policy_number"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    print("✅ dim_insurance completed")


# ==============================================================================
# 6. DIM DATE
# ==============================================================================

def process_dim_date():

    print("\n" + "-" * 80)
    print("[6] DIM_DATE")
    print("-" * 80)

    target_path = f"{GOLD_BASE_PATH}/dim_date"

    # Static calendar
    start_date = "2015-01-01"
    end_date = "2035-12-31"

    # --------------------------------------------------------------------------
    # Do not rebuild if already present
    # --------------------------------------------------------------------------

    if delta_exists(target_path):

        print("⏭️ dim_date already exists - skipping static calendar")
        return

    calendar = (
        spark.sql(
            f"""
            SELECT explode(
                sequence(
                    to_date('{start_date}'),
                    to_date('{end_date}'),
                    interval 1 day
                )
            ) AS full_date
            """
        )
    )

    dim_date = (
        calendar
        .withColumn(
            "date_key",
            F.date_format(
                "full_date",
                "yyyyMMdd"
            ).cast("int")
        )
        .withColumn(
            "day",
            F.dayofmonth("full_date")
        )
        .withColumn(
            "month",
            F.month("full_date")
        )
        .withColumn(
            "month_name",
            F.date_format(
                "full_date",
                "MMMM"
            )
        )
        .withColumn(
            "quarter",
            F.concat(
                F.lit("Q"),
                F.quarter("full_date")
            )
        )
        .withColumn(
            "year",
            F.year("full_date")
        )
        .withColumn(
            "week",
            F.weekofyear("full_date")
        )
        .withColumn(
            "day_name",
            F.date_format(
                "full_date",
                "EEEE"
            )
        )
        .withColumn(
            "is_weekend",
            F.dayofweek("full_date").isin([1, 7])
        )
        .select(
            "date_key",
            "full_date",
            "day",
            "month",
            "month_name",
            "quarter",
            "year",
            "week",
            "day_name",
            "is_weekend"
        )
    )

    (
        dim_date.write
        .format("delta")
        .mode("overwrite")
        .save(target_path)
    )

    print("✅ Static dim_date created")


# ==============================================================================
# 7. FACT ADMISSIONS
# ==============================================================================

def process_fact_admissions():

    print("\n" + "-" * 80)
    print("[7] FACT_ADMISSIONS")
    print("-" * 80)

    admissions = (
        spark.read
        .format("delta")
        .load(f"{SILVER_BASE_PATH}/admission")
    )

    patient = (
        spark.read
        .format("delta")
        .load(f"{GOLD_BASE_PATH}/dim_patient")
        .filter(F.col("is_current") == True)
        .select(
            "patient_id",
            "patient_key"
        )
    )

    diagnostics = (
        spark.read
        .format("delta")
        .load(f"{GOLD_BASE_PATH}/dim_diagnostics")
        .select(
            "icd_code",
            "diagnostics_key"
        )
    )

    hospital = (
        spark.read
        .format("delta")
        .load(f"{GOLD_BASE_PATH}/dim_hospital")
        .select(
            "hospital_name",
            "department",
            "hospital_key"
        )
    )

    doctor = (
        spark.read
        .format("delta")
        .load(f"{GOLD_BASE_PATH}/dim_doctor")
        .select(
            "doctor_id",
            "doctor_key"
        )
    )

    date_dim = (
        spark.read
        .format("delta")
        .load(f"{GOLD_BASE_PATH}/dim_date")
        .select(
            "date_key",
            F.col("full_date").alias("lookup_date")
        )
    )

    fact = (
        admissions.alias("a")

        .join(
            patient.alias("p"),
            F.col("a.patient_id") ==
            F.col("p.patient_id"),
            "left"
        )

        .join(
            diagnostics.alias("d"),
            F.col("a.diagnosis_code") ==
            F.col("d.icd_code"),
            "left"
        )

        .join(
            hospital.alias("h"),
            (
                (F.col("a.hospital_name") ==
                 F.col("h.hospital_name"))
                &
                (F.col("a.department") ==
                 F.col("h.department"))
            ),
            "left"
        )

        .join(
            doctor.alias("dr"),
            F.col("a.doctor_id") ==
            F.col("dr.doctor_id"),
            "left"
        )

        .join(
            date_dim.alias("ad"),
            F.to_date(
                F.col("a.admission_date")
            ) ==
            F.col("ad.lookup_date"),
            "left"
        )

        .join(
            date_dim.alias("dd"),
            F.to_date(
                F.col("a.discharge_date")
            ) ==
            F.col("dd.lookup_date"),
            "left"
        )

        .select(
            F.sha2(
                F.col("a.admission_id"),
                256
            ).alias("admission_key"),

            F.col("a.admission_id"),

            F.col("p.patient_key"),
            F.col("d.diagnostics_key"),
            F.col("h.hospital_key"),
            F.col("dr.doctor_key"),

            F.col("ad.date_key")
             .alias("admission_date_key"),

            F.col("dd.date_key")
             .alias("discharge_date_key"),

            F.col("a.admission_type"),
            F.col("a.discharge_status"),
            F.col("a.length_of_stay_days"),
            F.col("a.total_cost"),
            F.col("a.is_high_cost_admission"),

            F.year(
                F.to_date(
                    F.col("a.admission_date")
                )
            ).alias("admission_year"),

            F.col("a._silver_processed_at")
             .alias("_fact_updated_at")
        )
        .dropDuplicates(["admission_id"])
    )

    # --------------------------------------------------------------------------
    # FK Validation
    # --------------------------------------------------------------------------

    validate_null_fk(
        fact,
        "patient_key",
        "fact_admissions"
    )

    validate_null_fk(
        fact,
        "diagnostics_key",
        "fact_admissions"
    )

    validate_null_fk(
        fact,
        "hospital_key",
        "fact_admissions"
    )

    validate_null_fk(
        fact,
        "doctor_key",
        "fact_admissions"
    )

    target_path = f"{GOLD_BASE_PATH}/fact_admissions"

    if not delta_exists(target_path):

        (
            fact.write
            .format("delta")
            .partitionBy("admission_year")
            .mode("overwrite")
            .save(target_path)
        )

        print("✅ Initial fact_admissions load completed")

    else:

        target = DeltaTable.forPath(
            spark,
            target_path
        )

        (
            target.alias("t")
            .merge(
                fact.alias("s"),
                "t.admission_id = s.admission_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("🔄 fact_admissions MERGE completed")


# ==============================================================================
# 8. FACT CLAIMS
# ==============================================================================

def process_fact_claims():

    print("\n" + "-" * 80)
    print("[8] FACT_CLAIMS")
    print("-" * 80)

    claims = (
        spark.read
        .format("delta")
        .load(f"{SILVER_BASE_PATH}/claims")
    )

    admissions = (
        spark.read
        .format("delta")
        .load(f"{GOLD_BASE_PATH}/fact_admissions")
        .select(
            "admission_id",
            "admission_key",
            "patient_key"
        )
    )

    insurance = (
        spark.read
        .format("delta")
        .load(f"{GOLD_BASE_PATH}/dim_insurance")
        .select(
            "policy_number",
            "insurance_key"
        )
    )

    date_dim = (
        spark.read
        .format("delta")
        .load(f"{GOLD_BASE_PATH}/dim_date")
        .select(
            "date_key",
            F.col("full_date").alias("lookup_date")
        )
    )

    fact = (
        claims.alias("c")

        .join(
            admissions.alias("a"),
            F.col("c.admission_id") ==
            F.col("a.admission_id"),
            "left"
        )

        .join(
            insurance.alias("i"),
            F.col("c.policy_number") ==
            F.col("i.policy_number"),
            "left"
        )

        .join(
            date_dim.alias("cd"),
            F.to_date(
                F.col("c.claim_date")
            ) ==
            F.col("cd.lookup_date"),
            "left"
        )

        .join(
            date_dim.alias("sd"),
            F.to_date(
                F.col("c.settlement_date")
            ) ==
            F.col("sd.lookup_date"),
            "left"
        )

        .select(

            F.sha2(
                F.col("c.claim_id"),
                256
            ).alias("claim_key"),

            F.col("c.claim_id"),

            F.col("a.admission_key"),
            F.col("a.patient_key"),
            F.col("i.insurance_key"),

            F.col("cd.date_key")
             .alias("claim_date_key"),

            F.col("sd.date_key")
             .alias("settlement_date_key"),

            F.col("c.claim_type"),
            F.col("c.claim_category"),
            F.col("c.claim_status"),

            F.col("c.claim_amount"),
            F.col("c.approved_amount"),
            F.col("c.paid_amount"),

            # More meaningful business metric
            (
                F.coalesce(
                    F.col("c.claim_amount"),
                    F.lit(0.0)
                )
                -
                F.coalesce(
                    F.col("c.paid_amount"),
                    F.lit(0.0)
                )
            ).alias("pending_amount"),

            F.year(
                F.to_date(
                    F.col("c.claim_date")
                )
            ).alias("claim_year"),

            F.col("c._silver_processed_at")
             .alias("_fact_updated_at")
        )
        .dropDuplicates(["claim_id"])
    )

    # --------------------------------------------------------------------------
    # FK Validation
    # --------------------------------------------------------------------------

    validate_null_fk(
        fact,
        "admission_key",
        "fact_claims"
    )

    validate_null_fk(
        fact,
        "insurance_key",
        "fact_claims"
    )

    target_path = f"{GOLD_BASE_PATH}/fact_claims"

    if not delta_exists(target_path):

        (
            fact.write
            .format("delta")
            .partitionBy("claim_year")
            .mode("overwrite")
            .save(target_path)
        )

        print("✅ Initial fact_claims load completed")

    else:

        target = DeltaTable.forPath(
            spark,
            target_path
        )

        (
            target.alias("t")
            .merge(
                fact.alias("s"),
                "t.claim_id = s.claim_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("🔄 fact_claims MERGE completed")


# ==============================================================================
# 9. OPTIMIZE + ZORDER
# ==============================================================================

def optimize_gold():

    print("\n" + "-" * 80)
    print("⚡ OPTIMIZE + ZORDER")
    print("-" * 80)

    tables = {
        "dim_patient": "patient_id",
        "dim_diagnostics": "icd_code",
        "dim_hospital": "hospital_name",
        "dim_doctor": "doctor_id",
        "dim_insurance": "policy_number",

        # Small static dimension - optimization is optional
        "dim_date": "date_key",

        "fact_admissions":
            "patient_key, admission_date_key",

        "fact_claims":
            "admission_key, claim_date_key"
    }

    for table_name, zorder_columns in tables.items():

        path = f"{GOLD_BASE_PATH}/{table_name}"

        if delta_exists(path):

            print(
                f"🏎️ OPTIMIZE {table_name}"
            )

            spark.sql(
                f"""
                OPTIMIZE delta.`{path}`
                ZORDER BY ({zorder_columns})
                """
            )

    print("✅ OPTIMIZE + ZORDER completed")


# ==============================================================================
# 10. VACUUM
# ==============================================================================

def vacuum_gold(retention_hours=168):

    print(
        f"\n🧹 VACUUM GOLD "
        f"(Retention: {retention_hours} hours)"
    )

    tables = [
        "dim_patient",
        "dim_diagnostics",
        "dim_hospital",
        "dim_doctor",
        "dim_insurance",
        "dim_date",
        "fact_admissions",
        "fact_claims"
    ]

    for table_name in tables:

        path = f"{GOLD_BASE_PATH}/{table_name}"

        if delta_exists(path):

            print(
                f"🧹 VACUUM {table_name}"
            )

            spark.sql(
                f"""
                VACUUM delta.`{path}`
                RETAIN {retention_hours} HOURS
                """
            )

    print("✅ VACUUM completed")


# ==============================================================================
# 11. GOLD PIPELINE EXECUTION
# ==============================================================================

def run_gold_pipeline():

    start_time = datetime.now()

    try:

        print("\n")
        print("=" * 90)
        print("🚀 HEALTHCARE GOLD PIPELINE")
        print("=" * 90)

        # Dimensions
        process_dim_diagnostics()
        process_dim_patient_scd2()
        process_dim_hospital()
        process_dim_doctor()
        process_dim_insurance()
        process_dim_date()

        # Facts
        process_fact_admissions()
        process_fact_claims()

        # Optimization
        optimize_gold()

        # Maintenance
        vacuum_gold(168)

        end_time = datetime.now()

        duration = (
            end_time - start_time
        ).total_seconds()

        print("\n" + "=" * 90)
        print("🎉 GOLD PIPELINE COMPLETED SUCCESSFULLY")
        print(f"⏱️ Duration: {duration:.2f} seconds")
        print("=" * 90)

    except Exception as e:

        print("\n" + "=" * 90)
        print("❌ GOLD PIPELINE FAILED")
        print("=" * 90)

        print(
            f"Error Type : {type(e).__name__}"
        )

        print(
            f"Error      : {str(e)}"
        )

        raise


# ==============================================================================
# RUN
# ==============================================================================

run_gold_pipeline()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG proj_databricks;
# MAGIC USE SCHEMA healthcare;
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apne catalog aur schema (database) ka context set karein
# MAGIC USE CATALOG proj_databricks;
# MAGIC USE SCHEMA healthcare;
# MAGIC
# MAGIC -- Har folder location ke upar VIEW create karein taaki Power BI read kar sake
# MAGIC CREATE OR REPLACE VIEW dim_date AS 
# MAGIC SELECT * FROM delta.`/Volumes/proj_databricks/healthcare/gold/dim_date`;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW dim_diagnostics AS 
# MAGIC SELECT * FROM delta.`/Volumes/proj_databricks/healthcare/gold/dim_diagnostics`;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW dim_doctor AS 
# MAGIC SELECT * FROM delta.`/Volumes/proj_databricks/healthcare/gold/dim_doctor`;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW dim_hospital AS 
# MAGIC SELECT * FROM delta.`/Volumes/proj_databricks/healthcare/gold/dim_hospital`;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW dim_insurance AS 
# MAGIC SELECT * FROM delta.`/Volumes/proj_databricks/healthcare/gold/dim_insurance`;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW dim_patient AS 
# MAGIC SELECT * FROM delta.`/Volumes/proj_databricks/healthcare/gold/dim_patient`;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW fact_admissions AS 
# MAGIC SELECT * FROM delta.`/Volumes/proj_databricks/healthcare/gold/fact_admissions`;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW fact_claims AS 
# MAGIC SELECT * FROM delta.`/Volumes/proj_databricks/healthcare/gold/fact_claims`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_rows, 
# MAGIC     COUNT(DISTINCT admission_id) as unique_admissions,
# MAGIC     (COUNT(*) - COUNT(DISTINCT admission_id)) as duplicate_count
# MAGIC FROM proj_databricks.healthcare.fact_admissions;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_rows, 
# MAGIC     COUNT(DISTINCT claim_id) as unique_claims,
# MAGIC     (COUNT(*) - COUNT(DISTINCT claim_id)) as duplicate_count
# MAGIC FROM proj_databricks.healthcare.fact_claims;