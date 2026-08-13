# Databricks notebook source
# MAGIC %run /Workspace/Users/nk1956663@gmail.com/Retail_Project/Functions

# COMMAND ----------

connect()

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    to_date,
    current_timestamp,
    sha2,
    datediff,
    current_date,
    floor,
    when,
    coalesce,
    lit
)


# ============================================================
# BASE PATHS
# ============================================================

BRONZE_BASE_PATH = "/Volumes/proj_databricks/healthcare/bronze"
SILVER_BASE_PATH = "/Volumes/proj_databricks/healthcare/silver"


# ============================================================
# 1. PATIENTS SILVER
# PII MASKING + STANDARDIZATION + AGE LOGIC
# ============================================================

def process_silver_patients():

    print(
        "🚀 [SILVER] Processing Patients "
        "(PII Masking & Standardization)..."
    )

    bronze_patients = (
        spark.read
        .format("delta")
        .load(f"{BRONZE_BASE_PATH}/patients")
    )


    silver_patients = (
        bronze_patients

        # ----------------------------------------------------
        # Patient ID Cleaning
        # ----------------------------------------------------

        .withColumn(
            "patient_id",
            trim(col("patient_id"))
        )


        # ----------------------------------------------------
        # SHA-256 PII Masking
        # ----------------------------------------------------

        .withColumn(
            "first_name_masked",
            when(
                col("first_name").isNotNull(),
                sha2(
                    trim(col("first_name")),
                    256
                )
            ).otherwise(None)
        )

        .withColumn(
            "last_name_masked",
            when(
                col("last_name").isNotNull(),
                sha2(
                    trim(col("last_name")),
                    256
                )
            ).otherwise(None)
        )

        .withColumn(
            "email_masked",
            when(
                col("email").isNotNull(),
                sha2(
                    trim(lower(col("email"))),
                    256
                )
            ).otherwise(None)
        )

        .withColumn(
            "phone_masked",
            when(
                col("phone_number").isNotNull(),
                sha2(
                    trim(col("phone_number")),
                    256
                )
            ).otherwise(None)
        )

        .withColumn(
            "ssn_masked",
            when(
                col("ssn_national_id").isNotNull(),
                sha2(
                    trim(col("ssn_national_id")),
                    256
                )
            ).otherwise(None)
        )


        # ----------------------------------------------------
        # Remove Original PII
        # ----------------------------------------------------

        .drop(
            "first_name",
            "last_name",
            "email",
            "phone_number",
            "ssn_national_id",
            "address"
        )


        # ----------------------------------------------------
        # Standardization
        # ----------------------------------------------------

        .withColumn(
            "dob",
            to_date(col("dob"))
        )

        .withColumn(
            "gender",
            when(
                upper(trim(col("gender"))).isin("MALE", "M"),
                "M"
            )
            .when(
                upper(trim(col("gender"))).isin("FEMALE", "F"),
                "F"
            )
            .otherwise("U")
        )


        # ----------------------------------------------------
        # Business Transformation
        # Calculate Age
        # ----------------------------------------------------

        .withColumn(
            "age",
            floor(
                datediff(
                    current_date(),
                    col("dob")
                ) / 365.25
            )
        )


        # ----------------------------------------------------
        # Data Cleansing
        # ----------------------------------------------------

        .filter(
            col("patient_id").isNotNull()
        )

        .dropDuplicates(
            ["patient_id"]
        )


        # ----------------------------------------------------
        # Silver Audit Metadata
        # ----------------------------------------------------

        .withColumn(
            "_silver_processed_at",
            current_timestamp()
        )
    )


    (
        silver_patients.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(
            f"{SILVER_BASE_PATH}/patients"
        )
    )


    print(
        "✅ [SILVER] Patients processed "
        "with PII masking!\n"
    )


# ============================================================
# 2. ADMISSION SILVER
# CLEANSING + BUSINESS LOGIC
# ============================================================

def process_silver_admission():

    print(
        "🚀 [SILVER] Processing Admission "
        "(Business Logic & Standardization)..."
    )

    bronze_admission = (
        spark.read
        .format("delta")
        .load(f"{BRONZE_BASE_PATH}/admission")
    )


    silver_admission = (
        bronze_admission

        # ----------------------------------------------------
        # Cleaning
        # ----------------------------------------------------

        .withColumn(
            "admission_id",
            trim(col("admission_id"))
        )

        .withColumn(
            "patient_id",
            trim(col("patient_id"))
        )

        .withColumn(
            "doctor_id",
            trim(col("doctor_id"))
        )

        .withColumn(
            "hospital_name",
            trim(col("hospital_name"))
        )

        .withColumn(
            "department",
            trim(col("department"))
        )

        .withColumn(
            "total_cost",
            coalesce(
                col("total_cost").cast("double"),
                lit(0.0)
            )
        )


        # ----------------------------------------------------
        # Date Standardization
        # ----------------------------------------------------

        .withColumn(
            "admission_date",
            to_date(col("admission_date"))
        )

        .withColumn(
            "discharge_date",
            to_date(col("discharge_date"))
        )


        # ----------------------------------------------------
        # Text Standardization
        # ----------------------------------------------------

        .withColumn(
            "admission_type",
            upper(trim(col("admission_type")))
        )

        .withColumn(
            "discharge_status",
            upper(trim(col("discharge_status")))
        )

        .withColumn(
            "payment_type",
            upper(trim(col("payment_type")))
        )


        # ----------------------------------------------------
        # Business Transformation
        # Length of Stay
        # ----------------------------------------------------

        .withColumn(
            "length_of_stay_days",
            when(
                col("discharge_date").isNotNull()
                & col("admission_date").isNotNull(),
                datediff(
                    col("discharge_date"),
                    col("admission_date")
                )
            ).otherwise(0)
        )


        # ----------------------------------------------------
        # Business Transformation
        # High Cost Admission
        # ----------------------------------------------------

        .withColumn(
            "is_high_cost_admission",
            when(
                col("total_cost") > 10000,
                True
            ).otherwise(False)
        )


        # ----------------------------------------------------
        # Data Quality
        # ----------------------------------------------------

        .filter(
            col("admission_id").isNotNull()
        )

        .dropDuplicates(
            ["admission_id"]
        )


        # ----------------------------------------------------
        # Silver Audit Timestamp
        # ----------------------------------------------------

        .withColumn(
            "_silver_processed_at",
            current_timestamp()
        )
    )


    (
        silver_admission.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(
            f"{SILVER_BASE_PATH}/admission"
        )
    )


    print(
        "✅ [SILVER] Admission processed "
        "with Length of Stay metrics!\n"
    )


# ============================================================
# 3. CLAIMS SILVER
# CORRECTED FOR NEW BRONZE SCHEMA
# ============================================================

def process_silver_claims():

    print(
        "🚀 [SILVER] Processing Claims "
        "(Cleansing & Business Categories)..."
    )


    bronze_claims = (
        spark.read
        .format("delta")
        .load(f"{BRONZE_BASE_PATH}/claims")
    )


    # --------------------------------------------------------
    # Expected Bronze Claims Schema
    #
    # claim_id
    # admission_id
    # policy_number
    # insurer_id
    # insurer_name
    # claim_type
    # claim_date
    # settlement_date
    # claim_amount
    # approved_amount
    # paid_amount
    # claim_status
    # remarks
    # _ingested_at
    # _source_file
    # --------------------------------------------------------


    silver_claims = (

        bronze_claims


        # ----------------------------------------------------
        # ID / Text Cleansing
        # ----------------------------------------------------

        .withColumn(
            "claim_id",
            trim(col("claim_id"))
        )

        .withColumn(
            "admission_id",
            trim(col("admission_id"))
        )

        .withColumn(
            "policy_number",
            trim(col("policy_number"))
        )

        .withColumn(
            "insurer_id",
            trim(col("insurer_id"))
        )

        .withColumn(
            "insurer_name",
            trim(col("insurer_name"))
        )

        .withColumn(
            "claim_type",
            upper(trim(col("claim_type")))
        )

        .withColumn(
            "claim_status",
            upper(trim(col("claim_status")))
        )

        .withColumn(
            "remarks",
            trim(col("remarks"))
        )


        # ----------------------------------------------------
        # Numeric Standardization
        # ----------------------------------------------------

        .withColumn(
            "claim_amount",
            coalesce(
                col("claim_amount").cast("double"),
                lit(0.0)
            )
        )

        .withColumn(
            "approved_amount",
            coalesce(
                col("approved_amount").cast("double"),
                lit(0.0)
            )
        )

        .withColumn(
            "paid_amount",
            coalesce(
                col("paid_amount").cast("double"),
                lit(0.0)
            )
        )


        # ----------------------------------------------------
        # Date Standardization
        # ----------------------------------------------------

        .withColumn(
            "claim_date",
            to_date(col("claim_date"))
        )

        .withColumn(
            "settlement_date",
            to_date(col("settlement_date"))
        )


        # ----------------------------------------------------
        # Business Transformation
        # Claim Category
        # ----------------------------------------------------

        .withColumn(
            "claim_category",

            when(
                col("claim_amount") >= 15000,
                "HIGH"
            )

            .when(
                col("claim_amount") >= 5000,
                "MEDIUM"
            )

            .otherwise(
                "LOW"
            )
        )


        # ----------------------------------------------------
        # Data Quality
        # ----------------------------------------------------

        .filter(
            col("claim_id").isNotNull()
        )

        .dropDuplicates(
            ["claim_id"]
        )


        # ----------------------------------------------------
        # Silver Audit Metadata
        # ----------------------------------------------------

        .withColumn(
            "_silver_processed_at",
            current_timestamp()
        )
    )


    # --------------------------------------------------------
    # Remove unwanted Auto Loader columns if present
    # --------------------------------------------------------

    cols_to_drop = [
        c
        for c in [
            "_c13",
            "_c14",
            "_c15",
            "_rescued_data"
        ]
        if c in silver_claims.columns
    ]


    if cols_to_drop:

        silver_claims = silver_claims.drop(
            *cols_to_drop
        )


    # --------------------------------------------------------
    # Write Silver Claims
    # --------------------------------------------------------

    (
        silver_claims.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(
            f"{SILVER_BASE_PATH}/claims"
        )
    )


    print(
        "✅ [SILVER] Claims processed successfully!\n"
    )


# ============================================================
# 4. DIAGNOSTICS SILVER
# CLEANSING + STANDARDIZATION
# ============================================================

def process_silver_diagnostics():

    print(
        "🚀 [SILVER] Processing Diagnostics "
        "Master Table..."
    )


    bronze_diag = (
        spark.read
        .format("delta")
        .load(f"{BRONZE_BASE_PATH}/diagnostics")
    )


    silver_diag = (

        bronze_diag


        # ----------------------------------------------------
        # Key Cleaning
        # ----------------------------------------------------

        .withColumn(
            "icd_code",
            upper(trim(col("icd_code")))
        )

        .withColumn(
            "diagnosis_category",
            trim(col("diagnosis_category"))
        )

        .withColumn(
            "disease_sub_category",
            trim(col("disease_sub_category"))
        )

        .withColumn(
            "specialty_department",
            trim(col("specialty_department"))
        )

        .withColumn(
            "icd_description",
            trim(col("icd_description"))
        )


        # ----------------------------------------------------
        # Numeric Standardization
        # ----------------------------------------------------

        .withColumn(
            "estimated_treatment_cost",
            coalesce(
                col("estimated_treatment_cost")
                .cast("double"),
                lit(0.0)
            )
        )

        .withColumn(
            "benchmark_treatment_days",
            coalesce(
                col("benchmark_treatment_days")
                .cast("int"),
                lit(0)
            )
        )


        # ----------------------------------------------------
        # Standardization
        # ----------------------------------------------------

        .withColumn(
            "severity_level",
            upper(trim(col("severity_level")))
        )

        .withColumn(
            "disease_type",
            trim(col("disease_type"))
        )


        # ----------------------------------------------------
        # Data Quality
        # ----------------------------------------------------

        .filter(
            col("icd_code").isNotNull()
        )

        .dropDuplicates(
            ["icd_code"]
        )


        # ----------------------------------------------------
        # Silver Audit Metadata
        # ----------------------------------------------------

        .withColumn(
            "_silver_processed_at",
            current_timestamp()
        )
    )


    # --------------------------------------------------------
    # Remove rescued data
    # --------------------------------------------------------

    if "_rescued_data" in silver_diag.columns:

        silver_diag = silver_diag.drop(
            "_rescued_data"
        )


    # --------------------------------------------------------
    # Write Silver Diagnostics
    # --------------------------------------------------------

    (
        silver_diag.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(
            f"{SILVER_BASE_PATH}/diagnostics"
        )
    )


    print(
        "✅ [SILVER] Diagnostics master "
        "processed successfully!\n"
    )


# ============================================================
# 5. EXECUTE SILVER PIPELINE
# ============================================================

print("=" * 75)
print("🚀 HEALTHCARE SILVER PIPELINE STARTED")
print("=" * 75)


try:

    process_silver_patients()

    process_silver_admission()

    process_silver_claims()

    process_silver_diagnostics()


    print("=" * 75)
    print(
        "🎉 ALL SILVER TRANSFORMATIONS COMPLETED "
        "SUCCESSFULLY!"
    )
    print(
        "   ✓ PII Masking"
    )
    print(
        "   ✓ Standardization"
    )
    print(
        "   ✓ Data Cleansing"
    )
    print(
        "   ✓ Deduplication"
    )
    print(
        "   ✓ Business Logic"
    )
    print(
        "   ✓ Claims Categorization"
    )
    print(
        "   ✓ Audit Metadata"
    )
    print("=" * 75)


except Exception as e:

    print(
        "❌ ERROR DURING SILVER PROCESSING:"
    )

    print(str(e))

    raise

# COMMAND ----------

# Reading admsiion file 

admsiion = "/Volumes/proj_databricks/healthcare/silver/admission/"


admsiion1 = spark.read \
    .format("delta") \
    .load(admsiion)

print(admsiion1.columns)

# COMMAND ----------

# Reading claim file 

claim = "/Volumes/proj_databricks/healthcare/silver/claims/"


claim1 = spark.read \
    .format("delta") \
    .load(claim)

print(claim1.columns)

# COMMAND ----------

# Reading diagnostics file 

diagnostics = "/Volumes/proj_databricks/healthcare/silver/diagnostics/"


diagnostics1 = spark.read \
    .format("delta") \
    .load(diagnostics)

print(diagnostics1.columns)

# COMMAND ----------

# Reading patient file 

patient = "/Volumes/proj_databricks/healthcare/silver/patients/"


patient1 = spark.read \
    .format("delta") \
    .load(patient)

print(patient1.columns)