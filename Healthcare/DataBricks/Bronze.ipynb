# Databricks notebook source
# MAGIC %run /Workspace/Users/nk1956663@gmail.com/Retail_Project/Functions

# COMMAND ----------

connect()

# COMMAND ----------

# CLAIMS  path 

path_claims="abfss://test@projstorageaccount.dfs.core.windows.net/HealthCare/Landing/CLAIMS_20260807_133316.csv"


# admission path 

path_admission="abfss://test@projstorageaccount.dfs.core.windows.net/HealthCare/Landing/admission_20260807_133218.csv"



# diagnostics  path 


path_diagnostics="abfss://test@projstorageaccount.dfs.core.windows.net/HealthCare/Landing/diagnostics.parquet"


# patients  path 

path_patients="abfss://test@projstorageaccount.dfs.core.windows.net/HealthCare/Landing/patients.json"




# COMMAND ----------

# Purane checkpoints aur schema paths clear karein
BRONZE_VOLUME_PATH = "/Volumes/proj_databricks/healthcare/bronze"

dbutils.fs.rm(f"{BRONZE_VOLUME_PATH}/_checkpoints", recurse=True)
dbutils.fs.rm(f"{BRONZE_VOLUME_PATH}/_schemas", recurse=True)
print("🧹 Checkpoints aur Schema locations successfully clear ho gaye hain!")

# COMMAND ----------

STORAGE_ACCOUNT_NAME = "projstorageaccount"
# 🔑 Azure Portal -> Storage Account -> Access Keys se copy karke yahan paste karein
STORAGE_ACCOUNT_KEY = "sw9eRmOe8VYSOBO1NTpHL9eGrnBm4XMYW0Sqpp56YzFtbxEw2NVemkLcRcFX+TwW9nSxyy65Diyx+ASt+3Gmmg=="

# Spark Session ko storage authentication ki details dein
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    STORAGE_ACCOUNT_KEY
)

# COMMAND ----------

import re
from pyspark.sql.functions import current_timestamp, input_file_name

# 🔑 1. Azure Authentication Setup
STORAGE_ACCOUNT_NAME = "projstorageaccount"

# ⚠️ Yahan real key paste karein (Double Quotes ke andar)
STORAGE_ACCOUNT_KEY = "sw9eRmOe8VYSOBO1NTpHL9eGrnBm4XMYW0Sqpp56YzFtbxEw2NVemkLcRcFX+TwW9nSxyy65Diyx+ASt+3Gmmg==".strip() 

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    STORAGE_ACCOUNT_KEY
)

# COMMAND ----------

test_path = "abfss://test@projstorageaccount.dfs.core.windows.net/HealthCare/Landing"

try:
    files = dbutils.fs.ls(test_path)
    print("✅ Connection Successful! Landing folder files:")
    for f in files:
        print(f.path)
except Exception as e:
    print("❌ Key/Connection Issue:", str(e))

# COMMAND ----------

RESET_CLAIMS_BRONZE = False

# COMMAND ----------

# ============================================================
# BRONZE LAYER - HEALTHCARE DYNAMIC INGESTION
# ============================================================

import re

from pyspark.sql.functions import (
    current_timestamp,
    input_file_name
)


# ============================================================
# 1. AZURE AUTHENTICATION SETUP
# ============================================================

STORAGE_ACCOUNT_NAME = "projstorageaccount"

# Paste your NEWLY ROTATED storage account key here
STORAGE_ACCOUNT_KEY = "sw9eRmOe8VYSOBO1NTpHL9eGrnBm4XMYW0Sqpp56YzFtbxEw2NVemkLcRcFX+TwW9nSxyy65Diyx+ASt+3Gmmg=="

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    STORAGE_ACCOUNT_KEY
)


# ============================================================
# 2. BASE PATHS
# ============================================================

BRONZE_VOLUME_PATH = "/Volumes/proj_databricks/healthcare/bronze"

LANDING_BASE_PATH = (
    f"abfss://test@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"
    "HealthCare/Landing"
)


# ============================================================
# 3. RESET CLAIMS BRONZE
# ============================================================

# TRUE only for this correction run.
# Change to FALSE after successful ingestion.

RESET_CLAIMS_BRONZE = True


if RESET_CLAIMS_BRONZE:

    print("⚠️ Resetting old CLAIMS Bronze state...")

    # Remove old incorrect Claims Delta table
    try:
        dbutils.fs.rm(
            f"{BRONZE_VOLUME_PATH}/claims",
            recurse=True
        )
    except Exception:
        pass

    # Remove Claims Auto Loader schema
    try:
        dbutils.fs.rm(
            f"{BRONZE_VOLUME_PATH}/_schemas/claims",
            recurse=True
        )
    except Exception:
        pass

    # Remove Claims checkpoint
    try:
        dbutils.fs.rm(
            f"{BRONZE_VOLUME_PATH}/_checkpoints/claims",
            recurse=True
        )
    except Exception:
        pass

    print("✅ Old Claims Bronze state removed.\n")


# ============================================================
# 4. ADMISSION COLUMN MAPPING
# ============================================================

admission_col_mapping = {

    "_c0": "admission_id",
    "_c1": "patient_id",
    "_c2": "doctor_id",
    "_c3": "hospital_name",
    "_c4": "department",
    "_c5": "admission_date",
    "_c6": "discharge_date",
    "_c7": "admission_type",
    "_c8": "discharge_status",
    "_c9": "diagnosis_code",
    "_c10": "room_type",
    "_c11": "room_number",
    "_c12": "nurse_id",
    "_c13": "payment_type",
    "_c14": "total_cost",
    "_c15": "created_date",
    "_c16": "modified_date"
}


# ============================================================
# 5. CLAIMS COLUMN MAPPING
# ============================================================

claims_col_mapping = {

    "_c0": "claim_id",
    "_c1": "admission_id",
    "_c2": "policy_number",
    "_c3": "insurer_id",
    "_c4": "insurer_name",
    "_c5": "claim_type",
    "_c6": "claim_date",
    "_c7": "settlement_date",
    "_c8": "claim_amount",
    "_c9": "approved_amount",
    "_c10": "paid_amount",
    "_c11": "claim_status",
    "_c12": "remarks"
}


# ============================================================
# 6. SOURCE CONFIGURATION
# ============================================================

sources_config = {

    # --------------------------------------------------------
    # CLAIMS
    # --------------------------------------------------------
    "claims": {

        "glob_pattern": "CLAIMS_*.csv",

        "format": "csv",

        # IMPORTANT:
        # Claims CSV is headerless
        "has_header": False,

        "mapping": claims_col_mapping
    },


    # --------------------------------------------------------
    # ADMISSION
    # --------------------------------------------------------
    "admission": {

        "glob_pattern": "admission_*.csv",

        "format": "csv",

        # Admission CSV is headerless
        "has_header": False,

        "mapping": admission_col_mapping
    },


    # --------------------------------------------------------
    # DIAGNOSTICS
    # --------------------------------------------------------
    "diagnostics": {

        "glob_pattern": "diagnostics*.parquet",

        "format": "parquet",

        "has_header": None,

        "mapping": None
    },


    # --------------------------------------------------------
    # PATIENTS
    # --------------------------------------------------------
    "patients": {

        "glob_pattern": "patients*.json",

        "format": "json",

        "has_header": None,

        "mapping": None
    }
}


# ============================================================
# 7. COLUMN NAME SANITIZATION
# ============================================================

def sanitize_column_names(df):

    for col_name in df.columns:

        clean_name = re.sub(
            r'[ ,;{}()\n\t=:-]+',
            '_',
            col_name.strip()
        )

        if clean_name != col_name:

            df = df.withColumnRenamed(
                col_name,
                clean_name
            )

    return df


# ============================================================
# 8. PROCESS SOURCE TO BRONZE
# ============================================================

def process_to_bronze(
    source_name: str,
    config: dict
):

    glob_pattern = config["glob_pattern"]
    file_format = config["format"]
    has_header = config.get("has_header")
    col_mapping = config.get("mapping")


    # --------------------------------------------------------
    # Paths
    # --------------------------------------------------------

    target_delta_path = (
        f"{BRONZE_VOLUME_PATH}/{source_name}"
    )

    checkpoint_path = (
        f"{BRONZE_VOLUME_PATH}/_checkpoints/{source_name}"
    )

    schema_path = (
        f"{BRONZE_VOLUME_PATH}/_schemas/{source_name}"
    )


    print(
        f"🚀 [BRONZE] Ingesting "
        f"'{source_name.upper()}' "
        f"[{file_format.upper()}]..."
    )


    # ========================================================
    # 9. AUTO LOADER
    # ========================================================

    reader = (
        spark.readStream
        .format("cloudFiles")

        .option(
            "cloudFiles.format",
            file_format
        )

        .option(
            "pathGlobFilter",
            glob_pattern
        )

        .option(
            "cloudFiles.schemaLocation",
            schema_path
        )

        .option(
            "cloudFiles.schemaEvolutionMode",
            "addNewColumns"
        )
    )


    # ========================================================
    # 10. FORMAT-SPECIFIC OPTIONS
    # ========================================================

    if file_format == "csv":

        reader = (
            reader

            .option(
                "header",
                has_header
            )

            .option(
                "cloudFiles.inferColumnTypes",
                "true"
            )
        )


    elif file_format == "json":

        reader = (
            reader
            .option(
                "multiLine",
                "true"
            )
        )


    # ========================================================
    # 11. READ LANDING
    # ========================================================

    raw_df = reader.load(
        LANDING_BASE_PATH
    )


    print(
        f"   📥 Raw columns: "
        f"{raw_df.columns}"
    )


    # ========================================================
    # 12. APPLY COLUMN MAPPING
    # ========================================================

    if col_mapping:

        for old_col, new_col in col_mapping.items():

            if old_col in raw_df.columns:

                raw_df = raw_df.withColumnRenamed(
                    old_col,
                    new_col
                )


    # ========================================================
    # 13. SANITIZE COLUMN NAMES
    # ========================================================

    cleaned_df = sanitize_column_names(
        raw_df
    )


    # ========================================================
    # 14. ADD BRONZE METADATA
    # ========================================================

    bronze_df = (
        cleaned_df

        .withColumn(
            "_ingested_at",
            current_timestamp()
        )

        .withColumn(
            "_source_file",
            input_file_name()
        )
    )


    print(
        f"   🧹 Final columns: "
        f"{bronze_df.columns}"
    )


    # ========================================================
    # 15. WRITE DELTA
    # ========================================================

    query = (
        bronze_df

        .writeStream

        .format("delta")

        .option(
            "checkpointLocation",
            checkpoint_path
        )

        .option(
            "mergeSchema",
            "true"
        )

        .outputMode(
            "append"
        )

        .trigger(
            availableNow=True
        )

        .start(
            target_delta_path
        )
    )


    query.awaitTermination()


    print(
        f"✅ [BRONZE] Written to: "
        f"{target_delta_path}"
    )


    # ========================================================
    # 16. VALIDATE TABLE
    # ========================================================

    final_df = (
        spark.read
        .format("delta")
        .load(target_delta_path)
    )


    print(
        f"   📊 Columns: "
        f"{final_df.columns}"
    )

    print(
        f"   📈 Records: "
        f"{final_df.count()}"
    )

    print(
        f"✅ [BRONZE] "
        f"'{source_name.upper()}' "
        f"completed successfully.\n"
    )


# ============================================================
# 17. EXECUTE ALL SOURCES
# ============================================================

print("=" * 70)
print("🚀 HEALTHCARE BRONZE INGESTION STARTED")
print("=" * 70)


for source_name, config in sources_config.items():

    try:

        process_to_bronze(
            source_name,
            config
        )

    except Exception as e:

        print(
            f"❌ [BRONZE] Error processing "
            f"'{source_name}': {str(e)}"
        )


print("=" * 70)
print("🎉 HEALTHCARE BRONZE INGESTION COMPLETED")
print("=" * 70)

# COMMAND ----------

# 1. Reset Claims Checkpoint & Schema in Bronze
BRONZE_VOLUME_PATH = "/Volumes/proj_databricks/healthcare/bronze"
dbutils.fs.rm(f"{BRONZE_VOLUME_PATH}/_schemas/claims", recurse=True)
dbutils.fs.rm(f"{BRONZE_VOLUME_PATH}/_checkpoints/claims", recurse=True)
dbutils.fs.rm(f"{BRONZE_VOLUME_PATH}/claims", recurse=True)

# 2. Headerless Claims Column Mapping
claims_col_mapping = {
    "_c0": "claim_id",
    "_c1": "admission_id",
    "_c2": "policy_number",
    "_c3": "insurer_id",
    "_c4": "insurer_name",
    "_c5": "claim_type",
    "_c6": "claim_date",
    "_c7": "settlement_date",
    "_c8": "claim_amount",
    "_c9": "approved_amount",
    "_c10": "paid_amount",
    "_c11": "claim_status",
    "_c12": "remarks"
}

# 3. Corrected Claims Bronze Ingestion
claims_config = {
    "glob_pattern": "CLAIMS_*.csv",
    "format": "csv",
    "has_header": "false",  # Fixed: set to false
    "mapping": claims_col_mapping
}

print("🚀 [BRONZE RE-RUN] Ingesting 'CLAIMS' with correct mapping...")
process_to_bronze("claims", claims_config)

# COMMAND ----------

spark.read.format("delta").load(f"{BRONZE_VOLUME_PATH}/claims").printSchema()