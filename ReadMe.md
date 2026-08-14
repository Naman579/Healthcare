#🚀 Healthcare Data Engineering Pipeline

An end-to-end healthcare data engineering project built on Microsoft Azure.

The pipeline ingests healthcare data from multiple sources, applies data quality checks and transformations, stores the data using a Medallion Architecture, and serves the final data through Power BI dashboards.

## Project Overview

The project processes around **950K+ healthcare records** across four datasets:

- Patients – 100K+
- Admissions – 300K+
- Claims – 500K+
- Diagnostics – 50K+

The main goal was to build a reusable ETL pipeline instead of separate hard-coded pipelines for every source.

## Architecture

```text
Data Sources
     ↓
Metadata Control Database
     ↓
Azure Data Factory
     ↓
ADLS Gen2
     ↓
Data Quality & Validation
     ↓
Azure Databricks
     ↓
Bronze → Silver → Gold
     ↓
Star Schema
     ↓
Power BI
```

### Data Sources

- Claims – Azure SQL
- Admissions – Azure SQL
- Patients – ADLS Gen2
- Diagnostics – ADLS Gen2

Supported formats:

- CSV
- JSON
- Parquet

## Metadata-Driven ETL

Azure SQL is used as the metadata control database.

Metadata manages:

- Source configuration
- File paths
- Active flags
- Pipeline parameters
- Watermark values
- Incremental load information

### ADF Pipeline Flow

```text
Lookup Metadata
       ↓
ForEach Source
       ↓
If Condition
       ↓
Dynamic Copy
       ↓
ADLS Gen2
```

This makes the ingestion process reusable and reduces the need for separate pipelines for each source.

## Incremental Loading

Watermark-based incremental loading is implemented for sources where new or modified records need to be identified.

Instead of processing the complete dataset every time, the pipeline uses the previous watermark to identify new and updated records.

## Data Quality & Validation

Validation checks include:

- Schema validation
- Null checks
- Duplicate detection
- Data type validation
- Business rule validation
- Invalid record quarantine

## Azure Databricks – Medallion Architecture

### Bronze Layer

The Bronze layer contains raw ingested data.

Implemented using:

- Azure Databricks
- PySpark
- Auto Loader / CloudFiles
- Delta Lake
- Schema evolution
- Audit columns

Audit columns include ingestion timestamp and source file information.

### Silver Layer

The Silver layer contains cleaned and standardized data.

Main transformations:

- Data cleansing
- Column standardization
- Deduplication
- Data type conversion
- Business transformations
- Data validation
- PII masking using SHA-256

Patient PII such as names, email, phone number and national ID are hashed before reaching the analytical layer.

### Gold Layer

The Gold layer contains business-ready analytical tables using a Star Schema.

#### Dimension Tables

- `dim_patient`
- `dim_diagnostics`
- `dim_hospital`
- `dim_doctor`
- `dim_insurance`
- `dim_date`

#### Fact Tables

- `fact_admissions`
- `fact_claims`

SCD Type 2 is implemented for the Patient dimension using Delta Lake `MERGE`.

Gold tables are optimized using:

- OPTIMIZE
- Z-ORDER
- VACUUM

## Power BI Dashboards

Four Power BI dashboards were created:

### 1. Executive Overview

High-level healthcare operations and KPI overview.

### 2. Patient & Admission Analytics

Patient and admission-related analysis.

### 3. Claims & Insurance Analytics

Claims, approval/rejection status, insurance and claim amount analysis.

### 4. Hospital & Clinical Analytics

Hospital, diagnostic and clinical-related insights.

## Azure Data Factory

ADF handles ingestion and orchestration.

Main activities:

- Lookup
- ForEach
- If Condition
- Copy Activity
- Parameterized pipelines

Additional features:

- Incremental loading
- Retry logic
- Error handling
- Logging
- Pipeline monitoring
- Failure alerts

## DevOps

Git and Azure DevOps are used for source control and project management.

Includes:

- Git version control
- Azure DevOps
- Databricks Repos
- Pipeline monitoring
- Audit and execution logs
- CI/CD-ready structure

## Technology Stack

| Area | Technology |
|---|---|
| Cloud | Microsoft Azure |
| ETL / Orchestration | Azure Data Factory |
| Data Lake | ADLS Gen2 |
| Data Processing | Azure Databricks |
| Programming | Python / PySpark |
| Storage Format | Delta Lake |
| Database | Azure SQL |
| Data Modeling | Star Schema |
| Data Quality | PySpark Validation |
| BI | Power BI |
| Version Control | Git / Azure DevOps |

## Project Structure

```text
Healthcare-Data-Engineering/
│
├── ADF/
│   ├── Pipelines/
│   ├── Datasets/
│   ├── LinkedServices/
│   └── Triggers/
│
├── Databricks/
│   ├── Bronze/
│   ├── Silver/
│   └── Gold/
│
├── SQL/
│   └── Metadata_Control_Tables/
│
├── PowerBI/
│   └── Dashboards/
│
├── Documentation/
│   └── Architecture/
│
└── README.md
```

## End-to-End Pipeline

```text
DATA SOURCES
  Claims (Azure SQL)
  Admissions (Azure SQL)
  Patients (ADLS Gen2)
  Diagnostics (ADLS Gen2)
             ↓
METADATA CONTROL DATABASE
  Source Configuration
  Watermarks
  File Paths
  Active Flag
  Pipeline Parameters
             ↓
AZURE DATA FACTORY
  Lookup → ForEach → If Condition → Dynamic Copy
  Incremental Load | Retry | Logging | Alerts
             ↓
ADLS GEN2
  Landing / Raw Zone
  Archive Zone
             ↓
DATA QUALITY
  Schema | Null | Duplicate | Data Type
  Business Rules | Quarantine
             ↓
AZURE DATABRICKS
  BRONZE → SILVER → GOLD
             ↓
STAR SCHEMA
  Fact + Dimension Tables
             ↓
POWER BI
  Executive Overview
  Patient & Admission Analytics
  Claims & Insurance Analytics
  Hospital & Clinical Analytics
```

## Data Flow

1. Source information is stored in metadata control tables.
2. ADF reads the configuration and dynamically processes active sources.
3. Data is copied into ADLS Gen2 Landing / Raw storage.
4. Data quality checks identify invalid records.
5. Bronze stores incoming data as Delta tables with audit information.
6. Silver cleans, standardizes, deduplicates and masks PII.
7. Gold creates Fact and Dimension tables.
8. Power BI reads the Gold layer for reporting.

## Example Business Outputs

The Gold layer supports analysis such as:

- Total claims
- Approved claims
- Rejected claims
- Pending claims
- Claim amounts
- Approved amounts
- Admission volume
- Length of stay
- High-cost admissions
- Patient demographics
- Insurance analysis
- Hospital analysis
- Diagnostic analysis

## Security

Patient-sensitive fields are not exposed directly in the Silver analytical data.

PII fields are transformed using SHA-256 hashing, including:

- First name
- Last name
- Email
- Phone number
- National ID / SSN

## Performance

The Gold layer uses Delta Lake optimization techniques:

- **OPTIMIZE** – improves physical table layout.
- **Z-ORDER** – improves data skipping for frequently queried columns.
- **VACUUM** – removes old files according to the configured retention period.

## Monitoring & Error Handling

ADF pipelines include:

- Retry policies
- Error handling
- Pipeline monitoring
- Execution logging
- Failure alerts

Audit information is maintained to help track processing.

## Key Features

- Metadata-driven ingestion
- Watermark-based incremental loading
- Multi-source ingestion
- Dynamic ADF pipelines
- Data quality validation
- Quarantine handling
- Medallion Architecture
- Delta Lake
- Auto Loader
- PII masking
- SCD Type 2
- Star Schema
- Delta MERGE
- OPTIMIZE
- Z-ORDER
- VACUUM
- Power BI reporting
- Azure DevOps
- Git version control
- Pipeline monitoring
- Logging and failure handling

## Project Learning

This project provided hands-on practice with the complete Azure data engineering flow:

```text
Source Systems
      ↓
Metadata & Control Tables
      ↓
Azure Data Factory
      ↓
ADLS Gen2
      ↓
Data Quality
      ↓
Databricks
      ↓
Bronze
      ↓
Silver
      ↓
Gold
      ↓
Star Schema
      ↓
Power BI
```

Main areas covered:

- Metadata-driven ETL
- Incremental data loading
- Dynamic ADF pipelines
- PySpark transformations
- Delta Lake
- Data quality
- PII masking
- Medallion Architecture
- SCD Type 2
- Star Schema
- Power BI
- Azure DevOps

## Future Improvements

- Automated CI/CD deployment
- More automated data quality reporting
- Additional alerting scenarios
- Automated PySpark unit tests
- More detailed pipeline cost monitoring
- Additional incremental loading scenarios

## Screenshots

Recommended screenshots to add:

1. Overall architecture
2. Azure Data Factory pipeline
3. Metadata control tables
4. Bronze / Silver / Gold tables
5. Power BI Executive Overview
6. Power BI Patient & Admission Analytics
7. Power BI Claims & Insurance Analytics
8. Power BI Hospital & Clinical Analytics

## Repository Links

**GitHub Repository:** Add your repository link here

**LinkedIn:** Add your LinkedIn profile here

## Author

**Naman Kanojia**

Aspiring Data Engineer

**Skills:** Azure | Python | SQL | PySpark | Databricks | ADF | ADLS Gen2 | Delta Lake | Power BI | Azure DevOps

## Disclaimer

This is a personal learning/project implementation created to practice Azure Data Engineering concepts. The healthcare datasets used in the project are not intended to represent real patient data.
