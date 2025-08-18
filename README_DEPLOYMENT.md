# OMOP Pregnancy V2 - Deployment Guide

## Overview

The OMOP Pregnancy V2 package identifies pregnancy episodes from OMOP CDM databases using the HIP, PPS, and ESD algorithms aligned with the All of Us implementation. This version supports both SQL Server and Databricks environments.

## Quick Start

### 1. Initial Setup

```bash
# Clone or copy the OMOPPregnancyV2 directory to your environment
cd OMOPPregnancyV2

# Run the setup script
Rscript inst/scripts/setup.R
```

### 2. Configure Environment

Choose your database environment and configure accordingly:

#### For SQL Server

Copy the template and edit with your credentials:
```bash
cp inst/scripts/.env.sqlserver.template .env
# Edit .env with your SQL Server credentials
```

Example .env for SQL Server with Windows AD authentication:
```bash
OMOP_ENV=SQLSERVER
SQL_SERVER=your.server.com
SQL_DATABASE=your_database
# Leave blank for Windows AD authentication:
SQL_USER=
SQL_PASSWORD=
SQL_PORT=1433
SQL_CDM_SCHEMA=dbo
SQL_RESULTS_SCHEMA=dbo
SQL_JDBC_PATH=./jdbc_drivers
```

Example .env for SQL Server with SQL authentication:
```bash
OMOP_ENV=SQLSERVER
SQL_SERVER=your.server.com
SQL_DATABASE=your_database
SQL_USER=your_username
SQL_PASSWORD=your_password
SQL_PORT=1433
SQL_CDM_SCHEMA=dbo
SQL_RESULTS_SCHEMA=dbo
SQL_JDBC_PATH=./jdbc_drivers
```

#### For Databricks

Copy the template and edit with your credentials:
```bash
cp inst/scripts/.env.databricks.template .env
# Edit .env with your Databricks token
```

Example .env for Databricks:
```bash
OMOP_ENV=DATABRICKS
DATABRICKS_SERVER=adb-xxxx.azuredatabricks.net
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxxx
DATABRICKS_TOKEN=your_token_here
DATABRICKS_CDM_SCHEMA=omop.data
DATABRICKS_RESULTS_SCHEMA=your_schema.your_project
DATABRICKS_JDBC_PATH=./jdbc_drivers
```

### 3. Run the Analysis

```bash
# Load environment variables (if using .env file)
export $(cat .env | xargs)

# Run the unified analysis script
Rscript inst/scripts/run_pregnancy_analysis.R
```

## Algorithm Overview

The pipeline runs four main algorithms in sequence:

1. **HIP (Hierarchical Identification of Pregnancy)**: Identifies episodes based on outcome codes
2. **PPS (Pregnancy Progression Signatures)**: Identifies episodes based on gestational timing
3. **Merge Episodes**: Combines HIP and PPS episodes, resolving overlaps
4. **ESD (Estimated Start Date)**: Refines pregnancy start dates using gestational timing

## Performance Benchmarks

Based on testing with 328,938 persons:

| Phase | Time | Records |
|-------|------|---------|
| Data Extraction | ~30 seconds | 328,938 persons |
| HIP Algorithm | ~2 seconds | 4,613 episodes |
| PPS Algorithm | ~11 seconds | 7,608 episodes |
| Episode Merging | ~3.5 seconds | 11,555 merged episodes |
| ESD Refinement | ~12 seconds | Date precision calculation |
| **Total Pipeline** | **~60 seconds** | **11,555 final episodes** |

## Output Files

The analysis generates two output files in the `output/` directory:

1. **pregnancy_episodes_[ENV]_[DATE].csv**: Detailed pregnancy episodes with:
   - Person ID and episode number
   - Episode start and end dates
   - Outcome category (LB, SB, AB, SA, ECT, DELIV, PREG)
   - Gestational age in days
   - Algorithm source (HIP, PPS, or MERGED)
   - ESD precision category

2. **pipeline_summary_[ENV]_[DATE].rds**: Summary statistics including:
   - Total episodes by outcome
   - Episodes per person statistics
   - Algorithm performance metrics
   - Runtime information

## Environment Variables Reference

### Common Variables
- `OMOP_ENV`: Database environment (SQLSERVER or DATABRICKS)

### SQL Server Variables
- `SQL_SERVER`: Server hostname
- `SQL_DATABASE`: Database name
- `SQL_USER`: Username (leave blank for Windows AD authentication)
- `SQL_PASSWORD`: Password (leave blank for Windows AD authentication)
- `SQL_PORT`: Port (default: 1433)
- `SQL_CDM_SCHEMA`: CDM schema name
- `SQL_RESULTS_SCHEMA`: Results schema name
- `SQL_JDBC_PATH`: Path to JDBC drivers

### Databricks Variables
- `DATABRICKS_SERVER`: Databricks server URL
- `DATABRICKS_HTTP_PATH`: HTTP path to SQL warehouse
- `DATABRICKS_TOKEN`: Personal access token
- `DATABRICKS_CDM_SCHEMA`: CDM schema name
- `DATABRICKS_RESULTS_SCHEMA`: Results schema name
- `DATABRICKS_JDBC_PATH`: Path to JDBC drivers

## Troubleshooting

### Connection Issues

**SQL Server timeout errors:**
```r
options(DatabaseConnector.queryTimeout = 7200)  # 2 hour timeout
```

**Databricks Arrow memory errors:**
Add to your script before connecting:
```r
options(java.parameters = c("-Xmx8g", "-XX:MaxDirectMemorySize=4g"))
```

### Missing Tables

Ensure your user has SELECT permissions on all required CDM tables:
- person
- observation
- measurement
- condition_occurrence
- procedure_occurrence
- visit_occurrence
- concept

### Large Cohort Performance

The package uses temporary tables to handle large cohorts efficiently. Ensure your database user has permissions to create temporary tables.

## Advanced Usage

### Custom Age Ranges

Edit the extraction parameters in `run_pregnancy_analysis.R`:
```r
cohort_data <- extract_pregnancy_cohort(
  connection = connection,
  cdm_schema = cdm_schema,
  hip_concepts = concepts$hip_concepts,
  pps_concepts = concepts$pps_concepts,
  min_age = 18,  # Custom minimum age
  max_age = 45,  # Custom maximum age
  use_temp_tables = TRUE
)
```

### Filtering by Date Range

Add date filters to the extraction:
```r
cohort_data <- extract_pregnancy_cohort(
  connection = connection,
  cdm_schema = cdm_schema,
  hip_concepts = concepts$hip_concepts,
  pps_concepts = concepts$pps_concepts,
  min_age = 15,
  max_age = 56,
  start_date = "2015-01-01",  # Optional start date
  end_date = "2023-12-31",     # Optional end date
  use_temp_tables = TRUE
)
```

## Directory Structure

```
OMOPPregnancyV2/
├── R/
│   ├── 00_concepts/        # Concept set definitions
│   ├── 01_extraction/       # Data extraction functions
│   ├── 02_algorithms/       # HIP, PPS, Merge, ESD algorithms
│   ├── 03_utilities/        # Helper functions
│   └── 03_results/          # Result processing
├── inst/
│   ├── scripts/             # Run scripts
│   │   ├── run_pregnancy_analysis.R
│   │   ├── setup.R
│   │   ├── .env.sqlserver.template
│   │   └── .env.databricks.template
│   └── concepts/            # Concept CSV files
├── jdbc_drivers/            # JDBC driver files
├── output/                  # Analysis output
└── README_DEPLOYMENT.md     # This file
```

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review error messages for specific guidance
3. Ensure all environment variables are set correctly
4. Verify database permissions

## Version Information

- **Version**: 2.0.0
- **Algorithm Alignment**: All of Us Pregnancy Algorithm
- **Database Support**: SQL Server, Databricks (Spark)
- **OMOP CDM**: v5.3+