# OMOPPregnancyV2

Database-agnostic pregnancy episode identification in OMOP CDM databases.

## Overview

This package implements the HIPPS (Hierarchical Identification of Pregnancy and Pregnancy Progression Signatures) algorithm to identify pregnancy episodes from OMOP CDM databases. It combines:

- **HIP Algorithm**: Hierarchical outcome-based episode identification
- **PPS Algorithm**: Temporal signature-based episode detection  
- **ESD Algorithm**: Gestational timing refinement for accurate start dates
- **Episode Merging**: Intelligent deduplication and episode consolidation

## Features

- ✅ **Database Agnostic**: Supports both SQL Server and Databricks/Spark
- ✅ **Performance Optimized**: Handles 300K+ person cohorts in ~60 seconds using temp tables
- ✅ **Full Algorithm Alignment**: Implements All of Us algorithm specifications
- ✅ **Comprehensive Output**: Identifies pregnancy episodes with outcome classification and date precision

## Architecture

The V2 implementation follows a clean three-layer architecture:

1. **Extraction Layer** (`R/01_extraction/`)
   - Single database operation to extract all needed data
   - Uses SqlRender for database-agnostic SQL generation
   - Enforces consistent data types across platforms

2. **Algorithm Layer** (`R/02_algorithms/`)
   - Pure R implementations of HIP and PPS algorithms
   - No SQL generation or database operations
   - Clear, testable functions

3. **Results Layer** (`R/03_results/`)
   - Saves results to CSV or database
   - Generates summary statistics
   - Exports data for analysis

## Key Improvements from V1

- **No smart_compute**: Removed complex optimization logic that caused platform-specific issues
- **No lazy evaluation**: All data collected immediately for consistent behavior
- **No platform branching**: Same code path for all databases
- **Type safety**: Explicit type enforcement prevents SQL Server date issues
- **Simplified merging**: Pure R episode merging without SQL window functions

## Installation

```r
# Install required packages
install.packages(c("DatabaseConnector", "SqlRender", "dplyr", "lubridate", "readr"))

# Setup JDBC drivers
source("inst/scripts/setup_jdbc_drivers.R")

# Create environment configuration
cp inst/templates/.env.template .env
# Edit .env with your database settings
```

## Quick Start

### 1. Setup Environment

```bash
# Copy the environment template
cp inst/templates/.env.template .env
# Edit .env with your database settings
```

### 2. Run Analysis

```r
# Load connection functions
source("R/00_connection/create_connection.R")

# Create connection from environment
conn <- create_connection_from_env()

# Run pregnancy identification
source("R/main.R")
episodes <- run_pregnancy_identification(
  connection = conn,
  cdm_database_schema = "cdm_schema",
  results_database_schema = "results_schema",  # optional
  output_folder = "output/",
  min_age = 15,
  max_age = 56
)

DatabaseConnector::disconnect(conn)
```

### 3. Alternative: Run Complete Pipeline

```r
# Run the full pipeline script
source("inst/scripts/run_pregnancy_analysis.R")
```

## Files and Structure

```
OMOPPregnancyV2/
├── R/
│   ├── 00_concepts/         # Concept loading and validation
│   ├── 00_connection/       # Database connection management
│   ├── 01_extraction/       # Database extraction layer
│   ├── 02_algorithms/       # HIP, PPS, and merging algorithms
│   ├── 03_results/          # Result saving and export
│   ├── 03_utilities/        # Utility functions and helpers
│   └── main.R               # Main analysis function
├── inst/
│   ├── extdata/             # Concept definition CSV files
│   ├── scripts/             # Utility and setup scripts
│   ├── sql/                 # SQL templates
│   └── templates/           # Environment configuration templates
├── DESCRIPTION              # Package description
├── NAMESPACE                # Exported functions
└── README.md                # This file
```

## Concept Files

The algorithm requires concept definition files located in `inst/extdata/`:

1. **hip_concepts.csv**: HIP algorithm concepts (outcomes, pregnancy indicators)
2. **pps_concepts.csv**: PPS algorithm concepts with gestational timing
3. **matcho_limits.csv**: Term duration limits for different outcomes
4. **matcho_outcome_limits.csv**: Outcome-specific gestational limits
5. **matcho_term_durations.csv**: Term duration definitions

## Algorithms

### HIP (Hierarchical Identification of Pregnancy)
- Identifies pregnancies based on outcome codes
- Uses Matcho hierarchy for outcome prioritization
- Estimates start dates from gestational age or term limits

### PPS (Pregnancy Progression Signatures)
- Identifies pregnancies from temporal patterns
- Uses gestational timing windows
- Validates temporal consistency

### Episode Merging
- Combines HIP and PPS results
- Resolves overlaps based on evidence quality
- Assigns confidence scores

## Output

The analysis produces:
- **Pregnancy episodes**: Person-level pregnancy episodes with outcomes
- **Summary statistics**: Counts by outcome, algorithm, and confidence
- **Export files**: CSV and RDS formats for further analysis

## Testing

Test your connection and run a sample analysis:

```r
# Test database connection
source("inst/scripts/test_connection.R")

# Run full pipeline test
source("inst/scripts/test_full_pipeline.R")
```

## Performance

- Extraction: Single database query (typically 10-60 seconds)
- Processing: Pure R algorithms (typically 5-30 seconds)
- Total runtime: Usually under 2 minutes for typical cohorts

## Database Compatibility

Tested with:
- SQL Server
- PostgreSQL
- Spark/Databricks
- BigQuery (via OHDSI adapters)

## Support

This is a complete rewrite addressing the issues found in V1:
- SQL Server date parsing errors
- Databricks performance degradation
- Complex smart_compute logic
- Platform-specific branching

The new implementation prioritizes simplicity and reliability over optimization.