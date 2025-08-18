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
```

## Usage

```r
# Source the main script
source("run_pregnancy_analysis.R")

# Create database connection
connection <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",  # or "sql server", "spark", etc.
  server = "localhost/cdm",
  user = "user",
  password = "password"
)

conn <- DatabaseConnector::connect(connection)

# Run pregnancy identification
episodes <- run_pregnancy_identification(
  connection = conn,
  cdm_schema = "cdm_53",
  vocabulary_schema = "cdm_53",
  output_folder = "output/",
  min_age = 15,
  max_age = 56
)

DatabaseConnector::disconnect(conn)
```

## Files and Structure

```
OMOPPregnancyV2/
├── R/
│   ├── 00_concepts/       # Concept loading and validation
│   ├── 01_extraction/      # Database extraction layer
│   ├── 02_algorithms/      # HIP, PPS, and merging algorithms
│   └── 03_results/         # Result saving and export
├── inst/
│   ├── csv/               # Concept definition files
│   └── sql/               # SQL templates
├── run_pregnancy_analysis.R  # Main execution script
├── review_implementation.R   # Implementation validation
└── ARCHITECTURE.md          # Detailed architecture documentation
```

## Concept Files

The algorithm requires three CSV files with concept definitions:

1. **hip_concepts.csv**: HIP algorithm concepts (outcomes, pregnancy indicators)
2. **pps_concepts.csv**: PPS algorithm concepts with gestational timing
3. **matcho_limits.csv**: Term duration limits for different outcomes

These are provided in `inst/csv/` directory.

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

Run the implementation review to verify all components:

```r
source("review_implementation.R")
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