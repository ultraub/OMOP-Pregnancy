# OMOP Pregnancy V2 Architecture

## Overview
This document describes the simplified, database-agnostic architecture for the OMOP Pregnancy Package V2. This redesign prioritizes maintainability, testability, and cross-platform compatibility over complex optimization strategies.

## Core Principles

### 1. Database Agnosticism
- All SQL generation through OHDSI SqlRender
- No platform-specific branching in application code
- Single code path for all databases

### 2. Clear Separation of Concerns
- **Extraction Layer**: Database → Local R data frames
- **Algorithm Layer**: Pure R processing
- **Results Layer**: Optional write-back to database

### 3. Explicit Data Contracts
- Data types enforced immediately after extraction
- No ambiguous lazy/eager evaluation
- Clear interfaces between layers

### 4. Simplicity Over Cleverness
- No smart_compute() or complex optimization
- Straightforward, readable code
- Predictable performance characteristics

## Architecture Layers

### Layer 1: Data Extraction
**Purpose**: Extract pregnancy-related data from OMOP CDM

**Key Features**:
- Single SQL query per domain table
- Immediate collection to local R
- Type enforcement at extraction boundary
- No lazy evaluation

**Components**:
- `extract_cohort.R`: Main extraction orchestrator
- `extract_concepts.R`: Load concept sets from CSV
- `type_enforcement.R`: Standardize data types across platforms

### Layer 2: Algorithm Processing
**Purpose**: Implement HIP and PPS algorithms in pure R

**Key Features**:
- No database operations
- Standard R/tidyverse operations only
- Testable, debuggable functions
- No SQL generation

**Components**:
- `hip_algorithm.R`: Hierarchy-based pregnancy identification
- `pps_algorithm.R`: Pregnancy progression signatures
- `merge_episodes.R`: Episode merging logic

### Layer 3: Utilities & Results
**Purpose**: Support functions and result handling

**Key Features**:
- Simple connection management
- Date manipulation helpers
- Optional result persistence

**Components**:
- `connection.R`: Database connection helpers
- `date_helpers.R`: Date arithmetic functions
- `validation.R`: Data validation
- `save_results.R`: Optional database write-back

## Data Flow

```
1. Load Concepts (CSV files)
   ↓
2. Extract Data (One-time database query)
   - Conditions
   - Procedures  
   - Observations
   - Measurements
   - Person demographics
   ↓
3. Enforce Types (Standardize across platforms)
   ↓
4. Run Algorithms (Pure R)
   - HIP Algorithm
   - PPS Algorithm
   ↓
5. Merge Episodes (Pure R)
   ↓
6. Return Results (Local data frame)
   ↓
7. Optional: Save to Database/File
```

## What We're Eliminating

### ❌ No More:
- `smart_compute()` with reuse counting
- Platform detection (`if dbms == "sql server"`)
- Mixed lazy/eager evaluation
- Nested computed tables/views
- Manual SQL string construction
- Complex window functions in SQL
- Type confusion between platforms

### ✅ Instead:
- Explicit extraction points
- Pure R processing
- OHDSI SqlRender for all SQL
- Simple, flat queries
- Consistent type handling
- R-based episode assignment
- Clear error messages

## Performance Considerations

### Trade-offs
- **Memory**: Full dataset in R memory (~100-500MB typical)
- **Speed**: Single extraction is fast, R processing is efficient
- **Scalability**: Works well up to ~10M pregnancies

### Optimization Opportunities
- Parallel processing in R (future package)
- Data.table for large datasets
- Chunked processing if needed

## Benefits

### 1. Maintainability
- 80% less code
- Clear, linear flow
- Easy to understand

### 2. Testability
- Pure functions
- Mockable inputs
- Deterministic outputs

### 3. Debuggability
- Inspect data at each step
- Clear error messages
- No query complexity issues

### 4. Compatibility
- Works identically across:
  - SQL Server
  - PostgreSQL
  - Oracle
  - Databricks/Spark
  - BigQuery
  - Snowflake

## Migration Path

### Phase 1: Parallel Development
- Keep V1 intact
- Develop V2 alongside
- No breaking changes

### Phase 2: Validation
- Compare outputs V1 vs V2
- Performance benchmarking
- Cross-platform testing

### Phase 3: Transition
- Documentation update
- User migration guide
- Deprecation notices

## Implementation Standards

### Coding Standards
- Tidyverse style guide
- Comprehensive roxygen documentation
- Unit tests for all functions
- No more than 100 lines per function

### SQL Templates
- Store in `inst/sql/`
- Use SqlRender parameters
- Keep queries simple and flat
- Comment complex logic

### Error Handling
- Informative error messages
- Input validation
- Graceful failures
- Clear stack traces

## Success Metrics

### Technical
- ✅ Zero platform-specific code branches
- ✅ All tests pass on all platforms
- ✅ <5 second execution for typical dataset
- ✅ <500MB memory usage

### Quality
- ✅ 90% code coverage
- ✅ No critical bugs in 6 months
- ✅ Clear documentation
- ✅ Easy onboarding (<1 day)

## Conclusion

This architecture represents a fundamental shift from optimization-focused to maintainability-focused design. By accepting slightly higher memory usage, we gain dramatically simpler code that works reliably across all OMOP CDM platforms.

The key insight: **For typical pregnancy cohorts (100K-1M episodes), the performance difference between complex database optimization and simple R processing is negligible, but the maintenance burden difference is enormous.**