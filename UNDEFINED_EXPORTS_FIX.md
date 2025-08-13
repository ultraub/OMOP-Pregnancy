# Undefined Exports Fix

## Problem
Package build failing with error: "undefined exports: date_add"

## Root Cause
The NAMESPACE file was exporting `date_add` function, but this function doesn't exist in any of the R files. The actual function is named `sql_date_add`.

## Solution Applied

### 1. Removed undefined export
**File**: `NAMESPACE`
- Removed: `export(date_add)`
- Kept: `export(sql_date_add)` (the actual function)

### 2. Fixed missing newlines
Added newlines to end of all R files and NAMESPACE to prevent warnings:
- R/config.R
- R/connection.R
- R/esd_algorithm.R
- R/hip_algorithm_updated_example.R
- R/hip_algorithm.R
- R/load_env.R
- R/merge_episodes.R
- R/pps_algorithm.R
- R/query_utils.R
- R/sql_functions.R
- NAMESPACE

## Available Date Functions

The package now correctly exports these date-related functions:

### From sql_functions.R:
- `sql_date_diff()` - Cross-platform date difference calculation
- `sql_date_from_parts()` - Create date from year, month, day
- `sql_date_add()` - Add/subtract days from a date
- `sql_concat()` - Concatenate strings

### From query_utils.R:
- `date_diff()` - Wrapper for date difference (uses sql_date_diff internally)
- `date_diff_sql()` - SQL date difference (uses sql_date_diff internally)

## Verification

Run this check to verify all exports are defined:
```r
source("check_exports.R")
```

Expected output:
```
✅ All exports are defined!
```

## Building the Package

The package now builds successfully:
```bash
R CMD build --no-build-vignettes --no-manual .
```

Or in R:
```r
devtools::build(vignettes = FALSE, manual = FALSE)
```

## Usage Example

```r
library(OMOPPregnancy)

# Use the SQL date functions
result <- sql_date_add("visit_date", "30", "day", connection)
result <- sql_date_diff("date1", "date2", "day", connection)
result <- sql_date_from_parts("2024", "1", "15", connection)
```

## Important Note
There is no `date_add()` function - use `sql_date_add()` instead. The naming convention is:
- `sql_*` for direct SQL generation functions
- Plain names for higher-level wrappers