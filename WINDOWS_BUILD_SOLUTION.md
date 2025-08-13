# Windows Package Build Solution

## Problem
`devtools::build()` fails on Windows with error: "System command 'Rcmd.exe' failed"

## Root Causes
1. The `source()` calls in algorithm files were causing build issues (FIXED)
2. Missing dependencies on Windows system
3. Potential file path or line ending issues

## Solutions

### Option 1: Build Without devtools (Recommended)
Use R CMD directly from the command line:

```bash
# In Windows Command Prompt or PowerShell
# Navigate to the OMOPPregnancy directory
cd path\to\OMOPPregnancy

# Build without vignettes (faster)
R CMD build --no-build-vignettes --no-manual .

# Or build with everything
R CMD build .
```

### Option 2: Load Functions Directly (For Development)
If you just need to use the functions without building the package:

```r
# Set working directory to package root
setwd("path/to/OMOPPregnancy")

# Load required libraries
library(dplyr)
library(dbplyr)
library(SqlRender)
library(DatabaseConnector)

# Source the SQL functions
source("R/sql_functions.R")

# Source other required files
source("R/config.R")
source("R/connection.R")
source("R/query_utils.R")
source("R/hip_algorithm.R")
source("R/pps_algorithm.R")

# Now all functions are available for use
```

### Option 3: Fix devtools Build
If you must use `devtools::build()`, try these steps:

1. **Install all dependencies first:**
```r
install.packages(c(
  "DBI", "dplyr", "dbplyr", "tidyr", "readr",
  "lubridate", "rlang", "tibble", "purrr", "here",
  "readxl", "yaml", "stringr", "DatabaseConnector", "SqlRender"
))
```

2. **Check for file issues:**
```r
# Run the diagnostic script
source("check_package_windows.R")
```

3. **Try building with options:**
```r
# Use pkgbuild directly with specific options
pkgbuild::build(vignettes = FALSE, manual = FALSE)

# Or try with devtools but skip checks
devtools::build(vignettes = FALSE, manual = FALSE, args = "--no-multiarch")
```

### Option 4: Use RStudio Build Tools
In RStudio:
1. Open the project
2. Go to Build menu → Build Source Package
3. Or use Build pane → More → Build Source Package

## What Was Fixed

### Changes Made to Resolve source() Issues:
1. ✅ Removed `source()` calls from `hip_algorithm.R` and `pps_algorithm.R`
2. ✅ Added SQL functions to NAMESPACE exports
3. ✅ Added required imports to NAMESPACE
4. ✅ Fixed DESCRIPTION file (added missing dependencies and newline)
5. ✅ Cleaned up backup files

### Files Modified:
- `R/hip_algorithm.R` - Removed source() block
- `R/pps_algorithm.R` - Removed source() block  
- `NAMESPACE` - Added exports and imports
- `DESCRIPTION` - Added stringr dependency, fixed newline

## Verification Steps

Run this test to verify everything works:

```r
# Test that functions load correctly
source("test_load_directly.R")
```

Expected output:
- ✅ All SQL functions working
- ✅ Algorithm files load successfully
- ✅ Key functions available

## If Still Having Issues

1. **Check R and Rtools versions:**
```r
R.version.string
# Should be R 4.0.0 or higher

# On Windows, check Rtools
pkgbuild::has_rtools()
# Should return TRUE
```

2. **Clear any cached build files:**
```bash
# Remove old build artifacts
rm OMOPPregnancy_*.tar.gz
rm -rf OMOPPregnancy.Rcheck
```

3. **Check file permissions:**
- Ensure you have write access to all directories
- No files are locked by other programs

4. **Antivirus considerations:**
- Some antivirus software can interfere with R package building
- Try temporarily disabling real-time scanning for the project folder

## Alternative: Install from GitHub

If local building continues to fail, push changes to GitHub and install from there:

```r
# Install from GitHub repository
devtools::install_github("ultraub/OMOP-Pregnancy")

# Or use remotes package
remotes::install_github("ultraub/OMOP-Pregnancy")
```

## Summary

The package code is working correctly - the SqlRender integration is functional and all algorithm files load properly. The build issue appears to be specific to the Windows devtools environment. Using R CMD build directly or loading functions via source() are reliable alternatives.