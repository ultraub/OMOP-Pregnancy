# Installation Guide for OMOP Pregnancy Package

## Overview
This package has dependencies on both CRAN and GitHub packages, particularly the OHDSI suite of tools. This guide provides multiple methods for installing all required dependencies.

## Prerequisites

### Java Requirements
DatabaseConnector requires Java to be installed and configured:

**macOS:**
```bash
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
```

**Linux:**
```bash
sudo apt-get install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
```

**Windows:**
- Download from: https://adoptium.net/
- Set JAVA_HOME to installation directory (e.g., `C:\Program Files\Java\jdk-17`)

## Installation Methods

### Method 1: Automated Script (Recommended)
Run the provided installation script that handles all dependencies:

```r
source("install_dependencies.R")
```

This script will:
- Check Java configuration
- Install all CRAN packages
- Install OHDSI packages from GitHub
- Download JDBC drivers for database connections
- Verify installation

### Method 2: Using renv (Reproducible Environments)
For reproducible environments with locked package versions:

```r
# Install renv if not already installed
install.packages("renv")

# Initialize renv in the project
renv::init()

# Install dependencies from renv.lock
renv::restore()
```

### Method 3: Manual Installation
Install packages individually:

```r
# CRAN packages
install.packages(c(
  "DBI", "dplyr", "dbplyr", "tidyr", "readr",
  "lubridate", "rlang", "tibble", "purrr", 
  "here", "readxl", "yaml"
))

# Install remotes for GitHub packages
install.packages("remotes")

# OHDSI packages
remotes::install_github("OHDSI/DatabaseConnector")
remotes::install_github("OHDSI/SqlRender")

# Optional but recommended
remotes::install_github("darwin-eu-dev/CDMConnector")
remotes::install_github("OHDSI/Eunomia")  # Test CDM data

# Download JDBC drivers
DatabaseConnector::downloadJdbcDrivers("sql server", pathToDriver = "inst/jdbc")
DatabaseConnector::downloadJdbcDrivers("postgresql", pathToDriver = "inst/jdbc")
```

### Method 4: Using devtools
If you want to install the package and its dependencies directly:

```r
# Install devtools
install.packages("devtools")

# Install the package with dependencies
devtools::install(dependencies = TRUE)

# Or from GitHub (once published)
devtools::install_github("ultraub/OMOP-Pregnancy")
```

## Package Dependencies

### Required (Imports)
- **Data Manipulation:** dplyr, tidyr, purrr, tibble
- **Database:** DBI, dbplyr, DatabaseConnector, SqlRender
- **File I/O:** readr, readxl, yaml, here
- **Date/Time:** lubridate
- **Programming:** rlang

### Suggested (Development/Testing)
- **Testing:** testthat
- **Documentation:** knitr, rmarkdown
- **Coverage:** covr
- **Test Data:** Eunomia, CDMConnector

## Troubleshooting

### Java Issues
If you encounter Java-related errors:

1. Verify Java installation:
```r
system("java -version")
```

2. Set JAVA_HOME in R:
```r
Sys.setenv(JAVA_HOME = "/path/to/java/home")
```

3. Check DatabaseConnector configuration:
```r
DatabaseConnector::checkDrivers()
```

### Package Installation Failures

**GitHub rate limit:**
```r
# Use personal access token
usethis::create_github_token()
gitcreds::gitcreds_set()
```

**Binary vs Source:**
```r
# Force source installation if binaries fail
install.packages("package_name", type = "source")
```

**Missing system dependencies (Linux):**
```bash
# For xml2 (dependency of many packages)
sudo apt-get install libxml2-dev

# For curl
sudo apt-get install libcurl4-openssl-dev

# For openssl
sudo apt-get install libssl-dev
```

### JDBC Driver Issues
If JDBC drivers fail to download:

1. Create directory manually:
```r
dir.create("inst/jdbc", recursive = TRUE)
```

2. Set environment variable:
```r
Sys.setenv(DATABASECONNECTOR_JAR_FOLDER = "inst/jdbc")
```

3. Download specific driver:
```r
DatabaseConnector::downloadJdbcDrivers(
  dbms = "sql server",
  pathToDriver = "inst/jdbc"
)
```

## Verifying Installation

Run this check to verify all dependencies are installed:

```r
check_dependencies <- function() {
  required <- c("DBI", "dplyr", "dbplyr", "tidyr", "readr", 
                "lubridate", "rlang", "tibble", "purrr", 
                "here", "readxl", "yaml", 
                "DatabaseConnector", "SqlRender")
  
  missing <- required[!sapply(required, requireNamespace, quietly = TRUE)]
  
  if (length(missing) == 0) {
    cat("✅ All required packages are installed!\n")
    return(TRUE)
  } else {
    cat("❌ Missing packages:\n")
    cat(paste("  -", missing, "\n"))
    return(FALSE)
  }
}

check_dependencies()
```

## Database Configuration

After installation, configure your database connection:

1. Copy `.Renv.example` to `.Renv`
2. Edit `.Renv` with your database credentials
3. Load configuration:
```r
source("R/load_env.R")
load_env()
connectionDetails <- get_connection_from_env()
```

## Getting Help

- **GitHub Issues:** https://github.com/ultraub/OMOP-Pregnancy/issues
- **OHDSI Forums:** https://forums.ohdsi.org/
- **DatabaseConnector Docs:** https://ohdsi.github.io/DatabaseConnector/