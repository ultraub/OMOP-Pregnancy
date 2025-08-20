# Installation Guide

## Prerequisites

1. R version 4.0.0 or higher
2. Java 8 or higher (for DatabaseConnector)
3. JDBC driver for your database

## Install Dependencies

```r
# Install required packages
install.packages(c(
  "DatabaseConnector",
  "SqlRender", 
  "dplyr",
  "lubridate",
  "readr",
  "DBI"
))

# For development/testing
install.packages(c("testthat", "knitr", "rmarkdown", "tidyr", "tibble"))
```

## Setup Instructions

```bash
# 1. Clone or download the repository
git clone https://github.com/ultraub/OMOP-Pregnancy.git
cd OMOPPregnancyV2

# 2. Setup environment configuration
cp inst/templates/.env.template .env
# Edit .env with your database settings

# 3. Install JDBC drivers (from R)
Rscript -e "source('inst/scripts/setup_jdbc_drivers.R')"

# 4. Test your setup (from R)
Rscript -e "source('inst/scripts/test_connection.R')"
```

## Database Setup

### Quick Configuration with .env File

The easiest way to configure your database connection:

1. Copy the environment template:
   ```bash
   cp inst/templates/.env.template .env
   ```

2. Edit `.env` with your database settings

3. Use the environment-based connection:
   ```r
   source("R/00_connection/create_connection.R")
   con <- create_connection_from_env()
   ```

### Manual Configuration

### SQL Server
```r
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "sql server",
  connectionString = "jdbc:sqlserver://server:port;database=db;encrypt=true;trustServerCertificate=true;",
  user = "username",
  password = "password",
  pathToDriver = "path/to/jdbc/drivers"
)
```

### PostgreSQL
```r
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  server = "server/database",
  user = "username",
  password = "password",
  port = 5432
)
```

### Databricks
```r
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "spark",
  connectionString = "jdbc:databricks://workspace.cloud.databricks.com:443;httpPath=/sql/1.0/warehouses/warehouse_id;AuthMech=3;UID=token;PWD=your_token",
  pathToDriver = "jdbc_drivers"
)
```

## Quick Test

```r
# Using environment-based configuration (recommended)
source("R/00_connection/create_connection.R")
source("R/main.R")

# Connect using .env file
connection <- create_connection_from_env()

# Test with a small cohort
episodes <- run_pregnancy_identification(
  connection = connection,
  cdm_database_schema = "your_cdm_schema",
  results_database_schema = "your_results_schema",  # optional
  min_age = 15,
  max_age = 56,
  output_folder = "test_output/"
)

# Check results
print(paste("Found", nrow(episodes), "pregnancy episodes"))

# Disconnect
DatabaseConnector::disconnect(connection)
```

## Troubleshooting

### Java Issues
If you get Java-related errors:
```r
# Check Java configuration
Sys.getenv("JAVA_HOME")

# Set Java home if needed
Sys.setenv(JAVA_HOME = "/path/to/java/home")
```

### JDBC Driver Issues
```r
# Download JDBC driver for your database
DatabaseConnector::downloadJdbcDrivers("sql server", pathToDriver = "jdbc_drivers")
```

### Memory Issues
For large cohorts, increase Java heap size:
```r
options(java.parameters = "-Xmx8g")
```

## Support

Report issues at: https://github.com/ultraub/OMOP-Pregnancy/issues