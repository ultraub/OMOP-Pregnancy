# Installation Guide

## Prerequisites

1. R version 4.0.0 or higher
2. Java 8 or higher (for DatabaseConnector)
3. JDBC driver for your database

## Install from GitHub

```r
# Install devtools if not already installed
install.packages("devtools")

# Install OMOPPregnancyV2
devtools::install_github("ultraub/OMOP-Pregnancy")
```

## Install from Source

```bash
# Clone the repository
git clone https://github.com/ultraub/OMOP-Pregnancy.git
cd OMOP-Pregnancy

# Build and install
R CMD build .
R CMD INSTALL OMOPPregnancyV2_2.0.0.tar.gz
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
  connectionString = Sys.getenv("DATABRICKS_TOKEN"),
  user = "",
  password = ""
)
```

## Quick Test

```r
library(OMOPPregnancyV2)
library(DatabaseConnector)

# Connect to your database
connection <- connect(connectionDetails)

# Test with a small cohort
episodes <- run_pregnancy_identification(
  connection = connection,
  cdm_schema = "your_cdm_schema",
  min_age = 15,
  max_age = 56,
  output_folder = "test_output/"
)

# Check results
print(paste("Found", nrow(episodes), "pregnancy episodes"))

# Disconnect
disconnect(connection)
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