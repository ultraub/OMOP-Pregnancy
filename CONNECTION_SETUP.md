# Database Connection Setup Guide

This guide explains how to configure database connections for the OMOP Pregnancy Package V2.

## Quick Start

1. **Install JDBC Drivers**
   ```r
   source("inst/scripts/setup_jdbc_drivers.R")
   ```

2. **Copy appropriate .env template**
   ```bash
   # For SQL Server with Windows AD:
   cp inst/templates/.env.sqlserver_windows .env
   
   # For SQL Server with standard auth:
   cp inst/templates/.env.sqlserver_standard .env
   
   # For Databricks:
   cp inst/templates/.env.databricks .env
   ```

3. **Edit .env file with your settings**

4. **Run the analysis**
   ```r
   source("inst/scripts/run_pregnancy_analysis.R")
   ```

## Supported Database Platforms

### SQL Server with Windows AD Authentication

Perfect for enterprise environments using Active Directory.

**Requirements:**
- SQL Server JDBC driver with Windows authentication support
- Windows authentication DLL in system PATH
- R/RStudio running with domain credentials

**.env Configuration:**
```bash
DB_TYPE=sql server
DB_SERVER=your-server.domain.com
DB_DATABASE=OMOP_CDM
USE_WINDOWS_AUTH=true
CDM_SCHEMA=dbo
```

**No username/password needed** - uses your Windows credentials automatically.

### SQL Server with Standard Authentication

Traditional username/password authentication.

**.env Configuration:**
```bash
DB_TYPE=sql server
DB_SERVER=your-server.domain.com
DB_DATABASE=OMOP_CDM
USE_WINDOWS_AUTH=false
DB_USER=your_username
DB_PASSWORD=your_password
CDM_SCHEMA=dbo
```

### Databricks

Optimized for Databricks with Apache Arrow support.

**Requirements:**
- Databricks JDBC driver (download manually)
- Personal access token from Databricks workspace

**.env Configuration:**
```bash
DB_TYPE=databricks
DB_SERVER=your-workspace.cloud.databricks.com
DB_DATABASE=default
DB_USER=token
DB_PASSWORD=your-databricks-token
DB_EXTRA_SETTINGS=HTTPPath=/sql/1.0/warehouses/your-warehouse-id
CDM_SCHEMA=omop.data
VOCABULARY_SCHEMA=omop.vocabulary
RESULTS_SCHEMA=your_project.results
```

### PostgreSQL

Standard PostgreSQL connection.

**.env Configuration:**
```bash
DB_TYPE=postgresql
DB_SERVER=localhost
DB_DATABASE=omop_cdm
DB_USER=postgres
DB_PASSWORD=your_password
CDM_SCHEMA=public
```

## Advanced Configuration

### Custom Connection Strings

For complex scenarios, you can use the connection functions directly:

```r
# Load connection functions
source("R/00_connection/create_connection.R")

# Windows AD with specific domain
con <- create_omop_connection(
  dbms = "sql server",
  server = "server.domain.com",
  database = "OMOP_CDM",
  use_windows_auth = TRUE,
  cdm_schema = "dbo"
)

# Databricks with custom settings
con <- create_omop_connection(
  dbms = "databricks",
  connectionString = "jdbc:databricks://...",
  pathToDriver = "path/to/drivers",
  cdm_schema = "omop.data"
)
```

### Environment Variables

All settings can be provided via environment variables:

```r
Sys.setenv(
  DB_TYPE = "sql server",
  DB_SERVER = "server.domain.com",
  DB_DATABASE = "OMOP_CDM",
  USE_WINDOWS_AUTH = "true",
  CDM_SCHEMA = "dbo"
)

con <- create_connection_from_env()
```

### Performance Tuning

#### Databricks
- Set `ENABLE_ARROW=TRUE` for columnar data transfer (3x faster)
- Configure JVM memory: `-Xmx8g` for large datasets
- Use warehouse compute for better performance

#### SQL Server
- Adjust timeout for long queries: `DB_TIMEOUT=600`
- Use temp tables for large cohorts: automatically enabled
- Consider indexed views for frequently accessed data

## Troubleshooting

### Windows AD Authentication Issues

1. **"Login failed for user"**
   - Ensure R/RStudio is running with domain credentials
   - Check SQL Server has Windows authentication enabled
   - Verify domain trust relationships

2. **"Cannot find authentication DLL"**
   - Download: [Microsoft JDBC Auth Library](https://docs.microsoft.com/en-us/sql/connect/jdbc/)
   - Add to PATH: `mssql-jdbc_auth-<version>-x64.dll`
   - Restart R/RStudio

### Databricks Connection Issues

1. **"Arrow memory initialization failed"**
   - Set JVM options before loading packages:
   ```r
   options(java.parameters = c("-Xmx8g", "-XX:MaxDirectMemorySize=4g"))
   ```

2. **"Token authentication failed"**
   - Generate new token in Databricks workspace
   - Ensure token has SQL warehouse permissions
   - Check firewall/network access

### General Issues

1. **"JDBC driver not found"**
   - Run: `source("inst/scripts/setup_jdbc_drivers.R")`
   - Verify drivers in `jdbc_drivers/` folder
   - Check `JDBC_DRIVER_PATH` in .env

2. **"Schema not found"**
   - Verify schema names are correct
   - Check user has permissions
   - For Databricks, use catalog.schema format

## Security Best Practices

1. **Never commit .env files** - Added to .gitignore
2. **Use environment variables** for CI/CD pipelines
3. **Rotate tokens/passwords** regularly
4. **Use Windows AD** when possible (no passwords stored)
5. **Encrypt connections** - SSL/TLS enabled by default

## Support

For connection issues, please provide:
- Database platform and version
- Error messages
- .env configuration (without passwords)
- JDBC driver versions