# Database Connection Setup Guide

This guide explains how to configure database connections for the OMOP Pregnancy Package V2.

## Quick Start

1. **Install JDBC Drivers**
   ```r
   source("inst/scripts/setup_jdbc_drivers.R")
   ```

2. **Copy the environment template**
   ```bash
   # Copy the unified template to your project root
   cp inst/templates/.env.template .env
   ```

3. **Edit .env file with your settings**
   - Set `DB_TYPE` to your database platform
   - Fill in connection details
   - Configure schema names
   - See template comments for platform-specific settings

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
- **Arrow Optimization**: Can provide 3x faster columnar data transfer
  - Default is `ENABLE_ARROW=FALSE` to avoid initialization errors
  - To enable Arrow, you must:
    1. Download complete Databricks JDBC driver with all dependencies
    2. Set JVM options: `-Dio.netty.tryReflectionSetAccessible=true -Xmx8g`
    3. Ensure sufficient memory allocation
    4. Set `ENABLE_ARROW=TRUE` in your .env file
- Use SQL warehouse compute for better performance
- Configure appropriate warehouse size for your workload

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

1. **"Could not initialize class com.databricks.client.jdbc42.internal.apache.arrow.memory.util.MemoryUtil"**
   - This error occurs when Arrow optimization is enabled but environment isn't configured
   - **Quick Fix**: Set `ENABLE_ARROW=FALSE` in your .env file (default)
   - **To Enable Arrow** (for better performance):
     ```r
     # Set JVM options BEFORE loading any packages
     options(java.parameters = c(
       "-Xmx8g",
       "-XX:MaxDirectMemorySize=4g",
       "-Dio.netty.tryReflectionSetAccessible=true"
     ))
     ```
     Then ensure you have the complete Databricks JDBC driver with all Arrow dependencies

2. **"Token authentication failed"**
   - Generate new token in Databricks workspace
   - Ensure token has SQL warehouse permissions
   - Check firewall/network access

3. **"HTTP Path parsing error"**
   - Ensure httpPath uses lowercase 'h' (not HTTPPath)
   - Format: `DB_EXTRA_SETTINGS=httpPath=/sql/1.0/warehouses/your-warehouse-id`
   - Do not include quotes or extra semicolons

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