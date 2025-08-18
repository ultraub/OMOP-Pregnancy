#!/usr/bin/env Rscript

# ============================================
# OMOP Pregnancy V2 - Connection Test Script
# Tests database connection and table access
# ============================================

cat("========================================\n")
cat("OMOP Pregnancy V2 - Connection Test\n")
cat("========================================\n\n")

# Load required libraries
library(DatabaseConnector)
library(SqlRender)

# Load environment variables from .env file if it exists
if (file.exists(".env")) {
  cat("Loading environment from .env file...\n")
  env_lines <- readLines(".env")
  for (line in env_lines) {
    line <- trimws(line)
    if (nchar(line) > 0 && !startsWith(line, "#")) {
      parts <- strsplit(line, "=", fixed = TRUE)[[1]]
      if (length(parts) >= 2) {
        key <- trimws(parts[1])
        value <- trimws(paste(parts[-1], collapse = "="))
        do.call(Sys.setenv, setNames(list(value), key))
      }
    }
  }
  cat("✓ Environment loaded\n\n")
}

# Determine environment
ENVIRONMENT <- Sys.getenv("OMOP_ENV", "SQLSERVER")
cat(sprintf("Testing %s connection...\n\n", ENVIRONMENT))

# Configure connection based on environment
if (ENVIRONMENT == "DATABRICKS") {
  
  # Databricks configuration
  cat("Configuring Databricks connection...\n")
  
  # Set JVM parameters
  options(java.parameters = c(
    "-Xmx4g",
    "--add-opens=java.base/java.nio=ALL-UNNAMED"
  ))
  
  library(rJava)
  .jinit()
  
  server <- Sys.getenv("DATABRICKS_SERVER")
  httpPath <- Sys.getenv("DATABRICKS_HTTP_PATH")
  token <- Sys.getenv("DATABRICKS_TOKEN")
  pathToDriver <- Sys.getenv("DATABRICKS_JDBC_PATH", "./jdbc_drivers")
  
  if (token == "") {
    stop("DATABRICKS_TOKEN not set!")
  }
  
  jdbc_url <- paste0(
    "jdbc:databricks://", server, ":443;",
    "httpPath=", httpPath, ";",
    "AuthMech=3;",
    "UseNativeQuery=0;",
    "EnableArrow=1;",
    "UID=token;",
    "PWD=", token
  )
  
  connectionDetails <- createConnectionDetails(
    dbms = "spark",
    connectionString = jdbc_url,
    pathToDriver = pathToDriver
  )
  
  cdm_schema <- Sys.getenv("DATABRICKS_CDM_SCHEMA", "omop.data")
  cdm_catalog <- Sys.getenv("DATABRICKS_CDM_CATALOG", "")
  results_schema <- Sys.getenv("DATABRICKS_RESULTS_SCHEMA", "rit_projects.preeclampsia_irb00501176")
  results_catalog <- Sys.getenv("DATABRICKS_RESULTS_CATALOG", "")
  
  # Build full schema paths if catalogs are specified
  if (cdm_catalog != "") {
    cdm_schema <- paste(cdm_catalog, cdm_schema, sep = ".")
  }
  if (results_catalog != "") {
    results_schema <- paste(results_catalog, results_schema, sep = ".")
  }
  
} else {
  
  # SQL Server configuration
  cat("Configuring SQL Server connection...\n")
  
  server <- Sys.getenv("SQL_SERVER")
  database <- Sys.getenv("SQL_DATABASE")
  user <- Sys.getenv("SQL_USER")
  password <- Sys.getenv("SQL_PASSWORD")
  port <- as.numeric(Sys.getenv("SQL_PORT", "1433"))
  pathToDriver <- Sys.getenv("SQL_JDBC_PATH", "./jdbc_drivers")
  
  # Check if using Windows AD authentication or SQL authentication
  if (user == "" || password == "") {
    cat("Using Windows AD authentication (integrated security)\n")
    
    jdbc_url <- sprintf(
      "jdbc:sqlserver://%s:%d;database=%s;integratedSecurity=true;encrypt=true;trustServerCertificate=true;",
      server, port, database
    )
    
    connectionDetails <- createConnectionDetails(
      dbms = "sql server",
      connectionString = jdbc_url,
      pathToDriver = pathToDriver
    )
  } else {
    cat("Using SQL Server authentication\n")
    
    jdbc_url <- sprintf(
      "jdbc:sqlserver://%s:%d;database=%s;encrypt=true;trustServerCertificate=true;",
      server, port, database
    )
    
    connectionDetails <- createConnectionDetails(
      dbms = "sql server",
      connectionString = jdbc_url,
      user = user,
      password = password,
      pathToDriver = pathToDriver
    )
  }
  
  cdm_schema <- Sys.getenv("SQL_CDM_SCHEMA", "dbo")
  cdm_database <- Sys.getenv("SQL_CDM_DATABASE", database)
  results_schema <- Sys.getenv("SQL_RESULTS_SCHEMA", "dbo")
  results_database <- Sys.getenv("SQL_RESULTS_DATABASE", database)
  
  # Build full schema paths with database names if different
  if (cdm_database != database) {
    cdm_schema <- paste(cdm_database, cdm_schema, sep = ".")
  }
  if (results_database != database) {
    results_schema <- paste(results_database, results_schema, sep = ".")
  }
}

# Test connection
cat("\n========================================\n")
cat("Testing Database Connection\n")
cat("========================================\n\n")

connection <- tryCatch({
  conn <- connect(connectionDetails)
  cat("✓ Connected successfully!\n\n")
  conn
}, error = function(e) {
  cat("✗ Connection failed!\n")
  cat(sprintf("Error: %s\n", e$message))
  NULL
})

if (!is.null(connection)) {
  
  # Test basic query
  cat("Testing basic query...\n")
  test_result <- tryCatch({
    querySql(connection, "SELECT 1 as test")
  }, error = function(e) {
    NULL
  })
  
  if (!is.null(test_result)) {
    cat("✓ Basic query successful\n\n")
  } else {
    cat("✗ Basic query failed\n\n")
  }
  
  # Test CDM tables
  cat("Testing CDM table access...\n")
  cat(sprintf("CDM Schema: %s\n", cdm_schema))
  cat(sprintf("Results Schema: %s\n\n", results_schema))
  
  tables_to_check <- c(
    "person",
    "observation",
    "measurement",
    "condition_occurrence",
    "procedure_occurrence",
    "visit_occurrence",
    "concept"
  )
  
  table_counts <- list()
  accessible_tables <- 0
  
  for (table in tables_to_check) {
    full_table <- paste(cdm_schema, table, sep = ".")
    
    if (ENVIRONMENT == "DATABRICKS") {
      count_sql <- sprintf("SELECT COUNT(*) as n FROM %s LIMIT 1", full_table)
    } else {
      count_sql <- sprintf("SELECT COUNT(*) as n FROM %s", full_table)
    }
    
    result <- tryCatch({
      res <- querySql(connection, count_sql)
      count <- res$N[1]
      table_counts[[table]] <- count
      accessible_tables <- accessible_tables + 1
      cat(sprintf("  ✓ %-25s %s rows\n", 
                  paste0(table, ":"), 
                  format(count, big.mark = ",")))
      TRUE
    }, error = function(e) {
      cat(sprintf("  ✗ %-25s NOT ACCESSIBLE\n", paste0(table, ":")))
      FALSE
    })
  }
  
  # Summary
  cat("\n========================================\n")
  cat("Connection Test Summary\n")
  cat("========================================\n\n")
  
  cat(sprintf("Environment: %s\n", ENVIRONMENT))
  cat(sprintf("CDM Schema: %s\n", cdm_schema))
  cat(sprintf("Results Schema: %s\n", results_schema))
  cat(sprintf("Tables Accessible: %d/%d\n", accessible_tables, length(tables_to_check)))
  
  if (accessible_tables == length(tables_to_check)) {
    cat("\n✅ ALL TESTS PASSED! Ready to run analysis.\n")
    cat("\nRun the analysis with:\n")
    cat("  Rscript inst/scripts/run_pregnancy_analysis.R\n")
  } else {
    cat("\n⚠ Some tables are not accessible.\n")
    cat("Check your permissions and schema configuration.\n")
  }
  
  # Disconnect
  disconnect(connection)
  cat("\n✓ Connection closed\n")
  
} else {
  cat("\n========================================\n")
  cat("Connection Test Failed\n")
  cat("========================================\n\n")
  cat("Please check:\n")
  cat("  1. Environment variables are set correctly\n")
  cat("  2. JDBC drivers are in place\n")
  cat("  3. Database server is accessible\n")
  cat("  4. Credentials are correct\n")
}