#!/usr/bin/env Rscript

# Debug script to troubleshoot table access issues

library(DatabaseConnector)

cat("========================================\n")
cat("DEBUG: Connection and Table Access\n")
cat("========================================\n\n")

# Load environment variables from .env file if it exists
if (file.exists(".env")) {
  cat("Loading environment from .env file...\n")
  env_lines <- readLines(".env", warn = FALSE)
  for (line in env_lines) {
    line <- trimws(line)
    if (nchar(line) > 0 && !startsWith(line, "#")) {
      parts <- strsplit(line, "=", fixed = TRUE)[[1]]
      if (length(parts) >= 2) {
        key <- trimws(parts[1])
        value <- trimws(paste(parts[-1], collapse = "="))
        do.call(Sys.setenv, setNames(list(value), key))
        # Debug: show what we loaded (hide passwords)
        if (!grepl("PASSWORD|TOKEN", key)) {
          cat(sprintf("  Loaded: %s = %s\n", key, value))
        } else {
          cat(sprintf("  Loaded: %s = ***\n", key))
        }
      }
    }
  }
  cat("\n")
}

# Show all SQL-related environment variables
cat("Environment variables:\n")
env_vars <- Sys.getenv()
sql_vars <- env_vars[grep("^SQL_|^DB_", names(env_vars))]
for (name in names(sql_vars)) {
  if (!grepl("PASSWORD", name)) {
    cat(sprintf("  %s = %s\n", name, sql_vars[name]))
  } else {
    cat(sprintf("  %s = ***\n", name))
  }
}
cat("\n")

# SQL Server configuration
cat("Configuring SQL Server connection...\n")

server <- Sys.getenv("SQL_SERVER")
database <- Sys.getenv("SQL_DATABASE")
user <- Sys.getenv("SQL_USER")
password <- Sys.getenv("SQL_PASSWORD")
port <- as.numeric(Sys.getenv("SQL_PORT", "1433"))
pathToDriver <- Sys.getenv("SQL_JDBC_PATH", "./jdbc_drivers")

cat(sprintf("Server: %s\n", server))
cat(sprintf("Database: %s\n", database))
cat(sprintf("User: %s\n", if(user == "") "EMPTY!" else user))
cat(sprintf("Port: %d\n", port))
cat("\n")

# Check authentication method
if (user == "" || password == "") {
  cat("ISSUE: No credentials found - script will try Windows AD auth\n")
  cat("This will fail on Azure SQL Database!\n\n")
  
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
  cat("Using SQL Server authentication\n\n")
  
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

# Connect
connection <- tryCatch({
  connect(connectionDetails)
}, error = function(e) {
  cat(sprintf("Connection failed: %s\n", e$message))
  NULL
})

if (!is.null(connection)) {
  cat("✓ Connected successfully!\n\n")
  
  # Debug: What database are we actually in?
  cat("Checking current database context...\n")
  current_db <- tryCatch({
    result <- querySql(connection, "SELECT DB_NAME() as current_database")
    result$CURRENT_DATABASE[1]
  }, error = function(e) {
    "UNKNOWN"
  })
  cat(sprintf("Current database: %s\n\n", current_db))
  
  # Debug: What schemas are available?
  cat("Available schemas in current database:\n")
  schemas <- tryCatch({
    querySql(connection, "SELECT DISTINCT schema_name FROM information_schema.schemata ORDER BY schema_name")
  }, error = function(e) {
    NULL
  })
  if (!is.null(schemas)) {
    print(schemas)
  }
  cat("\n")
  
  # Debug: Look for OMOP tables in any schema
  cat("Searching for OMOP tables (person, observation, etc.)...\n")
  omop_tables <- tryCatch({
    sql <- "
      SELECT 
        table_catalog as database_name,
        table_schema as schema_name,
        table_name
      FROM information_schema.tables
      WHERE table_name IN ('person', 'observation', 'measurement', 
                           'condition_occurrence', 'procedure_occurrence',
                           'visit_occurrence', 'concept')
      ORDER BY table_catalog, table_schema, table_name
    "
    querySql(connection, sql)
  }, error = function(e) {
    cat(sprintf("Error searching for tables: %s\n", e$message))
    NULL
  })
  
  if (!is.null(omop_tables) && nrow(omop_tables) > 0) {
    cat("\nFound OMOP tables:\n")
    print(omop_tables)
    
    # Show the correct schema path to use
    if (nrow(omop_tables) > 0) {
      first_table <- omop_tables[1,]
      cat(sprintf("\n➜ Your CDM tables appear to be in: %s.%s\n", 
                  first_table$SCHEMA_NAME, first_table$TABLE_NAME))
      cat(sprintf("  Set SQL_CDM_SCHEMA=%s in your .env file\n", first_table$SCHEMA_NAME))
      
      if (first_table$DATABASE_NAME != current_db) {
        cat(sprintf("  Tables are in different database: %s\n", first_table$DATABASE_NAME))
        cat(sprintf("  Set SQL_CDM_DATABASE=%s in your .env file\n", first_table$DATABASE_NAME))
      }
    }
  } else {
    cat("\n✗ No OMOP tables found in accessible schemas\n")
    cat("Possible issues:\n")
    cat("  1. Tables are in a different database\n")
    cat("  2. Your user lacks permissions\n")
    cat("  3. Tables have different names\n")
  }
  
  # Test the schema variables that would be used
  cat("\n========================================\n")
  cat("Testing with current schema settings:\n")
  cat("========================================\n")
  
  cdm_schema <- Sys.getenv("SQL_CDM_SCHEMA", "dbo")
  cdm_database <- Sys.getenv("SQL_CDM_DATABASE", database)
  
  if (cdm_database != database) {
    cdm_schema <- paste(cdm_database, cdm_schema, sep = ".")
  }
  
  cat(sprintf("Would look for tables in: %s\n", cdm_schema))
  
  # Try to query with this schema
  test_table <- paste(cdm_schema, "person", sep = ".")
  cat(sprintf("Testing query: SELECT TOP 1 * FROM %s\n", test_table))
  
  test_result <- tryCatch({
    querySql(connection, sprintf("SELECT TOP 1 * FROM %s", test_table))
    "✓ Query successful!"
  }, error = function(e) {
    sprintf("✗ Query failed: %s", e$message)
  })
  
  cat(test_result)
  cat("\n")
  
  disconnect(connection)
  cat("\n✓ Connection closed\n")
}