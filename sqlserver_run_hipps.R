# ============================================
# OMOP Pregnancy Project - SQL Server Setup
# Fixed version with all SQL Server compatibility fixes
# ============================================

# Install the package from GitHub (with all recent fixes)
# Make sure you have the latest version with the day_diff fix
devtools::install_github("ultraub/OMOP-Pregnancy")

# Load required libraries
library(OMOPPregnancy)
library(DatabaseConnector)
library(dplyr)
library(dbplyr)
library(magrittr)  # For the pipe operator %>%
library(DBI)

# ============================================
# CONNECTION SETUP
# ============================================

# Create connection details for SQL Server
# Note: Adjust the pathToDriver to match your JDBC driver location
connectionDetails <- createConnectionDetails(
  dbms = "sql server",
  server = "Esmpmdbpr4.esm.johnshopkins.edu",
  pathToDriver = "./",  # Path to SQL Server JDBC driver
  user = Sys.getenv("SQL_SERVER_USER"),      # Set these in your environment
  password = Sys.getenv("SQL_SERVER_PASSWORD")  # for security
)

# Define database schemas
cdmDatabaseSchema <- "CAMP_OMOP_PROJECTION.dbo"
resultsDatabaseSchema <- "CAMP_OMOP_SCRATCH.dbo"

# ============================================
# CREATE CONNECTION
# ============================================

message("Creating connection to SQL Server...")

con <- tryCatch({
  create_connection(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    resultsDatabaseSchema = resultsDatabaseSchema
  )
}, error = function(e) {
  message("Connection failed: ", e$message)
  stop("Unable to connect to SQL Server. Check your connection details.")
})

# Test the connection
message("Testing connection...")
test_result <- tryCatch({
  DatabaseConnector::querySql(con, "SELECT 1 as test")
}, error = function(e) {
  message("Connection test failed: ", e$message)
  NULL
})

if (!is.null(test_result)) {
  message("✓ Connection successful!")
  
  # Test CDM tables are accessible
  message("Verifying CDM tables...")
  
  tables_to_check <- c("person", "observation", "measurement", 
                       "condition_occurrence", "procedure_occurrence", 
                       "visit_occurrence", "concept")
  
  for (table in tables_to_check) {
    full_table <- paste(cdmDatabaseSchema, table, sep = ".")
    check_sql <- paste0("SELECT TOP 1 * FROM ", full_table)
    
    table_check <- tryCatch({
      DatabaseConnector::querySql(con, check_sql)
      TRUE
    }, error = function(e) {
      FALSE
    })
    
    if (table_check) {
      message(paste("  ✓", table, "table accessible"))
    } else {
      message(paste("  ✗", table, "table NOT accessible"))
    }
  }
} else {
  stop("Connection test failed. Please check your connection settings.")
}

# ============================================
# RUN HIPPS ALGORITHM
# ============================================

message("\n========================================")
message("Starting HIPPS Algorithm")
message("========================================")
message(paste("CDM Schema:", cdmDatabaseSchema))
message(paste("Results Schema:", resultsDatabaseSchema))
message("========================================\n")

# Run with comprehensive error handling
results <- tryCatch({
  
  # Set a longer timeout for SQL Server operations
  options(DatabaseConnector.queryTimeout = 3600)  # 1 hour timeout
  
  run_hipps(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    resultsDatabaseSchema = resultsDatabaseSchema,
    outputFolder = "output/",
    saveIntermediateResults = TRUE  # Save intermediate results for debugging
  )
  
}, error = function(e) {
  message("\n========================================")
  message("ERROR: HIPPS algorithm failed")
  message("========================================")
  message("Error message: ", e$message)
  
  # Check for specific SQL Server errors
  if (grepl("Invalid column name", e$message)) {
    message("\n=== Column Reference Error Detected ===")
    message("This usually indicates a dbplyr SQL generation issue.")
    message("The fix for 'day_diff' error has been applied.")
    message("If you see a different column name, please report it.")
  }
  
  if (grepl("timeout", e$message, ignore.case = TRUE)) {
    message("\n=== Timeout Error Detected ===")
    message("The query took too long to execute.")
    message("Try increasing the timeout with:")
    message("  options(DatabaseConnector.queryTimeout = 7200)")
  }
  
  if (grepl("permission", e$message, ignore.case = TRUE)) {
    message("\n=== Permission Error Detected ===")
    message("Ensure you have:")
    message("  - SELECT permission on CDM schema")
    message("  - CREATE TABLE permission on results schema")
  }
  
  # Return the error for debugging
  e
})

# ============================================
# PROCESS RESULTS
# ============================================

if (!inherits(results, "error")) {
  message("\n========================================")
  message("SUCCESS: HIPPS algorithm completed!")
  message("========================================")
  
  # Display summary statistics
  if (!is.null(results$summary)) {
    message("\nSummary Statistics:")
    message("-------------------")
    
    if (!is.null(results$summary$n_episodes)) {
      message(paste("Total episodes:", results$summary$n_episodes))
    }
    
    if (!is.null(results$summary$n_persons)) {
      message(paste("Unique persons:", results$summary$n_persons))
    }
    
    if (!is.null(results$summary$episodes_by_outcome)) {
      message("\nEpisodes by outcome:")
      print(results$summary$episodes_by_outcome)
    }
    
    if (!is.null(results$summary$episodes_per_person)) {
      message("\nEpisodes per person:")
      print(results$summary$episodes_per_person)
    }
  }
  
  # Runtime information
  if (!is.null(results$runtime)) {
    message(paste("\nTotal runtime:", round(results$runtime, 2), "minutes"))
  }
  
  message("\nResults saved to: output/")
  
  # Optional: Generate comparison report
  if (exists("generate_report")) {
    message("\nGenerating comparison report...")
    tryCatch({
      generate_report(results, outputFolder = "output/")
      message("Report generated successfully!")
    }, error = function(e) {
      message("Report generation failed: ", e$message)
    })
  }
  
} else {
  message("\n========================================")
  message("FAILED: Algorithm did not complete")
  message("========================================")
  message("Check the error messages above for details.")
  message("\nTroubleshooting steps:")
  message("1. Verify your connection details are correct")
  message("2. Check that all CDM tables are accessible")
  message("3. Ensure you have necessary permissions")
  message("4. Check available disk space for temp tables")
  message("5. Review the specific error message above")
}

# ============================================
# CLEANUP
# ============================================

# Close the connection
if (exists("con") && !is.null(con)) {
  message("\nClosing database connection...")
  DatabaseConnector::disconnect(con)
  message("Connection closed.")
}

message("\n========================================")
message("Script execution complete")
message("========================================")