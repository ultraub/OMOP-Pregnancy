# ============================================
# OMOP Pregnancy Project - Databricks Setup
# Fixed version with Arrow memory solutions
# ============================================

# CRITICAL: Set JVM parameters BEFORE loading any packages
# This configures the JVM for Apache Arrow memory management
options(java.parameters = c(
  "-Xmx4g",                                         # Heap memory
  "-XX:MaxDirectMemorySize=2g",                      # Direct memory for Arrow
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",    # Java 17 module access
  "-Dio.netty.tryReflectionSetAccessible=true"      # Netty reflection for Arrow
))

# Initialize rJava with the configured JVM settings
library(rJava)
.jinit()

# Install the package from GitHub (with all recent fixes)
devtools::install_github("ultraub/OMOP-Pregnancy")

# Load required libraries
library(OMOPPregnancy)
library(DatabaseConnector)
library(dplyr)
library(dbplyr)
library(magrittr)  # For the pipe operator %>%
library(DBI)

# Optional: Install Arrow R package for native library support
# This can help with Arrow initialization issues
if (!require("arrow", quietly = TRUE)) {
  install.packages("arrow")
  library(arrow)
}

# Set JAVA_HOME to your JDK 17 installation
Sys.setenv(JAVA_HOME="C:/Program Files/Microsoft/jdk-17.0.11.9-hotspot")

# Download JDBC driver for Databricks
# Download JDBC driver at: https://www.databricks.com/spark/jdbc-drivers-download
pathToDriver <- "C:/Program Files/DatabricksJDBC42-2.6.38.1068"

# Create JDBC connection URL with Arrow optimization disabled
# This helps avoid Arrow memory initialization issues
jdbc_url <- paste0(
  "jdbc:databricks://adb-4068548029743470.10.azuredatabricks.net:443;",
  "httpPath=/sql/1.0/warehouses/ba264cbc36eb86d3;",
  "UseNativeQuery=0;",     # Disable native query optimization
  "EnableArrow=0;",         # Disable Arrow columnar transfer
  "LowLatency=0"           # Disable low latency mode that uses Arrow
)

# Create connection details using the optimized JDBC URL
connectionDetails <- createConnectionDetails(
  dbms = "spark",
  connectionString = jdbc_url,
  pathToDriver = pathToDriver,
  user = "token",           # Use "token" as username for Databricks
  password = Sys.getenv("DATABRICKS_TOKEN")  # Store token in environment variable
)

# Define database schemas
cdmDatabaseSchema = "omop.data"
resultsDatabaseSchema = "rit_projects.preeclampsia_irb00501176"

# Create the connection with error handling
con <- tryCatch({
  create_connection(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    resultsDatabaseSchema = resultsDatabaseSchema
  )
}, error = function(e) {
  message("Connection failed. Checking Arrow/JDBC configuration...")
  message("Error: ", e$message)
  
  # Try alternative connection without some optimizations
  jdbc_url_fallback <- paste0(
    "jdbc:databricks://adb-4068548029743470.10.azuredatabricks.net:443;",
    "httpPath=/sql/1.0/warehouses/ba264cbc36eb86d3"
  )
  
  connectionDetails_fallback <- createConnectionDetails(
    dbms = "spark",
    connectionString = jdbc_url_fallback,
    pathToDriver = pathToDriver,
    user = "token",
    password = Sys.getenv("DATABRICKS_TOKEN")
  )
  
  create_connection(
    connectionDetails = connectionDetails_fallback,
    cdmDatabaseSchema = cdmDatabaseSchema,
    resultsDatabaseSchema = resultsDatabaseSchema
  )
})

# Test the connection
message("Testing connection...")
test_result <- tryCatch({
  DatabaseConnector::querySql(con, "SELECT 1 as test")
}, error = function(e) {
  message("Simple query test failed: ", e$message)
  NULL
})

if (!is.null(test_result)) {
  message("Connection successful!")
} else {
  message("Warning: Connection established but queries may fail")
}

# Run the HIPPS algorithm with additional error handling
results <- tryCatch({
  run_hipps(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    resultsDatabaseSchema = resultsDatabaseSchema,
    outputFolder = "output/",
    saveIntermediateResults = TRUE  # Save intermediate results for debugging
  )
}, error = function(e) {
  message("HIPPS algorithm failed: ", e$message)
  
  # If it fails due to Arrow issues, provide diagnostic information
  if (grepl("Arrow|MemoryUtil", e$message)) {
    message("\n=== Arrow Memory Error Detected ===")
    message("This is a known issue with Databricks JDBC and Apache Arrow.")
    message("Solutions:")
    message("1. Ensure JVM parameters are set at the very beginning of the script")
    message("2. Try restarting your R session and running the script again")
    message("3. Consider using Databricks notebooks instead of remote JDBC")
    message("4. Contact your Databricks admin to check server-side Arrow configuration")
  }
  
  # Return error for debugging
  e
})

# If successful, display summary
if (!inherits(results, "error")) {
  message("HIPPS algorithm completed successfully!")
  message("Results saved to: output/")
  
  # Display summary statistics if available
  if (!is.null(results$summary)) {
    print(results$summary)
  }
} else {
  message("Algorithm failed. Check the error message above for details.")
}

# Clean up connection
if (exists("con") && !is.null(con)) {
  DatabaseConnector::disconnect(con)
  message("Connection closed.")
}