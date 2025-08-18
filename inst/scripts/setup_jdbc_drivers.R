#!/usr/bin/env Rscript

#' Setup JDBC Drivers for Database Connections
#'
#' This script downloads and configures JDBC drivers for various database platforms.
#' Run this before using the package to ensure proper database connectivity.

library(DatabaseConnector)

# Create jdbc_drivers directory if it doesn't exist
jdbc_dir <- "jdbc_drivers"
if (!dir.exists(jdbc_dir)) {
  dir.create(jdbc_dir)
  message("Created jdbc_drivers directory")
}

# Function to download drivers
download_drivers <- function(dbms_list = NULL) {
  
  # Default to common drivers if none specified
  if (is.null(dbms_list)) {
    dbms_list <- c("sql server", "postgresql", "spark")
  }
  
  message("\n========================================")
  message("JDBC Driver Setup for OMOP Pregnancy")
  message("========================================\n")
  
  for (dbms in dbms_list) {
    
    message(sprintf("Setting up driver for: %s", dbms))
    
    if (dbms == "sql server") {
      # Download SQL Server JDBC driver
      message("  Downloading Microsoft SQL Server JDBC driver...")
      tryCatch({
        DatabaseConnector::downloadJdbcDrivers("sql server", pathToDriver = jdbc_dir)
        message("  ✓ SQL Server driver downloaded successfully")
      }, error = function(e) {
        message("  ✗ Failed to download SQL Server driver")
        message("    You can manually download from:")
        message("    https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server")
      })
      
    } else if (dbms == "postgresql") {
      # Download PostgreSQL JDBC driver
      message("  Downloading PostgreSQL JDBC driver...")
      tryCatch({
        DatabaseConnector::downloadJdbcDrivers("postgresql", pathToDriver = jdbc_dir)
        message("  ✓ PostgreSQL driver downloaded successfully")
      }, error = function(e) {
        message("  ✗ Failed to download PostgreSQL driver")
        message("    You can manually download from:")
        message("    https://jdbc.postgresql.org/download.html")
      })
      
    } else if (dbms %in% c("spark", "databricks")) {
      # Databricks driver must be downloaded manually
      message("  Databricks/Spark JDBC driver must be downloaded manually")
      message("  Steps:")
      message("    1. Go to: https://www.databricks.com/spark/jdbc-drivers-download")
      message("    2. Download the Databricks JDBC driver (version 2.6.x or later)")
      message("    3. Extract the ZIP file")
      message("    4. Copy all JAR files to: ", file.path(getwd(), jdbc_dir))
      message("")
      message("  Required files:")
      message("    - DatabricksJDBC42.jar (or similar)")
      message("    - All dependency JARs included in the download")
      
    } else if (dbms == "redshift") {
      # Download Redshift JDBC driver
      message("  Downloading Amazon Redshift JDBC driver...")
      tryCatch({
        DatabaseConnector::downloadJdbcDrivers("redshift", pathToDriver = jdbc_dir)
        message("  ✓ Redshift driver downloaded successfully")
      }, error = function(e) {
        message("  ✗ Failed to download Redshift driver")
        message("    You can manually download from:")
        message("    https://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html")
      })
      
    } else if (dbms == "oracle") {
      # Oracle driver must be downloaded manually due to licensing
      message("  Oracle JDBC driver must be downloaded manually due to licensing")
      message("  Steps:")
      message("    1. Go to: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html")
      message("    2. Download ojdbc8.jar (or appropriate version)")
      message("    3. Copy the JAR file to: ", file.path(getwd(), jdbc_dir))
      
    } else {
      message(sprintf("  Driver for %s not configured in this script", dbms))
    }
    
    message("")
  }
  
  message("========================================")
  message("Setup Complete")
  message("========================================\n")
  
  # List downloaded drivers
  jar_files <- list.files(jdbc_dir, pattern = "\\.jar$", full.names = FALSE)
  if (length(jar_files) > 0) {
    message("Found JDBC drivers:")
    for (jar in jar_files) {
      message("  - ", jar)
    }
  } else {
    message("No JDBC drivers found in ", jdbc_dir)
    message("Please download drivers manually as instructed above")
  }
}

# Windows-specific setup for AD authentication
setup_windows_auth <- function() {
  message("\n========================================")
  message("Windows AD Authentication Setup")
  message("========================================\n")
  
  message("For Windows AD authentication with SQL Server:")
  message("")
  message("1. Ensure you have the SQL Server JDBC driver with Windows auth support")
  message("2. Download the Microsoft JDBC Driver Authentication Library:")
  message("   https://docs.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url#Connectingintegrated")
  message("")
  message("3. The following DLL must be in your system PATH or java.library.path:")
  message("   - For 64-bit: mssql-jdbc_auth-<version>-x64.dll")
  message("   - Location: Usually in the JDBC driver package under 'auth' folder")
  message("")
  message("4. Set your .env file with:")
  message("   USE_WINDOWS_AUTH=true")
  message("   # No DB_USER or DB_PASSWORD needed")
  message("")
  message("5. Ensure R/RStudio is running with your Windows domain credentials")
}

# Interactive setup
if (interactive()) {
  cat("\nWhich database platform(s) do you need drivers for?\n")
  cat("1. SQL Server (with Windows AD support)\n")
  cat("2. SQL Server (standard authentication)\n")
  cat("3. Databricks\n")
  cat("4. PostgreSQL\n")
  cat("5. All common platforms\n")
  cat("6. Exit\n\n")
  
  choice <- readline(prompt = "Enter choice (1-6): ")
  
  if (choice == "1") {
    download_drivers("sql server")
    setup_windows_auth()
  } else if (choice == "2") {
    download_drivers("sql server")
  } else if (choice == "3") {
    download_drivers("spark")
  } else if (choice == "4") {
    download_drivers("postgresql")
  } else if (choice == "5") {
    download_drivers(c("sql server", "postgresql", "spark"))
  } else if (choice == "6") {
    message("Setup cancelled")
  } else {
    message("Invalid choice")
  }
} else {
  # Non-interactive mode: download common drivers
  download_drivers(c("sql server", "postgresql"))
}