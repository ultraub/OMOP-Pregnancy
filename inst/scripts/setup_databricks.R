#!/usr/bin/env Rscript

#' Setup Databricks Environment for OMOP Pregnancy
#'
#' This script configures the R environment for Databricks connections,
#' including Java setup, rJava initialization, and optional Arrow support.
#'
#' @usage source("inst/scripts/setup_databricks.R")

# ============================================
# DATABRICKS ENVIRONMENT SETUP
# ============================================

setup_databricks <- function(enable_arrow = FALSE, verbose = TRUE) {
  
  if (verbose) {
    cat("========================================\n")
    cat("Databricks Environment Setup\n")
    cat("========================================\n\n")
  }
  
  # Step 1: Check JAVA_HOME
  # --------------------------------
  if (verbose) cat("Checking Java configuration...\n")
  
  java_home <- Sys.getenv("JAVA_HOME")
  if (java_home == "") {
    stop("JAVA_HOME is not set. Please set it in your .env file or environment.\n",
         "Examples:\n",
         "  macOS Homebrew: JAVA_HOME=/opt/homebrew/opt/openjdk@17\n",
         "  Linux: JAVA_HOME=/usr/lib/jvm/java-17-openjdk\n",
         "  Windows: JAVA_HOME=C:/Program Files/Java/jdk-17")
  }
  
  if (!dir.exists(java_home)) {
    stop(sprintf("JAVA_HOME directory does not exist: %s\n", java_home),
         "Please check your JAVA_HOME setting.")
  }
  
  if (verbose) cat(sprintf("  ✓ JAVA_HOME: %s\n", java_home))
  
  # Step 2: Configure JVM parameters
  # ---------------------------------
  if (verbose) cat("\nConfiguring JVM parameters...\n")
  
  if (enable_arrow) {
    # Full Arrow-optimized configuration
    jvm_params <- c(
      "-Xmx8g",                                          # 8GB heap for large datasets
      "-XX:MaxDirectMemorySize=4g",                      # Direct memory for Arrow buffers
      "--add-opens=java.base/java.nio=ALL-UNNAMED",      # Java 17 module access
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",    # Additional module access
      "-Dio.netty.tryReflectionSetAccessible=true",      # Netty reflection for Arrow
      "-Dio.netty.allocator.type=unpooled"              # Avoid pooled memory issues
    )
    if (verbose) cat("  ✓ Arrow-optimized JVM settings (8GB heap)\n")
  } else {
    # Standard configuration without Arrow
    jvm_params <- c(
      "-Xmx4g",                                          # 4GB heap is usually sufficient
      "--add-opens=java.base/java.nio=ALL-UNNAMED"       # Basic Java 17 module access
    )
    if (verbose) cat("  ✓ Standard JVM settings (4GB heap)\n")
  }
  
  options(java.parameters = jvm_params)
  
  # Step 3: Initialize rJava
  # ------------------------
  if (verbose) cat("\nInitializing rJava...\n")
  
  if (!require("rJava", quietly = TRUE)) {
    if (verbose) cat("  Installing rJava package...\n")
    install.packages("rJava")
  }
  
  library(rJava)
  .jinit()
  
  # Verify Java version
  java_version <- .jcall("java/lang/System", "S", "getProperty", "java.version")
  if (verbose) cat(sprintf("  ✓ rJava initialized (Java %s)\n", java_version))
  
  # Step 4: Optional Arrow R package
  # --------------------------------
  if (enable_arrow) {
    if (verbose) cat("\nSetting up Arrow R package...\n")
    
    if (!require("arrow", quietly = TRUE)) {
      if (verbose) cat("  Installing Arrow R package...\n")
      install.packages("arrow", quiet = !verbose)
    }
    
    library(arrow)
    if (verbose) cat("  ✓ Arrow R package loaded\n")
    
    # Set environment variable to track Arrow status
    Sys.setenv(ENABLE_ARROW = "TRUE")
  } else {
    Sys.setenv(ENABLE_ARROW = "FALSE")
    if (verbose) cat("\n  ℹ Arrow optimization disabled (use enable_arrow=TRUE to enable)\n")
  }
  
  # Step 5: Configure Spark/Databricks options
  # ------------------------------------------
  if (verbose) cat("\nConfiguring Spark/Databricks options...\n")
  
  options(
    dbplyr.compute.defaults = list(temporary = FALSE),  # No # prefix for temp tables
    dbplyr.temp_prefix = "temp_",                       # Use temp_ prefix instead
    sparklyr.arrow = enable_arrow                       # Match Arrow setting
  )
  
  if (verbose) cat("  ✓ Spark options configured\n")
  
  # Step 6: Check JDBC drivers
  # --------------------------
  if (verbose) cat("\nChecking JDBC drivers...\n")
  
  jdbc_paths <- c(
    "jdbc_drivers",
    "inst/jdbc",
    file.path(getwd(), "jdbc_drivers")
  )
  
  jdbc_found <- FALSE
  for (path in jdbc_paths) {
    if (dir.exists(path)) {
      jar_files <- list.files(path, pattern = "\\.jar$", full.names = FALSE)
      if (length(jar_files) > 0) {
        if (verbose) {
          cat(sprintf("  ✓ Found %d JAR files in %s\n", length(jar_files), path))
          if (any(grepl("databricks|spark", jar_files, ignore.case = TRUE))) {
            cat("  ✓ Databricks/Spark driver detected\n")
          }
        }
        jdbc_found <- TRUE
        break
      }
    }
  }
  
  if (!jdbc_found) {
    if (verbose) {
      cat("  ⚠ No JDBC drivers found\n")
      cat("  Download from: https://www.databricks.com/spark/jdbc-drivers-download\n")
    }
    warning("JDBC drivers not found. Download from Databricks website.")
  }
  
  # Summary
  # -------
  if (verbose) {
    cat("\n========================================\n")
    cat("Setup Complete\n")
    cat("========================================\n")
    cat(sprintf("  JAVA_HOME: %s\n", Sys.getenv("JAVA_HOME")))
    cat(sprintf("  Java Version: %s\n", java_version))
    cat(sprintf("  Arrow Enabled: %s\n", enable_arrow))
    cat(sprintf("  Heap Size: %s\n", ifelse(enable_arrow, "8GB", "4GB")))
    cat("\nEnvironment ready for Databricks connection.\n")
  }
  
  invisible(list(
    java_home = Sys.getenv("JAVA_HOME"),
    java_version = java_version,
    arrow_enabled = enable_arrow,
    jdbc_found = jdbc_found
  ))
}

# Helper function to test Databricks connection
test_databricks_connection <- function() {
  
  cat("\n========================================\n")
  cat("Testing Databricks Connection\n")
  cat("========================================\n\n")
  
  # Check required environment variables
  required_vars <- c("DATABRICKS_SERVER", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")
  missing_vars <- character(0)
  
  for (var in required_vars) {
    if (Sys.getenv(var) == "") {
      missing_vars <- c(missing_vars, var)
    }
  }
  
  if (length(missing_vars) > 0) {
    cat("✗ Missing required environment variables:\n")
    for (var in missing_vars) {
      cat(sprintf("  - %s\n", var))
    }
    cat("\nSet these in your .env file or environment.\n")
    return(invisible(FALSE))
  }
  
  cat("✓ All required environment variables found\n\n")
  
  # Try to create connection
  cat("Creating connection...\n")
  
  tryCatch({
    library(DatabaseConnector)
    
    jdbc_url <- paste0(
      "jdbc:databricks://", Sys.getenv("DATABRICKS_SERVER"), ":443;",
      "httpPath=", Sys.getenv("DATABRICKS_HTTP_PATH"), ";",
      "AuthMech=3;",
      "UseNativeQuery=0;",
      "EnableArrow=", ifelse(Sys.getenv("ENABLE_ARROW") == "TRUE", "1", "0"), ";",
      "UID=token;",
      "PWD=", Sys.getenv("DATABRICKS_TOKEN")
    )
    
    connectionDetails <- createConnectionDetails(
      dbms = "spark",
      connectionString = jdbc_url,
      pathToDriver = "jdbc_drivers"
    )
    
    connection <- connect(connectionDetails)
    
    # Test query
    test_result <- querySql(connection, "SELECT 1 as test")
    
    if (!is.null(test_result)) {
      cat("✓ Connection successful!\n")
      cat("✓ Test query executed successfully\n")
      
      disconnect(connection)
      return(invisible(TRUE))
    } else {
      cat("✗ Test query failed\n")
      return(invisible(FALSE))
    }
    
  }, error = function(e) {
    cat("✗ Connection failed\n")
    cat(sprintf("Error: %s\n", e$message))
    return(invisible(FALSE))
  })
}

# Run setup if called directly
if (!interactive()) {
  # Parse command line arguments
  args <- commandArgs(trailingOnly = TRUE)
  enable_arrow <- "--arrow" %in% args || "--enable-arrow" %in% args
  
  setup_databricks(enable_arrow = enable_arrow, verbose = TRUE)
  
  if ("--test" %in% args) {
    test_databricks_connection()
  }
} else {
  # In interactive mode, just define the functions
  cat("Databricks setup functions loaded.\n")
  cat("Usage:\n")
  cat("  setup_databricks()              # Standard setup\n")
  cat("  setup_databricks(enable_arrow = TRUE)  # With Arrow optimization\n")
  cat("  test_databricks_connection()    # Test the connection\n")
}