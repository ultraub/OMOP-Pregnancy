#!/usr/bin/env Rscript

#' ============================================
#' Arrow Diagnostic Script for Databricks OMOP
#' ============================================
#' 
#' This script diagnoses Arrow-related connection issues with Databricks
#' Run this to determine if Arrow failures are due to storage access or other issues

cat("\n")
cat("========================================\n")
cat("   ARROW DIAGNOSTIC FOR DATABRICKS\n")
cat("========================================\n")
cat("Date:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# Track results
diagnostic_results <- list()

# ============================================
# SECTION 1: Environment Information
# ============================================
cat("1. ENVIRONMENT INFORMATION\n")
cat("--------------------------\n")

# R and Java versions
cat("R Version:", R.version.string, "\n")

# Java version
java_home <- Sys.getenv("JAVA_HOME")
if (java_home != "") {
  cat("JAVA_HOME:", java_home, "\n")
  java_version <- system2("java", "-version", stderr = TRUE, stdout = TRUE)
  cat("Java Version:", java_version[1], "\n")
} else {
  cat("⚠️  JAVA_HOME not set\n")
  diagnostic_results$java_home <- FALSE
}

# Package versions
packages <- c("DatabaseConnector", "DBI", "rJava", "httr", "curl")
cat("\nPackage Versions:\n")
for (pkg in packages) {
  if (requireNamespace(pkg, quietly = TRUE)) {
    ver <- packageVersion(pkg)
    cat(sprintf("  %s: %s\n", pkg, ver))
  } else {
    cat(sprintf("  %s: NOT INSTALLED\n", pkg))
  }
}

# Check for proxy
http_proxy <- Sys.getenv("http_proxy")
https_proxy <- Sys.getenv("https_proxy")
if (http_proxy != "" || https_proxy != "") {
  cat("\n⚠️  Proxy detected:\n")
  if (http_proxy != "") cat("  HTTP_PROXY:", http_proxy, "\n")
  if (https_proxy != "") cat("  HTTPS_PROXY:", https_proxy, "\n")
  diagnostic_results$proxy <- TRUE
} else {
  diagnostic_results$proxy <- FALSE
}

cat("\n")

# ============================================
# SECTION 2: Network Connectivity Tests
# ============================================
cat("2. NETWORK CONNECTIVITY TESTS\n")
cat("------------------------------\n")

# Function to test URL connectivity
test_url <- function(url, name, timeout_sec = 5) {
  cat(sprintf("Testing %s (%s)...\n", name, url))
  
  # Method 1: Using httr if available
  if (requireNamespace("httr", quietly = TRUE)) {
    result <- tryCatch({
      response <- httr::GET(url, httr::timeout(timeout_sec))
      list(success = TRUE, status = response$status_code, method = "httr")
    }, error = function(e) {
      list(success = FALSE, error = e$message, method = "httr")
    })
  } else {
    # Method 2: Using base R
    result <- tryCatch({
      con <- url(url)
      open(con, "rb", blocking = TRUE)
      close(con)
      list(success = TRUE, method = "base")
    }, error = function(e) {
      list(success = FALSE, error = e$message, method = "base")
    })
  }
  
  if (result$success) {
    cat(sprintf("  ✅ %s is reachable", name))
    if (!is.null(result$status)) cat(sprintf(" (status: %s)", result$status))
    cat("\n")
  } else {
    cat(sprintf("  ❌ %s is NOT reachable\n", name))
    cat(sprintf("     Error: %s\n", result$error))
  }
  
  return(result)
}

# Test general internet
internet_test <- test_url("https://www.google.com", "Internet")
diagnostic_results$internet <- internet_test$success

# Test Azure storage endpoints (adjust for your cloud provider)
cat("\nCloud Storage Endpoints:\n")
storage_endpoints <- list(
  "Azure Data Lake" = "https://dfs.core.windows.net",
  "Azure Blob" = "https://blob.core.windows.net",
  "AWS S3" = "https://s3.amazonaws.com",
  "Google Cloud" = "https://storage.googleapis.com"
)

storage_accessible <- FALSE
for (name in names(storage_endpoints)) {
  result <- test_url(storage_endpoints[[name]], name)
  if (result$success) storage_accessible <- TRUE
}

diagnostic_results$storage_accessible <- storage_accessible

# Test Databricks workspace (if configured)
databricks_server <- Sys.getenv("DB_SERVER")
if (databricks_server != "") {
  cat("\nDatabricks Workspace:\n")
  databricks_url <- sprintf("https://%s", databricks_server)
  databricks_test <- test_url(databricks_url, "Databricks Workspace")
  diagnostic_results$databricks_accessible <- databricks_test$success
}

cat("\n")

# ============================================
# SECTION 3: DNS Resolution Tests
# ============================================
cat("3. DNS RESOLUTION TESTS\n")
cat("-----------------------\n")

test_dns <- function(hostname) {
  cat(sprintf("Resolving %s...", hostname))
  result <- tryCatch({
    ip <- nsl(hostname)
    cat(sprintf(" ✅ Resolved to %s\n", ip[1]))
    TRUE
  }, error = function(e) {
    # Fallback to system nslookup
    system_result <- system2("nslookup", hostname, stdout = TRUE, stderr = TRUE)
    if (length(system_result) > 0 && !grepl("can't find", paste(system_result, collapse = " "), ignore.case = TRUE)) {
      cat(" ✅ Resolved (via system)\n")
      TRUE
    } else {
      cat(" ❌ Cannot resolve\n")
      FALSE
    }
  })
  return(result)
}

# Test DNS for storage endpoints
dns_results <- list()
dns_hosts <- c("dfs.core.windows.net", "blob.core.windows.net")
for (host in dns_hosts) {
  dns_results[[host]] <- test_dns(host)
}

diagnostic_results$dns_ok <- all(unlist(dns_results))

cat("\n")

# ============================================
# SECTION 4: Database Connection Tests
# ============================================
cat("4. DATABASE CONNECTION TESTS\n")
cat("----------------------------\n")

# Load environment
if (file.exists(".env")) {
  source("R/00_connection/create_connection.R")
  cat("Loading environment from .env...\n")
  
  # Test connection WITHOUT Arrow first
  cat("\nTest 1: Basic connection (Arrow DISABLED)\n")
  Sys.setenv(ENABLE_ARROW = "FALSE")
  
  connection_basic <- tryCatch({
    con <- create_connection_from_env()
    cat("  ✅ Connection established\n")
    
    # Test basic query
    test_result <- tryCatch({
      DatabaseConnector::querySql(con, "SELECT 1 as test")
    }, error = function(e) NULL)
    
    if (!is.null(test_result)) {
      cat("  ✅ Basic query successful\n")
      diagnostic_results$basic_connection <- TRUE
    } else {
      cat("  ❌ Basic query failed\n")
      diagnostic_results$basic_connection <- FALSE
    }
    
    DatabaseConnector::disconnect(con)
    TRUE
  }, error = function(e) {
    cat("  ❌ Connection failed:", e$message, "\n")
    diagnostic_results$basic_connection <- FALSE
    FALSE
  })
  
  # Only test Arrow if basic connection works
  if (connection_basic) {
    cat("\nTest 2: Connection with Arrow ENABLED\n")
    Sys.setenv(ENABLE_ARROW = "TRUE")
    
    # Enable verbose logging for Arrow
    Sys.setenv(DATABRICKS_LOG_LEVEL = "DEBUG")
    
    connection_arrow <- tryCatch({
      con <- create_connection_from_env()
      cat("  ✅ Connection established with Arrow\n")
      
      # Test progressively larger queries
      test_queries <- list(
        "Tiny (1 row)" = "SELECT * FROM person LIMIT 1",
        "Small (10 rows)" = "SELECT * FROM person LIMIT 10",
        "Medium (100 rows)" = "SELECT * FROM person LIMIT 100",
        "Large (1000 rows)" = "SELECT * FROM person LIMIT 1000"
      )
      
      cdm_schema <- Sys.getenv("CDM_SCHEMA", "omop.data")
      
      for (name in names(test_queries)) {
        cat(sprintf("\n  Testing %s query...\n", name))
        query <- gsub("FROM person", paste0("FROM ", cdm_schema, ".person"), test_queries[[name]])
        
        start_time <- Sys.time()
        result <- tryCatch({
          DatabaseConnector::querySql(con, query)
        }, error = function(e) {
          cat(sprintf("    ❌ Failed: %s\n", name))
          
          # Analyze error message
          error_msg <- e$message
          if (grepl("dfs\\.core\\.windows\\.net|blob\\.core\\.windows\\.net", error_msg, ignore.case = TRUE)) {
            cat("    📍 Error contains storage URLs - likely storage access issue\n")
            diagnostic_results$storage_urls_in_error <- TRUE
          }
          if (grepl("timeout|timed out", error_msg, ignore.case = TRUE)) {
            cat("    ⏱️ Timeout detected - likely network issue\n")
            diagnostic_results$timeout_error <- TRUE
          }
          if (grepl("connection reset|connection refused", error_msg, ignore.case = TRUE)) {
            cat("    🔌 Connection reset/refused - likely firewall issue\n")
            diagnostic_results$connection_reset <- TRUE
          }
          if (grepl("Arrow|MemoryUtil", error_msg, ignore.case = TRUE)) {
            cat("    🏹 Arrow-specific error detected\n")
            diagnostic_results$arrow_error <- TRUE
          }
          
          # Save full error for analysis
          diagnostic_results[[paste0("error_", name)]] <- error_msg
          
          NULL
        })
        end_time <- Sys.time()
        
        if (!is.null(result)) {
          time_taken <- round(as.numeric(end_time - start_time, units = "secs"), 2)
          cat(sprintf("    ✅ Success: %d rows in %s seconds\n", nrow(result), time_taken))
          diagnostic_results[[paste0("arrow_", name)]] <- TRUE
        } else {
          diagnostic_results[[paste0("arrow_", name)]] <- FALSE
          break  # Stop testing larger queries if smaller one failed
        }
      }
      
      DatabaseConnector::disconnect(con)
      TRUE
    }, error = function(e) {
      cat("  ❌ Arrow connection failed:", e$message, "\n")
      diagnostic_results$arrow_connection <- FALSE
      FALSE
    })
  }
  
} else {
  cat("❌ No .env file found. Please create one first.\n")
}

cat("\n")

# ============================================
# SECTION 5: Diagnostic Summary
# ============================================
cat("========================================\n")
cat("           DIAGNOSTIC SUMMARY\n")
cat("========================================\n\n")

# Determine the likely issue
if (!isTRUE(diagnostic_results$internet)) {
  cat("🔴 PRIMARY ISSUE: No internet connectivity\n")
  cat("   Cannot reach external services. Check network connection.\n")
} else if (!isTRUE(diagnostic_results$storage_accessible)) {
  cat("🔴 PRIMARY ISSUE: Cloud storage endpoints are blocked\n")
  cat("   Arrow requires direct access to cloud storage (Azure/AWS/GCS).\n")
  cat("   This is likely blocked by firewall/proxy.\n")
  cat("\n   SOLUTION: Either:\n")
  cat("   1. Request firewall exceptions for storage domains\n")
  cat("   2. Keep Arrow disabled (use ENABLE_ARROW=FALSE)\n")
} else if (!isTRUE(diagnostic_results$basic_connection)) {
  cat("🔴 PRIMARY ISSUE: Cannot connect to Databricks\n")
  cat("   The connection fails even without Arrow.\n")
  cat("   Check your Databricks credentials and network access.\n")
} else if (isTRUE(diagnostic_results$basic_connection) && !isTRUE(diagnostic_results$arrow_connection)) {
  cat("🟡 ISSUE: Arrow-specific problem\n")
  cat("   Connection works without Arrow but fails with Arrow enabled.\n")
  if (isTRUE(diagnostic_results$storage_urls_in_error)) {
    cat("   Error messages contain storage URLs - confirming storage access issue.\n")
  }
  if (isTRUE(diagnostic_results$timeout_error)) {
    cat("   Timeouts detected - network latency or firewall blocking.\n")
  }
  cat("\n   SOLUTION: Keep Arrow disabled or resolve network access.\n")
} else if (all(diagnostic_results$basic_connection, diagnostic_results$arrow_connection)) {
  cat("✅ All tests passed!\n")
  cat("   Both standard and Arrow connections are working.\n")
} else {
  cat("🟡 PARTIAL SUCCESS\n")
  cat("   Some tests passed but others failed. Review details above.\n")
}

# Save diagnostic results
cat("\n")
cat("Diagnostic results saved to: arrow_diagnostic_results.rds\n")
saveRDS(diagnostic_results, "arrow_diagnostic_results.rds")

cat("\n========================================\n")
cat("         DIAGNOSTIC COMPLETE\n")
cat("========================================\n")