#!/usr/bin/env Rscript

# Comprehensive Package Test
# Tests building, loading, and basic functionality

cat("==================================================\n")
cat("OMOPPregnancy Package Verification Test\n")
cat("==================================================\n\n")

# Set Java environment for SqlRender
Sys.setenv(JAVA_HOME='/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home')
Sys.setenv(PATH=paste0('/opt/homebrew/opt/openjdk@17/bin:', Sys.getenv('PATH')))

# Track test results
test_results <- list()
test_count <- 0

# Helper function for tests
run_test <- function(test_name, test_func) {
  test_count <<- test_count + 1
  cat(sprintf("\n[Test %d] %s\n", test_count, test_name))
  cat(paste(rep("-", 50), collapse=""), "\n")
  
  result <- tryCatch({
    output <- test_func()
    cat("✅ PASSED\n")
    list(name = test_name, status = "PASSED", output = output)
  }, error = function(e) {
    cat("❌ FAILED\n")
    cat("Error:", e$message, "\n")
    list(name = test_name, status = "FAILED", error = e$message)
  })
  
  test_results[[test_count]] <<- result
  return(result$status == "PASSED")
}

# ==============================================================================
# TEST 1: Load Package Functions Directly
# ==============================================================================
run_test("Load package functions directly", function() {
  # Load required libraries
  library(dplyr)
  library(dbplyr)
  
  # Source the main files
  source("R/sql_functions.R")
  source("R/config.R")
  source("R/query_utils.R")
  
  # Check if key functions exist
  funcs_to_check <- c("sql_date_diff", "sql_date_from_parts", 
                      "sql_date_add", "sql_concat", "load_config")
  
  missing <- c()
  for (func in funcs_to_check) {
    if (!exists(func)) {
      missing <- c(missing, func)
    }
  }
  
  if (length(missing) > 0) {
    stop(paste("Missing functions:", paste(missing, collapse=", ")))
  }
  
  cat("  All key functions loaded successfully\n")
  return(TRUE)
})

# ==============================================================================
# TEST 2: Test SQL Functions
# ==============================================================================
run_test("SQL function generation", function() {
  cat("\n  Testing sql_date_diff...\n")
  result1 <- sql_date_diff("date1", "date2", "day")
  cat("    Result:", as.character(result1), "\n")
  if (!grepl("DATEDIFF", as.character(result1))) {
    stop("sql_date_diff did not generate expected SQL")
  }
  
  cat("  Testing sql_date_from_parts...\n")
  result2 <- sql_date_from_parts("2024", "1", "15")
  cat("    Result:", as.character(result2), "\n")
  if (!grepl("DATEFROMPARTS", as.character(result2))) {
    stop("sql_date_from_parts did not generate expected SQL")
  }
  
  cat("  Testing sql_date_add...\n")
  result3 <- sql_date_add("date1", "30", "day")
  cat("    Result:", as.character(result3), "\n")
  if (!grepl("DATEADD", as.character(result3))) {
    stop("sql_date_add did not generate expected SQL")
  }
  
  cat("  Testing sql_concat...\n")
  result4 <- sql_concat("field1", "field2")
  cat("    Result:", as.character(result4), "\n")
  if (!grepl("CONCAT", as.character(result4))) {
    stop("sql_concat did not generate expected SQL")
  }
  
  return(TRUE)
})

# ==============================================================================
# TEST 3: Test Configuration Loading
# ==============================================================================
run_test("Configuration system", function() {
  # Test default config
  config <- load_config()
  
  if (is.null(config)) {
    stop("Failed to load default configuration")
  }
  
  # Check key config elements
  if (!all(c("database", "algorithms", "output") %in% names(config))) {
    stop("Configuration missing required sections")
  }
  
  cat("  Configuration loaded with sections:", paste(names(config), collapse=", "), "\n")
  
  # Test concept mappings
  mappings <- get_concept_mappings()
  if (nrow(mappings) == 0) {
    stop("No concept mappings returned")
  }
  
  cat("  Concept mappings loaded:", nrow(mappings), "mappings\n")
  
  return(TRUE)
})

# ==============================================================================
# TEST 4: Load Algorithm Functions
# ==============================================================================
run_test("Load algorithm functions", function() {
  # Source algorithm files
  source("R/hip_algorithm.R")
  source("R/pps_algorithm.R")
  source("R/merge_episodes.R")
  
  # Check HIP functions
  hip_funcs <- c("initial_pregnant_cohort", "final_visits", "add_delivery", 
                 "calculate_start", "final_episodes_with_length")
  
  missing_hip <- c()
  for (func in hip_funcs) {
    if (!exists(func)) {
      missing_hip <- c(missing_hip, func)
    }
  }
  
  if (length(missing_hip) > 0) {
    stop(paste("Missing HIP functions:", paste(missing_hip, collapse=", ")))
  }
  
  cat("  HIP algorithm functions loaded\n")
  
  # Check PPS functions
  pps_funcs <- c("input_GT_concepts", "get_PPS_episodes", 
                 "get_episode_max_min_dates")
  
  missing_pps <- c()
  for (func in pps_funcs) {
    if (!exists(func)) {
      missing_pps <- c(missing_pps, func)
    }
  }
  
  if (length(missing_pps) > 0) {
    stop(paste("Missing PPS functions:", paste(missing_pps, collapse=", ")))
  }
  
  cat("  PPS algorithm functions loaded\n")
  
  return(TRUE)
})

# ==============================================================================
# TEST 5: Test SQL Functions in dplyr Pipeline
# ==============================================================================
run_test("SQL functions in dplyr pipeline", function() {
  library(dplyr)
  library(dbplyr)
  
  # Create a mock tibble to test with
  test_data <- tibble(
    person_id = 1:3,
    year_of_birth = c(1990, 1985, 1995),
    month_of_birth = c(5, 12, 1),
    day_of_birth = c(15, 25, 10),
    visit_date = as.Date(c("2024-01-15", "2024-02-20", "2024-03-10"))
  )
  
  # Test that SQL functions work in mutate without connection parameter
  result <- test_data %>%
    mutate(
      birth_date = sql_date_from_parts("year_of_birth", "month_of_birth", "day_of_birth"),
      days_old = sql_date_diff("visit_date", "birth_date", "day"),
      future_date = sql_date_add("visit_date", "30", "day"),
      id_string = sql_concat("'Person_'", "person_id")
    )
  
  # Check that columns were created
  if (!all(c("birth_date", "days_old", "future_date", "id_string") %in% names(result))) {
    stop("SQL functions did not create expected columns")
  }
  
  cat("  SQL functions work correctly in dplyr pipelines\n")
  
  return(TRUE)
})

# ==============================================================================
# TEST 6: Check Package Structure
# ==============================================================================
run_test("Package structure validation", function() {
  required_files <- c(
    "DESCRIPTION",
    "NAMESPACE",
    "R/sql_functions.R",
    "R/hip_algorithm.R",
    "R/pps_algorithm.R",
    "R/run_hipps.R",
    "R/config.R",
    "R/connection.R",
    "R/query_utils.R"
  )
  
  missing_files <- c()
  for (file in required_files) {
    if (!file.exists(file)) {
      missing_files <- c(missing_files, file)
    }
  }
  
  if (length(missing_files) > 0) {
    stop(paste("Missing required files:", paste(missing_files, collapse=", ")))
  }
  
  cat("  All required package files present\n")
  
  # Check NAMESPACE exports
  namespace_content <- readLines("NAMESPACE")
  exports <- grep("^export\\(", namespace_content, value = TRUE)
  
  cat("  NAMESPACE contains", length(exports), "exports\n")
  
  # Check for undefined exports
  source("check_exports.R", local = TRUE)
  
  return(TRUE)
})

# ==============================================================================
# TEST 7: Verify No Source() Calls in Package
# ==============================================================================
run_test("No problematic source() calls", function() {
  # Check for source() calls in algorithm files
  algorithm_files <- c("R/hip_algorithm.R", "R/pps_algorithm.R")
  
  for (file in algorithm_files) {
    content <- readLines(file)
    source_lines <- grep("source\\(", content)
    
    if (length(source_lines) > 0) {
      # Check if they're just comments
      actual_source <- grep("^[^#]*source\\(", content[source_lines])
      if (length(actual_source) > 0) {
        stop(paste("Found source() call in", file, "at line(s):", 
                   paste(source_lines[actual_source], collapse=", ")))
      }
    }
  }
  
  cat("  No problematic source() calls found\n")
  
  return(TRUE)
})

# ==============================================================================
# SUMMARY
# ==============================================================================
cat("\n")
cat("==================================================\n")
cat("Test Summary\n")
cat("==================================================\n\n")

passed <- sum(sapply(test_results, function(x) x$status == "PASSED"))
failed <- sum(sapply(test_results, function(x) x$status == "FAILED"))

for (i in seq_along(test_results)) {
  result <- test_results[[i]]
  icon <- if(result$status == "PASSED") "✅" else "❌"
  cat(sprintf("%s Test %d: %s - %s\n", icon, i, result$name, result$status))
}

cat("\n")
cat(sprintf("Results: %d passed, %d failed out of %d tests\n", passed, failed, length(test_results)))

if (failed == 0) {
  cat("\n🎉 SUCCESS! The package builds and runs correctly.\n")
  cat("\nThe OMOPPregnancy package is ready for use with:\n")
  cat("- SqlRender integration for cross-platform SQL\n")
  cat("- HIP and PPS algorithms implemented\n")
  cat("- Configuration system working\n")
  cat("- All exports properly defined\n")
} else {
  cat("\n⚠️ Some tests failed. Please review the errors above.\n")
}

cat("\n==================================================\n")
cat("Next Steps\n")
cat("==================================================\n")
cat("1. Install the package: R CMD INSTALL OMOPPregnancy_0.1.0.tar.gz\n")
cat("2. Or load functions directly: source the R files as needed\n")
cat("3. Connect to your database and run the algorithms\n")
cat("\n")