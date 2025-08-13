#!/usr/bin/env Rscript

# Test SqlRender Integration with Actual Database
# This script tests the new SqlRender-based functions with your SQL Server database

cat("==================================================\n")
cat("SqlRender Integration Test\n")
cat("==================================================\n\n")

# Set Java environment
Sys.setenv(JAVA_HOME='/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home')
Sys.setenv(PATH=paste0('/opt/homebrew/opt/openjdk@17/bin:', Sys.getenv('PATH')))

# Load environment and configuration
source("R/load_env.R")
source("R/config.R")
source("R/connection.R")

# Load required libraries
library(DatabaseConnector)
library(DBI)
library(dplyr)
library(dbplyr)
library(SqlRender)

# Source the new SQL functions
source("R/sql_functions.R")

# Source query utilities (which now use SqlRender)
source("R/query_utils.R")

cat("Setting up database connection...\n")

# Create connection using your configuration
config <- load_config()

# Get connection details from environment
connectionDetails <- get_connection_from_env()

# Create the actual connection
connection <- create_connection(connectionDetails = connectionDetails,
                               cdmDatabaseSchema = config$database$cdmDatabaseSchema,
                               resultsDatabaseSchema = config$database$resultsDatabaseSchema,
                               config = config)

# Store the connection for reference
test_connection <- connection

cat("Connection established. Database:", attr(connection, "dbms", exact = TRUE), "\n\n")

# Test Suite
test_results <- list()

# Helper function to run tests
run_test <- function(test_name, test_func) {
  cat("Testing:", test_name, "... ")
  tryCatch({
    result <- test_func()
    cat("✅ PASSED\n")
    list(test = test_name, status = "PASSED", result = result)
  }, error = function(e) {
    cat("❌ FAILED\n")
    cat("  Error:", e$message, "\n")
    list(test = test_name, status = "FAILED", error = e$message)
  })
}

# ==============================================================================
# Test 1: sql_date_diff function
# ==============================================================================
test_results[[1]] <- run_test("sql_date_diff", function() {
  # Test with person table - calculate age
  person_tbl <- get_cdm_table(connection, "person", config)
  
  result <- person_tbl %>%
    filter(year_of_birth > 1950) %>%
    head(10) %>%
    mutate(
      # Create a birth date
      birth_date = sql_date_from_parts("year_of_birth", "1", "1", connection),
      # Calculate days since birth (using current date)
      days_old = sql_date_diff("GETDATE()", "birth_date", "day", connection),
      # Calculate years approximately
      years_old = days_old / 365.25
    ) %>%
    select(person_id, year_of_birth, birth_date, days_old, years_old) %>%
    collect()
  
  # Check that we got results
  if (nrow(result) > 0 && !is.na(result$days_old[1])) {
    cat("\n  Sample: Person", result$person_id[1], "born", result$year_of_birth[1], 
        "is approximately", round(result$years_old[1]), "years old\n")
    return(TRUE)
  }
  stop("No results returned")
})

# ==============================================================================
# Test 2: sql_date_from_parts function
# ==============================================================================
test_results[[2]] <- run_test("sql_date_from_parts", function() {
  # Test creating dates from parts
  person_tbl <- get_cdm_table(connection, "person", config)
  
  result <- person_tbl %>%
    filter(!is.na(year_of_birth)) %>%
    head(10) %>%
    mutate(
      # Handle missing month/day
      month_of_birth = if_else(is.na(month_of_birth), 1, month_of_birth),
      day_of_birth = if_else(is.na(day_of_birth), 1, day_of_birth),
      # Create date from parts
      constructed_date = sql_date_from_parts("year_of_birth", "month_of_birth", "day_of_birth", connection)
    ) %>%
    select(person_id, year_of_birth, month_of_birth, day_of_birth, constructed_date) %>%
    collect()
  
  if (nrow(result) > 0 && !is.na(result$constructed_date[1])) {
    cat("\n  Sample: Created date", result$constructed_date[1], 
        "from", result$year_of_birth[1], "/", result$month_of_birth[1], "/", result$day_of_birth[1], "\n")
    return(TRUE)
  }
  stop("Date construction failed")
})

# ==============================================================================
# Test 3: sql_date_add function
# ==============================================================================
test_results[[3]] <- run_test("sql_date_add", function() {
  # Test with condition occurrence - add days to start date
  condition_tbl <- get_cdm_table(connection, "condition_occurrence", config)
  
  result <- condition_tbl %>%
    filter(!is.na(condition_start_date)) %>%
    head(10) %>%
    mutate(
      # Add 30 days to condition start date
      date_plus_30 = sql_date_add("condition_start_date", "30", "day", connection),
      # Subtract 7 days
      date_minus_7 = sql_date_add("condition_start_date", "-7", "day", connection)
    ) %>%
    select(condition_occurrence_id, condition_start_date, date_plus_30, date_minus_7) %>%
    collect()
  
  if (nrow(result) > 0 && !is.na(result$date_plus_30[1])) {
    cat("\n  Sample: Date", result$condition_start_date[1], 
        "+ 30 days =", result$date_plus_30[1], "\n")
    return(TRUE)
  }
  stop("Date addition failed")
})

# ==============================================================================
# Test 4: sql_concat function
# ==============================================================================
test_results[[4]] <- run_test("sql_concat", function() {
  # Test concatenation with person table
  person_tbl <- get_cdm_table(connection, "person", config)
  
  result <- person_tbl %>%
    head(10) %>%
    mutate(
      # Create a composite ID
      composite_id = sql_concat("person_id", "'_'", "year_of_birth", connection = connection)
    ) %>%
    select(person_id, year_of_birth, composite_id) %>%
    collect()
  
  if (nrow(result) > 0 && !is.na(result$composite_id[1])) {
    cat("\n  Sample: Concatenated", result$person_id[1], "and", result$year_of_birth[1], 
        "to get", result$composite_id[1], "\n")
    return(TRUE)
  }
  stop("Concatenation failed")
})

# ==============================================================================
# Test 5: Integration with HIP algorithm functions
# ==============================================================================
test_results[[5]] <- run_test("HIP algorithm integration", function() {
  # Load HIP algorithm with new SQL functions
  source("R/hip_algorithm.R")
  
  # Test that the initial_pregnant_cohort function works with SqlRender
  # This uses sql_date_from_parts and sql_date_diff internally
  
  # Get required tables
  procedure_tbl <- get_cdm_table(connection, "procedure_occurrence", config)
  measurement_tbl <- get_cdm_table(connection, "measurement", config)
  observation_tbl <- get_cdm_table(connection, "observation", config)
  condition_tbl <- get_cdm_table(connection, "condition_occurrence", config)
  person_tbl <- get_cdm_table(connection, "person", config)
  
  # Load HIP concepts
  HIP_concepts <- readxl::read_excel("inst/concepts/HIP_CDM_value_concepts.xlsx") %>%
    filter(!is.na(concept_id))
  
  # Test the function (just check it runs without error)
  # We'll limit to a small subset for speed
  person_subset <- person_tbl %>%
    filter(gender_concept_id == 8532) %>%  # Female
    head(100)
  
  result <- initial_pregnant_cohort(
    procedure_tbl, measurement_tbl, observation_tbl, condition_tbl,
    person_subset, HIP_concepts, config, connection
  )
  
  # Try to get a count (may be 0 if no matching data)
  count_result <- result %>% count() %>% collect()
  
  cat("\n  Found", count_result$n, "pregnancy-related records in test subset\n")
  return(TRUE)
})

# ==============================================================================
# Test 6: Integration with PPS algorithm functions
# ==============================================================================
test_results[[6]] <- run_test("PPS algorithm integration", function() {
  # Load PPS algorithm with new SQL functions
  source("R/pps_algorithm.R")
  
  # Test input_GT_concepts function
  condition_tbl <- get_cdm_table(connection, "condition_occurrence", config)
  procedure_tbl <- get_cdm_table(connection, "procedure_occurrence", config)
  observation_tbl <- get_cdm_table(connection, "observation", config)
  measurement_tbl <- get_cdm_table(connection, "measurement", config)
  visit_tbl <- get_cdm_table(connection, "visit_occurrence", config)
  
  # Load PPS concepts
  PPS_concepts <- readxl::read_excel("inst/concepts/PPS_CDM_concepts.xlsx") %>%
    rename(domain_concept_id = concept_id) %>%
    filter(!is.na(domain_concept_id))
  
  # Test the function
  result <- input_GT_concepts(
    condition_tbl, procedure_tbl, observation_tbl, 
    measurement_tbl, visit_tbl, PPS_concepts
  )
  
  count_result <- result %>% count() %>% collect()
  
  cat("\n  Found", count_result$n, "PPS concepts in database\n")
  return(TRUE)
})

# ==============================================================================
# Test 7: Query utilities integration
# ==============================================================================
test_results[[7]] <- run_test("Query utilities integration", function() {
  # Test the updated date_diff function
  person_tbl <- get_cdm_table(connection, "person", config)
  
  result <- person_tbl %>%
    head(10) %>%
    mutate(
      birth_date = sql_date_from_parts("year_of_birth", "1", "1", connection),
      # Use the date_diff wrapper function
      age_days = date_diff("GETDATE()", birth_date, sql("day"), connection)
    ) %>%
    select(person_id, birth_date, age_days) %>%
    collect()
  
  if (nrow(result) > 0 && !is.na(result$age_days[1])) {
    cat("\n  date_diff wrapper working correctly\n")
    return(TRUE)
  }
  stop("date_diff wrapper failed")
})

# ==============================================================================
# Test 8: Complex query with multiple SqlRender functions
# ==============================================================================
test_results[[8]] <- run_test("Complex query integration", function() {
  # Test a complex query using multiple new functions together
  person_tbl <- get_cdm_table(connection, "person", config)
  
  result <- person_tbl %>%
    filter(year_of_birth > 1970) %>%
    head(10) %>%
    mutate(
      # Create birth date
      birth_date = sql_date_from_parts("year_of_birth", "6", "15", connection),
      # Add 18 years (approximately)
      adult_date = sql_date_add("birth_date", "6570", "day", connection),
      # Calculate current age
      current_age_days = sql_date_diff("GETDATE()", "birth_date", "day", connection),
      # Create ID string
      person_string = sql_concat("'Person_'", "person_id", connection = connection)
    ) %>%
    select(person_id, birth_date, adult_date, current_age_days, person_string) %>%
    collect()
  
  if (nrow(result) > 0 && 
      !is.na(result$birth_date[1]) && 
      !is.na(result$adult_date[1]) && 
      !is.na(result$current_age_days[1]) &&
      !is.na(result$person_string[1])) {
    cat("\n  Complex query with all SqlRender functions successful\n")
    cat("  Sample:", result$person_string[1], "born", result$birth_date[1], "\n")
    return(TRUE)
  }
  stop("Complex query failed")
})

# ==============================================================================
# Print Summary
# ==============================================================================
cat("\n==================================================\n")
cat("Test Results Summary\n")
cat("==================================================\n\n")

passed <- sum(sapply(test_results, function(x) x$status == "PASSED"))
failed <- sum(sapply(test_results, function(x) x$status == "FAILED"))

for (result in test_results) {
  status_icon <- if(result$status == "PASSED") "✅" else "❌"
  cat(sprintf("%s %s: %s\n", status_icon, result$test, result$status))
  if (result$status == "FAILED") {
    cat(sprintf("   Error: %s\n", result$error))
  }
}

cat("\n--------------------------------------------------\n")
cat(sprintf("Total: %d passed, %d failed out of %d tests\n", passed, failed, length(test_results)))

if (failed == 0) {
  cat("\n🎉 All tests passed! SqlRender integration is working correctly.\n")
} else {
  cat("\n⚠️ Some tests failed. Please review the errors above.\n")
}

# Disconnect
DatabaseConnector::disconnect(connection)
cat("\nDatabase connection closed.\n")

# Return test results for programmatic use
invisible(test_results)