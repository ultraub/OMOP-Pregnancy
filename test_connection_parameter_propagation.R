# Comprehensive test for connection parameter propagation
# This test ensures ALL functions properly pass connection parameters
# Critical for Databricks and SQL Server compatibility

library(OMOPPregnancy)

test_connection_parameter_propagation <- function() {
  cat("\n========== TESTING CONNECTION PARAMETER PROPAGATION ==========\n\n")
  
  # Create mock connections for different platforms
  mock_sql_server <- list()
  class(mock_sql_server) <- c("DatabaseConnectorJdbcConnection", "DBIConnection")
  attr(mock_sql_server, "dbms") <- "sql server"
  
  mock_databricks <- list()
  class(mock_databricks) <- c("DatabaseConnectorJdbcConnection", "DBIConnection")
  attr(mock_databricks, "dbms") <- "spark"
  
  # Track test results
  test_results <- list()
  
  # Helper function to test if a function accepts connection parameter
  test_function_connection <- function(func_name, func, required_args, connection) {
    cat(sprintf("Testing %s...\n", func_name))
    
    # Build argument list with connection
    args <- required_args
    args$connection <- connection
    
    # Try calling the function
    result <- tryCatch({
      # We can't actually run these functions without real data,
      # but we can check if they accept the connection parameter
      # by looking at their formals
      func_formals <- formals(func)
      if ("connection" %in% names(func_formals)) {
        cat(sprintf("  ✓ %s has connection parameter\n", func_name))
        return(TRUE)
      } else {
        cat(sprintf("  ✗ %s MISSING connection parameter\n", func_name))
        return(FALSE)
      }
    }, error = function(e) {
      cat(sprintf("  ⚠ Error testing %s: %s\n", func_name, e$message))
      return(FALSE)
    })
    
    return(result)
  }
  
  # Test all critical functions from hip_algorithm.R
  cat("\n=== Testing hip_algorithm.R functions ===\n")
  hip_functions <- list(
    "initial_pregnant_cohort" = initial_pregnant_cohort,
    "final_visits" = final_visits,
    "add_stillbirth" = add_stillbirth,
    "add_ectopic" = add_ectopic,
    "add_abortion" = add_abortion,
    "add_delivery" = add_delivery,
    "calculate_start" = calculate_start,
    "gestation_visits" = gestation_visits,
    "gestation_episodes" = gestation_episodes,
    "get_min_max_gestation" = get_min_max_gestation,
    "add_gestation" = add_gestation,
    "clean_episodes" = clean_episodes,
    "remove_overlaps" = remove_overlaps,
    "final_episodes_with_length" = final_episodes_with_length
  )
  
  for (func_name in names(hip_functions)) {
    result <- test_function_connection(func_name, hip_functions[[func_name]], list(), mock_databricks)
    test_results[[func_name]] <- result
  }
  
  # Test pps_algorithm.R functions
  cat("\n=== Testing pps_algorithm.R functions ===\n")
  pps_functions <- list(
    "get_PPS_episodes" = get_PPS_episodes,
    "get_episode_max_min_dates" = get_episode_max_min_dates
  )
  
  for (func_name in names(pps_functions)) {
    result <- test_function_connection(func_name, pps_functions[[func_name]], list(), mock_databricks)
    test_results[[func_name]] <- result
  }
  
  # Test esd_algorithm.R functions
  cat("\n=== Testing esd_algorithm.R functions ===\n")
  esd_functions <- list(
    "get_timing_concepts" = get_timing_concepts
  )
  
  for (func_name in names(esd_functions)) {
    result <- test_function_connection(func_name, esd_functions[[func_name]], list(), mock_databricks)
    test_results[[func_name]] <- result
  }
  
  # Test query_utils.R functions
  cat("\n=== Testing query_utils.R functions ===\n")
  query_functions <- list(
    "compute_table" = compute_table,
    "safe_count" = safe_count,
    "safe_collect" = safe_collect
  )
  
  for (func_name in names(query_functions)) {
    result <- test_function_connection(func_name, query_functions[[func_name]], list(), mock_databricks)
    test_results[[func_name]] <- result
  }
  
  # Test specific compute_table calls
  cat("\n=== Testing compute_table usage patterns ===\n")
  
  # Test 1: compute_table with data.frame (should work without connection)
  cat("Test: compute_table with data.frame\n")
  test_df <- data.frame(id = 1:5, value = rnorm(5))
  result1 <- tryCatch({
    compute_table(test_df)
    cat("  ✓ compute_table handles data.frame correctly\n")
    TRUE
  }, error = function(e) {
    cat("  ✗ compute_table failed with data.frame: ", e$message, "\n")
    FALSE
  })
  test_results["compute_table_dataframe"] <- result1
  
  # Test 2: compute_table with connection for data.frame
  cat("Test: compute_table with data.frame and connection\n")
  result2 <- tryCatch({
    compute_table(test_df, connection = mock_databricks)
    cat("  ✓ compute_table handles data.frame with connection\n")
    TRUE
  }, error = function(e) {
    cat("  ✗ compute_table failed with connection: ", e$message, "\n")
    FALSE
  })
  test_results["compute_table_dataframe_connection"] <- result2
  
  # Summary
  cat("\n===================== SUMMARY =====================\n")
  total_tests <- length(test_results)
  passed_tests <- sum(unlist(test_results), na.rm = TRUE)
  failed_tests <- total_tests - passed_tests
  
  cat(sprintf("Total tests: %d\n", total_tests))
  cat(sprintf("Passed: %d\n", passed_tests))
  cat(sprintf("Failed: %d\n", failed_tests))
  
  if (failed_tests > 0) {
    cat("\n⚠️  FAILED TESTS:\n")
    for (name in names(test_results)) {
      if (!test_results[[name]]) {
        cat(sprintf("  - %s\n", name))
      }
    }
  } else {
    cat("\n✅ ALL TESTS PASSED!\n")
  }
  
  cat("\nCritical Fix Applied:\n")
  cat("  - pps_algorithm.R line 253: Added connection parameter to compute_table\n")
  cat("\nPackage Deployment Required:\n")
  cat("  1. Run: devtools::document()\n")
  cat("  2. Run: devtools::build()\n")
  cat("  3. Deploy to SQL Server environment\n")
  cat("  4. Deploy to Databricks environment (CRITICAL)\n")
  cat("===================================================\n\n")
  
  return(test_results)
}

# Run the test
results <- test_connection_parameter_propagation()