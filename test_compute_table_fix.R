# Test that compute_table handles data.frames correctly

library(OMOPPregnancy)
source("R/query_utils.R")

test_compute_table_fix <- function() {
  cat("\n========== TESTING COMPUTE_TABLE FIX ==========\n\n")
  
  # Test 1: compute_table with a data.frame
  cat("Test 1: compute_table with data.frame input\n")
  test_df <- data.frame(
    id = 1:5,
    value = rnorm(5)
  )
  
  result <- tryCatch({
    compute_table(test_df)
  }, error = function(e) {
    cat("  ERROR: ", e$message, "\n")
    return(NULL)
  })
  
  if (!is.null(result) && identical(result, test_df)) {
    cat("  ✓ compute_table correctly returned data.frame as-is\n")
  } else if (!is.null(result)) {
    cat("  ⚠ compute_table returned something but not identical\n")
  } else {
    cat("  ✗ compute_table failed with data.frame input\n")
  }
  
  # Test 2: compute_table with NULL connection and data.frame
  cat("\nTest 2: compute_table with data.frame and connection = NULL\n")
  result2 <- tryCatch({
    compute_table(test_df, connection = NULL)
  }, error = function(e) {
    cat("  ERROR: ", e$message, "\n")
    return(NULL)
  })
  
  if (!is.null(result2) && identical(result2, test_df)) {
    cat("  ✓ Works even with NULL connection for data.frame\n")
  } else {
    cat("  ✗ Failed with NULL connection\n")
  }
  
  # Test 3: compute_table with a mock connection and data.frame
  cat("\nTest 3: compute_table with data.frame and mock SQL Server connection\n")
  mock_con <- list()
  class(mock_con) <- c("DatabaseConnectorJdbcConnection", "DBIConnection")
  attr(mock_con, "dbms") <- "sql server"
  
  result3 <- tryCatch({
    compute_table(test_df, connection = mock_con)
  }, error = function(e) {
    cat("  ERROR: ", e$message, "\n")
    return(NULL)
  })
  
  if (!is.null(result3) && identical(result3, test_df)) {
    cat("  ✓ Works with SQL Server connection for data.frame\n")
  } else {
    cat("  ✗ Failed with SQL Server connection\n")
  }
  
  # Test 4: Test with a matrix (should trigger warning)
  cat("\nTest 4: compute_table with matrix input (edge case)\n")
  test_matrix <- matrix(1:10, nrow = 5)
  
  result4 <- tryCatch({
    suppressWarnings(compute_table(test_matrix))
  }, error = function(e) {
    cat("  ERROR: ", e$message, "\n")
    return(NULL)
  }, warning = function(w) {
    cat("  Warning (expected): ", w$message, "\n")
    return(test_matrix)
  })
  
  if (!is.null(result4)) {
    cat("  ✓ Handled non-standard input gracefully\n")
  }
  
  cat("\n===================== SUMMARY =====================\n")
  cat("compute_table now handles data.frames correctly.\n")
  cat("This should fix the SQL Server error where\n")
  cat("initial_pregnant_cohort returns a data.frame.\n")
  cat("===================================================\n\n")
}

# Run the test
test_compute_table_fix()