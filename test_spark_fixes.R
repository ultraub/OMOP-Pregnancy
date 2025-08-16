# Test script to verify Spark/Databricks fixes
# This script tests the improvements to handle JDBC BackgroundFetcher errors

library(OMOPPregnancy)
library(dplyr)
library(DatabaseConnector)

test_spark_fixes <- function(connection, cdmDatabaseSchema) {
  
  cat("\n================== TESTING SPARK/DATABRICKS FIXES ==================\n\n")
  
  # Test 1: Create a simple test table
  cat("Test 1: Creating test table...\n")
  test_data <- data.frame(
    id = 1:10,
    value = rnorm(10),
    category = rep(c("A", "B"), 5),
    stringsAsFactors = FALSE
  )
  
  test_table <- tryCatch({
    create_temp_table(connection, test_data, "test_spark_table")
  }, error = function(e) {
    cat("  ERROR creating table: ", e$message, "\n")
    return(NULL)
  })
  
  if (!is.null(test_table)) {
    cat("  ✓ Table created successfully\n")
    
    # Test 2: Extract table name
    cat("\nTest 2: Extracting table name...\n")
    table_name <- get_spark_table_name(test_table)
    cat("  Extracted name: ", ifelse(is.null(table_name), "NULL", table_name), "\n")
    if (!is.null(table_name)) {
      cat("  ✓ Table name extraction successful\n")
    } else {
      cat("  ✗ Failed to extract table name\n")
    }
    
    # Test 3: Count rows using spark_safe_count
    cat("\nTest 3: Counting rows with spark_safe_count...\n")
    count_result <- spark_safe_count(connection, test_table)
    cat("  Count result: ", ifelse(is.na(count_result), "NA", as.character(count_result)), "\n")
    if (!is.na(count_result) && count_result == 10) {
      cat("  ✓ Count successful and correct\n")
    } else if (!is.na(count_result)) {
      cat("  ⚠ Count returned but incorrect value\n")
    } else {
      cat("  ✗ Count failed\n")
    }
    
    # Test 4: Count rows using safe_count
    cat("\nTest 4: Counting rows with safe_count...\n")
    count_result2 <- safe_count(test_table)
    cat("  Count result: ", ifelse(is.na(count_result2), "NA", as.character(count_result2)), "\n")
    if (!is.na(count_result2) && count_result2 == 10) {
      cat("  ✓ Count successful and correct\n")
    } else if (!is.na(count_result2)) {
      cat("  ⚠ Count returned but incorrect value\n")
    } else {
      cat("  ✗ Count failed\n")
    }
    
    # Test 5: Verify table upload
    cat("\nTest 5: Verifying table upload...\n")
    verify_result <- verify_table_upload(test_table, "test_spark_table")
    if (!is.na(verify_result$total_rows)) {
      cat("  ✓ Verification completed\n")
    } else {
      cat("  ✗ Verification failed\n")
    }
    
    # Test 6: Safe collect
    cat("\nTest 6: Testing safe_collect...\n")
    collected <- tryCatch({
      safe_collect(test_table)
    }, error = function(e) {
      cat("  ERROR in safe_collect: ", e$message, "\n")
      return(NULL)
    })
    
    if (!is.null(collected) && nrow(collected) == 10) {
      cat("  ✓ Collection successful\n")
      cat("  Collected ", nrow(collected), " rows with ", ncol(collected), " columns\n")
    } else if (!is.null(collected)) {
      cat("  ⚠ Collection returned incorrect data\n")
    } else {
      cat("  ✗ Collection failed\n")
    }
  }
  
  # Test 7: Test with HIP_concepts data
  cat("\nTest 7: Testing with HIP_concepts data...\n")
  hip_data <- data.frame(
    concept_id = c(1, 2, 3, 4, 5),
    concept_name = c("Test1", "Test2", "Test3", "Test4", "Test5"),
    category = c("PREG", "PREG", "GEST", "GEST", "AGE"),
    gest_value = c(NA, 10, 20, NA, 30),
    stringsAsFactors = FALSE
  )
  
  hip_table <- tryCatch({
    create_temp_table(connection, hip_data, "test_hip_concepts")
  }, error = function(e) {
    cat("  ERROR creating HIP table: ", e$message, "\n")
    return(NULL)
  })
  
  if (!is.null(hip_table)) {
    cat("  ✓ HIP_concepts test table created\n")
    
    # Verify the table
    verify_result <- verify_table_upload(hip_table, "test_hip_concepts")
    
    # Try to count with filtering
    cat("\nTest 8: Counting with filter conditions...\n")
    filtered_count <- tryCatch({
      hip_table %>%
        filter(!is.na(gest_value)) %>%
        safe_count()
    }, error = function(e) {
      cat("  ERROR in filtered count: ", e$message, "\n")
      return(NA)
    })
    
    cat("  Filtered count (non-NULL gest_value): ", 
        ifelse(is.na(filtered_count), "NA", as.character(filtered_count)), "\n")
    if (!is.na(filtered_count) && filtered_count == 3) {
      cat("  ✓ Filtered count successful and correct\n")
    } else if (!is.na(filtered_count)) {
      cat("  ⚠ Filtered count returned but incorrect value\n")
    } else {
      cat("  ✗ Filtered count failed\n")
    }
  }
  
  cat("\n===================== TEST SUMMARY =====================\n")
  cat("If all tests passed (✓), the Spark/Databricks fixes are working correctly.\n")
  cat("If any tests failed (✗), there may still be issues to resolve.\n")
  cat("========================================================\n\n")
}

# Usage:
# test_spark_fixes(con, cdmDatabaseSchema)