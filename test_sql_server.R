# Test script to verify SQL Server compatibility after Spark fixes
# This ensures our Databricks fixes don't break SQL Server functionality

library(OMOPPregnancy)
library(dplyr)
library(DatabaseConnector)

# Function to test SQL Server connection
test_sql_server <- function() {
  
  cat("\n================== TESTING SQL SERVER COMPATIBILITY ==================\n\n")
  
  # Create SQL Server connection
  cat("Creating SQL Server connection...\n")
  connectionDetails <- createConnectionDetails(
    dbms = "sql server",
    server = "omop-mssql.database.windows.net",
    user = "omop_user",
    password = keyring::key_get("omop_mssql_password"),
    port = 1433
  )
  
  con <- connect(connectionDetails)
  
  # Set attributes for our functions to recognize SQL Server
  attr(con, "dbms") <- "sql server"
  attr(con, "mode") <- "generic"
  attr(con, "cdmDatabaseSchema") <- "cdm_54"
  attr(con, "resultsDatabaseSchema") <- "results"
  
  cat("  Connection established. DBMS: ", attr(con, "dbms", exact = TRUE), "\n\n")
  
  # Test 1: Create a temp table (should use # prefix for SQL Server)
  cat("Test 1: Creating SQL Server temp table...\n")
  test_data <- data.frame(
    id = 1:10,
    value = rnorm(10),
    category = rep(c("A", "B"), 5),
    stringsAsFactors = FALSE
  )
  
  test_table <- tryCatch({
    create_temp_table(con, test_data, "test_sql_table")
  }, error = function(e) {
    cat("  ERROR creating table: ", e$message, "\n")
    return(NULL)
  })
  
  if (!is.null(test_table)) {
    cat("  ✓ Table created successfully\n")
    
    # Check if table name has # prefix as expected for SQL Server
    table_sql <- dbplyr::sql_render(test_table)
    if (grepl("#", as.character(table_sql))) {
      cat("  ✓ Temp table has correct # prefix\n")
    } else {
      cat("  ⚠ Warning: Temp table may not have # prefix\n")
    }
  }
  
  # Test 2: Count rows using safe_count
  cat("\nTest 2: Counting rows with safe_count...\n")
  count_result <- tryCatch({
    safe_count(test_table)
  }, error = function(e) {
    cat("  ERROR in safe_count: ", e$message, "\n")
    return(NA)
  })
  
  cat("  Count result: ", ifelse(is.na(count_result), "NA", as.character(count_result)), "\n")
  if (!is.na(count_result) && count_result == 10) {
    cat("  ✓ Count successful and correct\n")
  } else if (!is.na(count_result)) {
    cat("  ⚠ Count returned but incorrect value\n")
  } else {
    cat("  ✗ Count failed\n")
  }
  
  # Test 3: Verify table upload
  cat("\nTest 3: Verifying table upload...\n")
  verify_result <- tryCatch({
    verify_table_upload(test_table, "test_sql_table")
  }, error = function(e) {
    cat("  ERROR in verify: ", e$message, "\n")
    return(NULL)
  })
  
  if (!is.null(verify_result) && !is.na(verify_result$total_rows)) {
    cat("  ✓ Verification completed\n")
  } else {
    cat("  ✗ Verification failed\n")
  }
  
  # Test 4: Safe collect
  cat("\nTest 4: Testing safe_collect...\n")
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
  
  # Test 5: Test with HIP_concepts-like data
  cat("\nTest 5: Testing with HIP_concepts data structure...\n")
  hip_data <- data.frame(
    concept_id = c(1, 2, 3, 4, 5),
    concept_name = c("Test1", "Test2", "Test3", "Test4", "Test5"),
    category = c("PREG", "PREG", "GEST", "GEST", "AGE"),
    gest_value = c(NA, 10, 20, NA, 30),
    stringsAsFactors = FALSE
  )
  
  hip_table <- tryCatch({
    create_temp_table(con, hip_data, "test_hip_concepts")
  }, error = function(e) {
    cat("  ERROR creating HIP table: ", e$message, "\n")
    return(NULL)
  })
  
  if (!is.null(hip_table)) {
    cat("  ✓ HIP_concepts test table created\n")
    
    # Verify the table
    verify_result <- verify_table_upload(hip_table, "test_hip_concepts")
  }
  
  # Test 6: Filtered counting
  cat("\nTest 6: Counting with filter conditions...\n")
  if (!is.null(hip_table)) {
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
      cat("  ⚠ Filtered count returned but incorrect value (expected 3, got ", filtered_count, ")\n")
    } else {
      cat("  ✗ Filtered count failed\n")
    }
  }
  
  # Test 7: compute_table function
  cat("\nTest 7: Testing compute_table...\n")
  if (!is.null(hip_table)) {
    computed <- tryCatch({
      hip_table %>%
        filter(category == "GEST") %>%
        compute_table(connection = con)
    }, error = function(e) {
      cat("  ERROR in compute_table: ", e$message, "\n")
      return(NULL)
    })
    
    if (!is.null(computed)) {
      computed_count <- safe_count(computed)
      cat("  Computed table row count: ", 
          ifelse(is.na(computed_count), "NA", as.character(computed_count)), "\n")
      if (!is.na(computed_count) && computed_count == 2) {
        cat("  ✓ compute_table successful\n")
      } else {
        cat("  ⚠ compute_table returned unexpected count\n")
      }
    } else {
      cat("  ✗ compute_table failed\n")
    }
  }
  
  # Test 8: SQL translation functions
  cat("\nTest 8: Testing SQL translation functions...\n")
  tryCatch({
    # Test sql_translate
    translated <- sql_translate("DATEDIFF(day, date1, date2)", con)
    cat("  sql_translate result: ", as.character(translated), "\n")
    
    # Test sql_case_when
    case_sql <- sql_case_when("value > 10", "1", "0", con)
    cat("  sql_case_when result: ", as.character(case_sql), "\n")
    
    cat("  ✓ SQL translation functions working\n")
  }, error = function(e) {
    cat("  ✗ SQL translation error: ", e$message, "\n")
  })
  
  # Clean up
  cat("\nCleaning up...\n")
  tryCatch({
    if (!is.null(test_table)) {
      # For SQL Server, the temp table should be automatically dropped when connection closes
      # But we can try to drop it explicitly
      DBI::dbExecute(con, "DROP TABLE IF EXISTS #test_sql_table")
    }
    if (!is.null(hip_table)) {
      DBI::dbExecute(con, "DROP TABLE IF EXISTS #test_hip_concepts")
    }
  }, error = function(e) {
    # Ignore cleanup errors
  })
  
  disconnect(con)
  cat("  Connection closed\n")
  
  cat("\n===================== SQL SERVER TEST SUMMARY =====================\n")
  cat("Check that all tests passed (✓) to ensure SQL Server compatibility.\n")
  cat("Any failures (✗) indicate regression that needs to be fixed.\n")
  cat("===================================================================\n\n")
}

# Run the test
test_sql_server()