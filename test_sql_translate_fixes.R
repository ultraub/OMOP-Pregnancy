# Test script to verify sql_translate fixes
library(dplyr)
library(dbplyr)

# Source required files
source("R/sql_functions.R")

# Create a mock connection for testing
mock_connection <- structure(
  list(),
  class = "TestConnection",
  dbms = "spark"
)

# Test 1: Basic sql_translate works
cat("Test 1: Basic sql_translate...\n")
result1 <- sql_translate("DATEDIFF(day, date1, date2)", mock_connection)
cat("  Result:", as.character(result1), "\n")
if (grepl("DATEDIFF\\(date2, date1\\)", as.character(result1))) {
  cat("  ✓ PASSED: Spark DATEDIFF syntax correct\n")
} else {
  cat("  ✗ FAILED: Expected Spark DATEDIFF syntax\n")
}

# Test 2: DATEADD translation
cat("\nTest 2: DATEADD translation...\n")
result2 <- sql_translate("DATEADD(day, 10, visit_date)", mock_connection)
cat("  Result:", as.character(result2), "\n")
if (grepl("DATE_ADD\\(visit_date, 10\\)", as.character(result2))) {
  cat("  ✓ PASSED: Spark DATE_ADD syntax correct\n")
} else {
  cat("  ✗ FAILED: Expected Spark DATE_ADD syntax\n")
}

# Test 3: NULL casting
cat("\nTest 3: NULL casting...\n")
result3 <- sql_translate("CAST(NULL AS DATE)", mock_connection)
cat("  Result:", as.character(result3), "\n")
if (grepl("CAST\\(NULL AS DATE\\)", as.character(result3))) {
  cat("  ✓ PASSED: NULL cast preserved\n")
} else {
  cat("  ✗ FAILED: Expected NULL cast to be preserved\n")
}

# Test 4: CONCAT translation
cat("\nTest 4: CONCAT translation...\n")
result4 <- sql_translate("CONCAT(CAST(person_id AS VARCHAR), CAST(visit_date AS VARCHAR))", mock_connection)
cat("  Result:", as.character(result4), "\n")
if (grepl("CONCAT", as.character(result4))) {
  cat("  ✓ PASSED: CONCAT preserved for Spark\n")
} else {
  cat("  ✗ FAILED: Expected CONCAT to be preserved\n")
}

# Test 5: Check that pre-computation pattern works
cat("\nTest 5: Pre-computation and injection pattern...\n")
tryCatch({
  # Pre-compute SQL expression
  test_sql <- sql_translate("DATEDIFF(day, start_date, end_date)", mock_connection)
  
  # Create a mock tibble
  test_data <- tibble(
    person_id = 1:3,
    start_date = as.Date(c("2023-01-01", "2023-02-01", "2023-03-01")),
    end_date = as.Date(c("2023-01-10", "2023-02-15", "2023-03-20"))
  )
  
  # Test injection with !!
  result5 <- test_data %>%
    mutate(days_diff = !!test_sql)
  
  cat("  ✓ PASSED: Pre-computation and injection pattern works\n")
}, error = function(e) {
  cat("  ✗ FAILED:", e$message, "\n")
})

cat("\n=== All tests completed ===\n")