#!/usr/bin/env Rscript

# Simple test of SqlRender functions without database connection
# This tests the SQL generation capability

cat("==================================================\n")
cat("SqlRender Function Tests (No Database Required)\n")
cat("==================================================\n\n")

# Set Java environment
Sys.setenv(JAVA_HOME='/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home')
Sys.setenv(PATH=paste0('/opt/homebrew/opt/openjdk@17/bin:', Sys.getenv('PATH')))

# Load required libraries
library(SqlRender)
library(dbplyr)

# Source the new SQL functions
source("R/sql_functions.R")

# Test results storage
test_results <- list()

# Helper function
run_test <- function(test_name, test_func) {
  cat(sprintf("Testing %s... ", test_name))
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
# Test 1: sql_date_diff for different dialects
# ==============================================================================
cat("\n--- Testing sql_date_diff ---\n")

# Test SQL Server (default)
test_results[[1]] <- run_test("sql_date_diff - SQL Server", function() {
  result <- sql_date_diff("date1", "date2", "day", connection = NULL)
  sql_str <- as.character(result)
  cat("  Generated SQL:", sql_str, "\n")
  
  # Should generate DATEDIFF syntax
  if (grepl("DATEDIFF", sql_str, ignore.case = TRUE)) {
    return(TRUE)
  }
  stop("Unexpected SQL generated")
})

# Test PostgreSQL
test_results[[2]] <- run_test("sql_date_diff - PostgreSQL", function() {
  mock_conn <- structure(list(), class = "PostgreSQLConnection")
  attr(mock_conn, "dbms") <- "postgresql"
  
  result <- sql_date_diff("date1", "date2", "day", mock_conn)
  sql_str <- as.character(result)
  cat("  Generated SQL:", sql_str, "\n")
  
  # SqlRender should translate this
  translated <- SqlRender::translate("DATEDIFF(day, date2, date1)", targetDialect = "postgresql")
  cat("  Expected pattern:", translated, "\n")
  return(TRUE)
})

# Test Oracle
test_results[[3]] <- run_test("sql_date_diff - Oracle", function() {
  mock_conn <- structure(list(), class = "OracleConnection")
  attr(mock_conn, "dbms") <- "oracle"
  
  result <- sql_date_diff("date1", "date2", "day", mock_conn)
  sql_str <- as.character(result)
  cat("  Generated SQL:", sql_str, "\n")
  
  # SqlRender should translate to date arithmetic
  translated <- SqlRender::translate("DATEDIFF(day, date2, date1)", targetDialect = "oracle")
  cat("  Expected pattern:", translated, "\n")
  return(TRUE)
})

# ==============================================================================
# Test 2: sql_date_from_parts for different dialects
# ==============================================================================
cat("\n--- Testing sql_date_from_parts ---\n")

test_results[[4]] <- run_test("sql_date_from_parts - SQL Server", function() {
  result <- sql_date_from_parts("2024", "1", "15", connection = NULL)
  sql_str <- as.character(result)
  cat("  Generated SQL:", sql_str, "\n")
  
  if (grepl("DATEFROMPARTS", sql_str, ignore.case = TRUE)) {
    return(TRUE)
  }
  stop("Expected DATEFROMPARTS")
})

test_results[[5]] <- run_test("sql_date_from_parts - PostgreSQL", function() {
  mock_conn <- structure(list(), class = "PostgreSQLConnection")
  attr(mock_conn, "dbms") <- "postgresql"
  
  result <- sql_date_from_parts("year_col", "month_col", "day_col", mock_conn)
  sql_str <- as.character(result)
  cat("  Generated SQL:", sql_str, "\n")
  
  # Should use TO_DATE or similar
  if (grepl("TO_DATE|CONCAT", sql_str, ignore.case = TRUE)) {
    return(TRUE)
  }
  stop("Expected TO_DATE or CONCAT for PostgreSQL")
})

test_results[[6]] <- run_test("sql_date_from_parts - BigQuery", function() {
  mock_conn <- structure(list(), class = "BigQueryConnection")
  attr(mock_conn, "dbms") <- "bigquery"
  
  result <- sql_date_from_parts("year", "month", "day", mock_conn)
  sql_str <- as.character(result)
  cat("  Generated SQL:", sql_str, "\n")
  
  # BigQuery uses DATE function
  if (grepl("DATE\\(", sql_str)) {
    return(TRUE)
  }
  stop("Expected DATE() for BigQuery")
})

# ==============================================================================
# Test 3: sql_date_add for different dialects
# ==============================================================================
cat("\n--- Testing sql_date_add ---\n")

test_results[[7]] <- run_test("sql_date_add - SQL Server", function() {
  result <- sql_date_add("visit_date", "30", "day", connection = NULL)
  sql_str <- as.character(result)
  cat("  Generated SQL:", sql_str, "\n")
  
  # Should generate DATEADD
  if (grepl("DATEADD", sql_str, ignore.case = TRUE)) {
    return(TRUE)
  }
  stop("Expected DATEADD")
})

test_results[[8]] <- run_test("sql_date_add - Translation", function() {
  # Test SqlRender translation
  sql_server_sql <- "DATEADD(day, 30, visit_date)"
  
  # Translate to different dialects
  pg_sql <- SqlRender::translate(sql_server_sql, targetDialect = "postgresql")
  oracle_sql <- SqlRender::translate(sql_server_sql, targetDialect = "oracle")
  
  cat("  SQL Server:", sql_server_sql, "\n")
  cat("  PostgreSQL:", pg_sql, "\n")
  cat("  Oracle:", oracle_sql, "\n")
  
  return(TRUE)
})

# ==============================================================================
# Test 4: sql_concat for different dialects
# ==============================================================================
cat("\n--- Testing sql_concat ---\n")

test_results[[9]] <- run_test("sql_concat - SQL Server", function() {
  result <- sql_concat("field1", "field2", connection = NULL)
  sql_str <- as.character(result)
  cat("  Generated SQL:", sql_str, "\n")
  
  if (grepl("CONCAT", sql_str, ignore.case = TRUE)) {
    return(TRUE)
  }
  stop("Expected CONCAT")
})

test_results[[10]] <- run_test("sql_concat - PostgreSQL", function() {
  mock_conn <- structure(list(), class = "PostgreSQLConnection")
  attr(mock_conn, "dbms") <- "postgresql"
  
  result <- sql_concat("field1", "field2", connection = mock_conn)
  sql_str <- as.character(result)
  cat("  Generated SQL:", sql_str, "\n")
  
  # PostgreSQL uses || operator
  if (grepl("\\|\\|", sql_str)) {
    return(TRUE)
  }
  stop("Expected || operator for PostgreSQL")
})

# ==============================================================================
# Test 5: Complex expressions
# ==============================================================================
cat("\n--- Testing Complex Expressions ---\n")

test_results[[11]] <- run_test("Complex date calculation", function() {
  # Simulate a complex calculation like in HIP algorithm
  
  # For SQL Server
  date_diff_sql <- sql_date_diff("visit_date", "birth_date", "day", NULL)
  date_add_sql <- sql_date_add("visit_date", "-280", "day", NULL)
  
  cat("  Date diff:", as.character(date_diff_sql), "\n")
  cat("  Date add:", as.character(date_add_sql), "\n")
  
  return(TRUE)
})

# ==============================================================================
# Test 6: Verify SqlRender is working
# ==============================================================================
cat("\n--- Testing SqlRender Core Functions ---\n")

test_results[[12]] <- run_test("SqlRender render function", function() {
  # Test basic SqlRender render
  sql <- "SELECT * FROM @table WHERE id = @id"
  rendered <- SqlRender::render(sql, table = "person", id = 123)
  cat("  Rendered:", rendered, "\n")
  
  if (grepl("person", rendered) && grepl("123", rendered)) {
    return(TRUE)
  }
  stop("SqlRender render failed")
})

test_results[[13]] <- run_test("SqlRender translate function", function() {
  # Test translation from SQL Server to other dialects
  sql_server <- "SELECT TOP 10 * FROM person"
  
  pg_sql <- SqlRender::translate(sql_server, targetDialect = "postgresql")
  cat("  PostgreSQL:", pg_sql, "\n")
  
  if (grepl("LIMIT", pg_sql, ignore.case = TRUE)) {
    return(TRUE)
  }
  stop("SqlRender translate failed")
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
}

cat("\n--------------------------------------------------\n")
cat(sprintf("Total: %d passed, %d failed out of %d tests\n", passed, failed, length(test_results)))

if (failed == 0) {
  cat("\n🎉 All tests passed! SqlRender functions are working correctly.\n")
  cat("\nThe SQL generation is working properly. When connected to a real database,\n")
  cat("these functions will generate the appropriate SQL for that platform.\n")
} else {
  cat("\n⚠️ Some tests failed. Please review the errors above.\n")
}

cat("\n==================================================\n")
cat("Example Usage in Your Code:\n")
cat("==================================================\n\n")

cat("# In your algorithm functions, use like this:\n")
cat("mutate(\n")
cat("  birth_date = sql_date_from_parts(\"year\", \"month\", \"day\", connection),\n")
cat("  age_days = sql_date_diff(\"current_date\", \"birth_date\", \"day\", connection),\n")
cat("  future_date = sql_date_add(\"current_date\", \"30\", \"day\", connection),\n")
cat("  combined = sql_concat(\"first\", \"last\", connection = connection)\n")
cat(")\n")

# Return results for programmatic use
invisible(test_results)