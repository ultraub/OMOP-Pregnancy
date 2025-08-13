#!/usr/bin/env Rscript

# Test that the package loads correctly with SqlRender functions

cat("==================================================\n")
cat("Testing Package Load with SqlRender Functions\n")
cat("==================================================\n\n")

# Install the package locally
cat("Installing package from source...\n")
install.packages(".", repos = NULL, type = "source", quiet = TRUE)

cat("Loading package...\n")
library(OMOPPregnancy)

# Check if SQL functions are available
cat("\nChecking for SQL functions:\n")
functions_to_check <- c("sql_date_diff", "sql_date_from_parts", 
                       "sql_date_add", "sql_concat")

for (func in functions_to_check) {
  if (exists(func)) {
    cat(sprintf("  ✅ %s is available\n", func))
  } else {
    cat(sprintf("  ❌ %s is NOT available\n", func))
  }
}

# Test a simple SQL generation
cat("\nTesting SQL generation:\n")
result <- sql_date_diff("date1", "date2", "day")
cat("  sql_date_diff result:", as.character(result), "\n")

result <- sql_date_from_parts("2024", "1", "15")
cat("  sql_date_from_parts result:", as.character(result), "\n")

cat("\n✅ Package loads successfully with SqlRender functions!\n")