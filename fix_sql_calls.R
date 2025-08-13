#!/usr/bin/env Rscript

# Script to find and show all SQL function calls that need to be fixed

library(stringr)

# Read the hip_algorithm.R file
hip_content <- readLines("R/hip_algorithm.R")

# Find all lines with sql_date_diff, sql_date_add, sql_date_from_parts, sql_concat
patterns <- c(
  "sql_date_diff",
  "sql_date_add", 
  "sql_date_from_parts",
  "sql_concat"
)

cat("SQL function calls in hip_algorithm.R:\n")
cat("=====================================\n\n")

for (pattern in patterns) {
  lines <- grep(pattern, hip_content, value = TRUE)
  line_nums <- grep(pattern, hip_content)
  
  if (length(lines) > 0) {
    cat("Pattern:", pattern, "\n")
    cat("Found", length(lines), "occurrences\n")
    for (i in seq_along(lines)) {
      cat(sprintf("Line %d: %s\n", line_nums[i], str_trim(lines[i])))
    }
    cat("\n")
  }
}

# Do the same for pps_algorithm.R
pps_content <- readLines("R/pps_algorithm.R")

cat("\nSQL function calls in pps_algorithm.R:\n")
cat("=====================================\n\n")

for (pattern in patterns) {
  lines <- grep(pattern, pps_content, value = TRUE)
  line_nums <- grep(pattern, pps_content)
  
  if (length(lines) > 0) {
    cat("Pattern:", pattern, "\n")
    cat("Found", length(lines), "occurrences\n")
    for (i in seq_along(lines)) {
      cat(sprintf("Line %d: %s\n", line_nums[i], str_trim(lines[i])))
    }
    cat("\n")
  }
}