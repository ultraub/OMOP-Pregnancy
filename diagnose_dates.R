#!/usr/bin/env Rscript

# Diagnostic script to understand date handling issue

library(dplyr)
library(lubridate)

# Test the date conversion logic
test_dates <- function() {
  
  # Simulate what might be happening
  cat("\n=== Testing Date Conversions ===\n")
  
  # Example numeric values that might come from SQL Server
  # These represent days since 1900-01-01
  sql_server_dates <- c(
    27103,  # Should be ~1974
    36526,  # Should be ~2000
    40544,  # Should be ~2011
    44562   # Should be ~2022
  )
  
  cat("\nRaw numeric values (days since 1900-01-01):\n")
  print(sql_server_dates)
  
  # Convert using the origin from parse_date_safe
  converted_dates <- as.Date(sql_server_dates, origin = "1899-12-30")
  cat("\nConverted with origin 1899-12-30:\n")
  print(converted_dates)
  
  # What happens when we aggregate?
  df <- data.frame(
    group = c("A", "A", "B", "B"),
    date = converted_dates
  )
  
  cat("\nOriginal data frame:\n")
  print(df)
  cat("Date column class:", class(df$date), "\n")
  
  # Test aggregation
  result1 <- df %>%
    group_by(group) %>%
    summarise(
      max_date = max(date),
      .groups = "drop"
    )
  
  cat("\nAfter max() aggregation:\n")
  print(result1)
  cat("max_date column class:", class(result1$max_date), "\n")
  
  # Test with as.Date wrapper
  result2 <- df %>%
    group_by(group) %>%
    summarise(
      max_date = as.Date(max(date)),
      .groups = "drop"
    )
  
  cat("\nAfter max() with as.Date wrapper:\n")
  print(result2)
  cat("max_date column class:", class(result2$max_date), "\n")
  
  # Test what happens when dates lose their class
  numeric_dates <- as.numeric(converted_dates)
  cat("\nDates converted to numeric:\n")
  print(numeric_dates)
  
  # Try to convert back
  reconverted <- as.Date(numeric_dates)
  cat("\nReconverted with default origin (1970-01-01):\n")
  print(reconverted)
  
  # Correct reconversion
  reconverted_correct <- as.Date(numeric_dates, origin = "1970-01-01")
  cat("\nReconverted with 1970-01-01 origin:\n")
  print(reconverted_correct)
  
  # Check the actual numeric values in our problematic dates
  cat("\n=== Analyzing Problematic Dates ===\n")
  
  # From the CSV: episode_start_date = 1975-04-15, episode_end_date = 1906-02-08
  start_date <- as.Date("1975-04-15")
  cat("\nCorrect start date:", as.character(start_date), "\n")
  cat("Numeric value (days since 1970-01-01):", as.numeric(start_date), "\n")
  
  # The end date appears to be 1906-02-08 but should be later than 1975
  # With 301 days gestational age, end should be ~1976-02-10
  expected_end <- start_date + 301
  cat("\nExpected end date:", as.character(expected_end), "\n")
  cat("Numeric value:", as.numeric(expected_end), "\n")
  
  # What if the numeric value is interpreted with wrong origin?
  wrong_end <- as.Date("1906-02-08")
  cat("\nWrong end date from CSV:", as.character(wrong_end), "\n")
  cat("Numeric value:", as.numeric(wrong_end), "\n")
  
  # Calculate the difference
  diff_days <- as.numeric(wrong_end) - as.numeric(start_date)
  cat("\nDifference in days:", diff_days, "\n")
  
  # Check if there's a pattern
  cat("\n=== Checking for Pattern ===\n")
  
  # The difference between 1906 and 2006 is 100 years
  # 100 years * 365.25 days = 36525 days
  cat("100 years in days: ~36525\n")
  
  # Let's check if adding 36525 to the wrong date gives us something reasonable
  corrected_date <- wrong_end + 36525
  cat("Wrong date + 36525 days:", as.character(corrected_date), "\n")
  
  # Now let's trace the actual issue
  cat("\n=== Simulating the Issue ===\n")
  
  # If a date is stored as days since 1900-01-01 (SQL Server format)
  # and we interpret it as days since 1970-01-01 (R default)
  sql_numeric <- 27505  # Days since 1900-01-01 for a date around 1975
  
  wrong_interpretation <- as.Date(sql_numeric)  # Uses default origin 1970-01-01
  cat("\nSQL numeric", sql_numeric, "interpreted with default origin:", 
      as.character(wrong_interpretation), "\n")
  
  correct_interpretation <- as.Date(sql_numeric, origin = "1899-12-30")
  cat("Same numeric with SQL Server origin:", 
      as.character(correct_interpretation), "\n")
}

# Run the tests
test_dates()

# Load the actual data if available
if (file.exists("output/pregnancy_episodes_2025-08-18.csv")) {
  cat("\n\n=== Analyzing Actual Data ===\n")
  
  episodes <- read.csv("output/pregnancy_episodes_2025-08-18.csv", 
                       stringsAsFactors = FALSE)
  
  # Check first few rows
  cat("\nFirst 5 rows of actual data:\n")
  print(head(episodes[, c("person_id", "episode_start_date", "episode_end_date", 
                          "gestational_age_days")], 5))
  
  # Parse dates
  episodes$start_date <- as.Date(episodes$episode_start_date)
  episodes$end_date <- as.Date(episodes$episode_end_date)
  
  # Calculate what the gestational age should be
  episodes$calculated_gest <- as.numeric(episodes$end_date - episodes$start_date)
  
  # Compare
  episodes$gest_matches <- abs(episodes$calculated_gest - episodes$gestational_age_days) < 2
  
  cat("\nGestational age calculation check:\n")
  print(table(episodes$gest_matches))
  
  # Look for the pattern
  episodes$year_start <- year(episodes$start_date)
  episodes$year_end <- year(episodes$end_date)
  episodes$year_diff <- episodes$year_end - episodes$year_start
  
  cat("\nYear differences between start and end:\n")
  print(summary(episodes$year_diff))
  
  # Check if end dates are consistently ~70 years before start dates
  problem_rows <- episodes[episodes$year_diff < -50, ]
  if (nrow(problem_rows) > 0) {
    cat("\nRows where end date is >50 years before start date:\n")
    print(head(problem_rows[, c("episode_start_date", "episode_end_date", 
                                "year_diff", "gestational_age_days")]))
  }
}