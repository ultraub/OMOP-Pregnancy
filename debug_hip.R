#!/usr/bin/env Rscript

# Debug script to trace the date issue in HIP algorithm

library(dplyr)
library(lubridate)

# Simulate what happens in the HIP algorithm
test_hip_date_calculation <- function() {
  
  cat("\n=== Simulating HIP Date Calculation ===\n")
  
  # Create a sample outcome_date (as it would come from max(event_date))
  # Let's say this is February 8, 2006
  outcome_date <- as.Date("2006-02-08")
  cat("\nOriginal outcome_date:", as.character(outcome_date), "\n")
  cat("Class:", class(outcome_date), "\n")
  cat("Numeric value:", as.numeric(outcome_date), "\n")
  
  # Now simulate the calculation from HIP algorithm
  # From the code: as.Date(outcome_date) - 280
  
  # Test 1: Direct subtraction
  test1 <- outcome_date - 280
  cat("\nTest 1: outcome_date - 280\n")
  cat("Result:", as.character(test1), "\n")
  cat("Class:", class(test1), "\n")
  cat("Numeric:", as.numeric(test1), "\n")
  
  # Test 2: With as.Date wrapper
  test2 <- as.Date(outcome_date) - 280
  cat("\nTest 2: as.Date(outcome_date) - 280\n")
  cat("Result:", as.character(test2), "\n")
  cat("Class:", class(test2), "\n")
  cat("Numeric:", as.numeric(test2), "\n")
  
  # Test 3: What if outcome_date is actually numeric?
  outcome_numeric <- as.numeric(outcome_date)
  cat("\n\nWhat if outcome_date lost its Date class and became numeric?\n")
  cat("Numeric value:", outcome_numeric, "\n")
  
  # If we treat this numeric as a date without specifying origin
  wrong_date <- as.Date(outcome_numeric)
  cat("as.Date(numeric) with default origin:", as.character(wrong_date), "\n")
  
  # The numeric value 13187 represents days since 1970-01-01
  # But what if it's being interpreted differently?
  
  # Test different interpretations
  cat("\n=== Testing Different Numeric Interpretations ===\n")
  
  # If the date is stored as days since 1900-01-01 (SQL Server style)
  # 2006-02-08 would be approximately 38755 days since 1900-01-01
  sql_numeric <- as.numeric(as.Date("2006-02-08") - as.Date("1900-01-01"))
  cat("\n2006-02-08 as days since 1900-01-01:", sql_numeric, "\n")
  
  # If this gets interpreted with wrong origin
  wrong_interpretation <- as.Date(sql_numeric, origin = "1970-01-01")
  cat("Interpreted with 1970 origin:", as.character(wrong_interpretation), "\n")
  
  # What about the reverse problem?
  # If we have 1906-02-08 in the output, what's its numeric value?
  wrong_output <- as.Date("1906-02-08")
  cat("\n1906-02-08 numeric (days since 1970):", as.numeric(wrong_output), "\n")
  
  # And what would give us 1906-02-08?
  # Let's work backwards
  target_date <- as.Date("1906-02-08")
  target_numeric <- as.numeric(target_date)  # -23338 days since 1970
  
  # What date + 301 days = 2006-02-08?
  correct_end <- as.Date("2006-02-08")
  correct_start <- correct_end - 301
  cat("\nCorrect start date for end 2006-02-08:", as.character(correct_start), "\n")
  
  # But we're seeing 1975-04-15 as start
  actual_start <- as.Date("1975-04-15")
  calculated_end <- actual_start + 301
  cat("1975-04-15 + 301 days:", as.character(calculated_end), "\n")
  
  # The issue: 1976-02-10 is being displayed as 1906-02-08
  # Difference: exactly 70 years (25550 days)
  date_diff <- as.numeric(as.Date("1976-02-10") - as.Date("1906-02-08"))
  cat("\nDifference between 1976-02-10 and 1906-02-08:", date_diff, "days\n")
  cat("That's approximately:", round(date_diff / 365.25), "years\n")
  
  # Theory: The date calculation is correct but something is subtracting ~70 years
  # 70 years = 25550 days
  # This is close to the Unix epoch offset!
  
  cat("\n=== Checking Epoch Offsets ===\n")
  epoch_1900 <- as.Date("1900-01-01")
  epoch_1970 <- as.Date("1970-01-01")
  epoch_diff <- as.numeric(epoch_1970 - epoch_1900)
  cat("Days between 1900-01-01 and 1970-01-01:", epoch_diff, "\n")
  
  # So if a date is calculated correctly but then somehow 
  # has 25567 days subtracted from it...
  correct_date <- as.Date("1976-02-10")
  wrong_date <- correct_date - epoch_diff
  cat("\n1976-02-10 minus epoch difference:", as.character(wrong_date), "\n")
  
  cat("\n=== HYPOTHESIS ===\n")
  cat("The dates are being calculated correctly internally,\n")
  cat("but when converted for output, they're losing 70 years (25567 days).\n")
  cat("This is exactly the difference between the 1900 and 1970 epochs.\n")
  cat("\nThe issue might be that dates are being double-converted:\n")
  cat("1. First interpreted as days since 1900 (correct)\n")
  cat("2. Then re-interpreted as days since 1970 (incorrect)\n")
}

# Run the test
test_hip_date_calculation()