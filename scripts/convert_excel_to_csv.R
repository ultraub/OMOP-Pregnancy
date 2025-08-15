#!/usr/bin/env Rscript

#' Convert Excel concept files to CSV format
#' 
#' This script converts all Excel concept files to CSV format for better
#' compatibility and parsing. The original Excel files are preserved.

library(readxl)
library(readr)

# Define the files to convert
excel_files <- list(
  "inst/extdata/HIP_concepts.xlsx",
  "inst/extdata/PPS_concepts.xlsx",
  "inst/extdata/Matcho_outcome_limits.xlsx",
  "inst/extdata/Matcho_term_durations.xlsx"
)

# Convert each file
for (excel_file in excel_files) {
  if (file.exists(excel_file)) {
    # Read Excel file
    cat("Reading", excel_file, "...\n")
    df <- read_excel(excel_file)
    
    # Create CSV filename
    csv_file <- sub("\\.xlsx$", ".csv", excel_file)
    
    # Write CSV file
    cat("Writing", csv_file, "...\n")
    write_csv(df, csv_file)
    
    # Report statistics
    cat("  Converted", nrow(df), "rows with", ncol(df), "columns\n")
    cat("  Columns:", paste(names(df), collapse = ", "), "\n\n")
  } else {
    cat("File not found:", excel_file, "\n")
  }
}

cat("Conversion complete!\n")