#!/usr/bin/env Rscript

# Script to update hip_algorithm.R with SqlRender wrappers
# This script performs systematic replacement of SQL Server-specific functions

library(stringr)

# Read the file
file_path <- "R/hip_algorithm.R"
content <- readLines(file_path)

# Backup original
writeLines(content, paste0(file_path, ".backup"))

# Define replacements for DATEDIFF
# Pattern: sql("DATEDIFF(day, date2, date1)") -> sql_date_diff("date1", "date2", "day", connection)
content <- str_replace_all(content, 
  'sql\\("DATEDIFF\\(day, ([^,]+), ([^)]+)\\)"\\)', 
  'sql_date_diff("\\2", "\\1", "day", connection)')

# Handle DATEADD
# Pattern: sql("DATEADD(day, n, date)") -> sql_date_add("date", "n", "day", connection)
content <- str_replace_all(content,
  'sql\\("DATEADD\\(day, ([^,]+), ([^)]+)\\)"\\)',
  'sql_date_add("\\2", "\\1", "day", connection)')

# Handle concat
# Pattern: sql("concat(field1, field2)") -> sql_concat("field1", "field2", connection = connection)
content <- str_replace_all(content,
  'sql\\("concat\\(([^,]+), ([^)]+)\\)"\\)',
  'sql_concat("\\1", "\\2", connection = connection)')

# Handle DATEFROMPARTS (already done manually but check)
content <- str_replace_all(content,
  'sql\\("DATEFROMPARTS\\(([^,]+), ([^,]+), ([^)]+)\\)"\\)',
  'sql_date_from_parts("\\1", "\\2", "\\3", connection)')

# Add connection parameter to function signatures that need it
# This is more complex and needs careful handling

# Function signatures that need connection parameter
functions_needing_connection <- c(
  "final_visits",
  "add_stillbirth", 
  "add_ectopic",
  "add_abortion",
  "add_delivery",
  "calculate_start",
  "gestation_episodes",
  "add_gestation",
  "clean_episodes",
  "remove_overlaps"
)

for (func_name in functions_needing_connection) {
  # Find the function definition line
  pattern <- paste0("^", func_name, " <- function\\(")
  func_lines <- grep(pattern, content)
  
  if (length(func_lines) > 0) {
    func_line_num <- func_lines[1]
    func_line <- content[func_line_num]
    
    # Check if connection parameter already exists
    if (!grepl("connection", func_line)) {
      # Add connection parameter before the closing parenthesis
      content[func_line_num] <- str_replace(func_line, "\\)\\s*\\{", ", connection = NULL) {")
    }
    
    # Add connection extraction code after function definition
    if (func_line_num < length(content)) {
      next_line <- content[func_line_num + 1]
      # Check if extraction code already exists
      if (!grepl("Extract connection", next_line)) {
        extraction_code <- paste0(
          "  # Extract connection if not provided\n",
          "  if (is.null(connection) && inherits(", 
          switch(func_name,
            "final_visits" = "initial_pregnant_cohort_df",
            "add_stillbirth" = "final_stillbirth_visits_df",
            "add_ectopic" = "add_stillbirth_df",
            "add_abortion" = "add_ectopic_df",
            "add_delivery" = "add_abortion_df",
            "calculate_start" = "add_delivery_df",
            "gestation_episodes" = "gestation_visits_df",
            "add_gestation" = "calculate_start_df",
            "clean_episodes" = "add_gestation_df",
            "remove_overlaps" = "clean_episodes_df",
            "initial_pregnant_cohort_df"
          ),
          ", c(\"tbl_lazy\", \"tbl_sql\"))) {\n",
          "    connection <- ",
          switch(func_name,
            "final_visits" = "initial_pregnant_cohort_df",
            "add_stillbirth" = "final_stillbirth_visits_df",
            "add_ectopic" = "add_stillbirth_df",
            "add_abortion" = "add_ectopic_df",
            "add_delivery" = "add_abortion_df",
            "calculate_start" = "add_delivery_df",
            "gestation_episodes" = "gestation_visits_df",
            "add_gestation" = "calculate_start_df",
            "clean_episodes" = "add_gestation_df",
            "remove_overlaps" = "clean_episodes_df",
            "initial_pregnant_cohort_df"
          ),
          "$src$con\n",
          "  }"
        )
        
        # Insert the extraction code
        content <- append(content, extraction_code, after = func_line_num)
      }
    }
  }
}

# Write the updated content
writeLines(content, file_path)

cat("Updated hip_algorithm.R with SqlRender wrappers\n")
cat("Backup saved as", paste0(file_path, ".backup"), "\n")
cat("\nSummary of changes:\n")
cat("- Replaced sql(\"DATEDIFF(...)\") with sql_date_diff()\n")
cat("- Replaced sql(\"DATEADD(...)\") with sql_date_add()\n")
cat("- Replaced sql(\"concat(...)\") with sql_concat()\n")
cat("- Added connection parameters to functions\n")
cat("\nPlease review the changes and test with your database.\n")