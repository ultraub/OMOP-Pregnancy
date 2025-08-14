#!/usr/bin/env Rscript

#' Diagnostic script to trace data flow in run_hipps
#' 
#' This script adds logging to understand what data is being returned at each step

library(OMOPPregnancy)
library(dplyr)
library(DBI)

# Function to safely check table contents
check_table_contents <- function(tbl_obj, table_name) {
  message("\n=== Checking ", table_name, " ===")
  
  tryCatch({
    # Get column names
    cols <- colnames(tbl_obj)
    message("Columns: ", paste(cols, collapse = ", "))
    
    # Count rows
    row_count <- tbl_obj %>%
      count() %>%
      collect() %>%
      pull(n)
    message("Row count: ", row_count)
    
    # Sample first few rows if any exist
    if (row_count > 0) {
      sample_data <- tbl_obj %>%
        head(5) %>%
        collect()
      message("First few rows:")
      print(sample_data)
    } else {
      message("No data in table")
    }
    
    return(row_count)
  }, error = function(e) {
    message("Error checking ", table_name, ": ", e$message)
    return(0)
  })
}

# Modified run_hip_algorithm with diagnostics
run_hip_algorithm_diagnostic <- function(procedure_occurrence_tbl,
                                        measurement_tbl,
                                        observation_tbl,
                                        condition_occurrence_tbl,
                                        person_tbl,
                                        HIP_concepts,
                                        Matcho_outcome_limits,
                                        Matcho_term_durations,
                                        config,
                                        connection = NULL) {
  
  message("\n========================================")
  message("Starting HIP Algorithm Diagnostic")
  message("========================================")
  
  # Step 1: Initial pregnant cohort
  message("\n--- Step 1: Initial Pregnant Cohort ---")
  initial_pregnant_cohort_df <- initial_pregnant_cohort(
    procedure_occurrence_tbl,
    measurement_tbl,
    observation_tbl,
    condition_occurrence_tbl,
    person_tbl,
    HIP_concepts,
    config = config,
    connection = connection
  ) %>%
    compute_table()
  
  cohort_count <- check_table_contents(initial_pregnant_cohort_df, "initial_pregnant_cohort")
  
  # Check for presence of key columns
  if ("visit_date" %in% colnames(initial_pregnant_cohort_df)) {
    message("✓ visit_date column present")
  } else {
    message("✗ visit_date column MISSING")
  }
  
  if ("gest_value" %in% colnames(initial_pregnant_cohort_df)) {
    # Check how many records have gest_value
    gest_count <- initial_pregnant_cohort_df %>%
      filter(!is.na(gest_value)) %>%
      count() %>%
      collect() %>%
      pull(n)
    message("Records with gest_value: ", gest_count)
  }
  
  # Step 2: Outcome visits
  message("\n--- Step 2: Outcome Visits ---")
  
  categories <- c("AB", "SA", "DELIV", "ECT", "SB", "LB")
  for (cat in categories) {
    outcome_visits <- final_visits(
      initial_pregnant_cohort_df,
      Matcho_outcome_limits,
      if (cat %in% c("AB", "SA")) c("AB", "SA") else cat
    ) %>%
      compute_table()
    
    outcome_count <- outcome_visits %>%
      count() %>%
      collect() %>%
      pull(n)
    
    message(cat, " visits: ", outcome_count)
  }
  
  # Step 3: Gestation visits
  message("\n--- Step 3: Gestation Visits ---")
  gestation_visits_df <- gestation_visits(initial_pregnant_cohort_df)
  gest_visit_count <- check_table_contents(gestation_visits_df, "gestation_visits")
  
  # Check columns specifically
  gest_cols <- colnames(gestation_visits_df)
  message("Gestation visits columns: ", paste(gest_cols, collapse = ", "))
  
  if (!"visit_date" %in% gest_cols) {
    message("WARNING: visit_date missing from gestation_visits!")
  }
  
  # Step 4: Gestation episodes
  message("\n--- Step 4: Gestation Episodes ---")
  gestation_episodes_df <- gestation_episodes(gestation_visits_df)
  gest_episode_count <- check_table_contents(gestation_episodes_df, "gestation_episodes")
  
  # Check columns
  episode_cols <- colnames(gestation_episodes_df)
  message("Gestation episodes columns: ", paste(episode_cols, collapse = ", "))
  
  if (!"visit_date" %in% episode_cols) {
    message("WARNING: visit_date missing from gestation_episodes!")
  }
  
  # Step 5: Min/Max gestation
  message("\n--- Step 5: Min/Max Gestation ---")
  
  # This is where the error occurs
  tryCatch({
    get_min_max_gestation_df <- get_min_max_gestation(gestation_episodes_df)
    minmax_count <- check_table_contents(get_min_max_gestation_df, "get_min_max_gestation")
    message("✓ get_min_max_gestation completed successfully")
  }, error = function(e) {
    message("✗ get_min_max_gestation FAILED: ", e$message)
    
    # Additional diagnostics
    message("\nDiagnosing failure...")
    message("gestation_episodes_df class: ", paste(class(gestation_episodes_df), collapse = ", "))
    
    # Try to get the SQL query if it's a lazy table
    if (inherits(gestation_episodes_df, c("tbl_lazy", "tbl_sql"))) {
      message("\nSQL Query for gestation_episodes_df:")
      tryCatch({
        sql_query <- dbplyr::sql_render(gestation_episodes_df)
        message(sql_query)
      }, error = function(e2) {
        message("Could not render SQL: ", e2$message)
      })
    }
    
    stop(e)
  })
  
  message("\n========================================")
  message("Diagnostic Complete")
  message("========================================")
  
  # Return diagnostic info
  return(list(
    initial_cohort_count = cohort_count,
    gestation_visits_count = gest_visit_count,
    gestation_episodes_count = gest_episode_count
  ))
}

# Export the function for use
message("Diagnostic function loaded: run_hip_algorithm_diagnostic()")
message("Use this function in place of run_hip_algorithm to get detailed diagnostics")