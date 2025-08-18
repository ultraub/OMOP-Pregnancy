#!/usr/bin/env Rscript

#' Run OMOP Pregnancy Analysis V2
#'
#' Complete script to execute pregnancy identification on OMOP CDM
#' with database-agnostic architecture.

# Load required libraries
library(DatabaseConnector)
library(SqlRender)
library(dplyr)
library(lubridate)
library(readr)

# Source all R files in the package
source_dir <- function(path) {
  files <- list.files(path, pattern = "\\.R$", recursive = TRUE, full.names = TRUE)
  for (file in files) {
    source(file)
  }
}

# Source all components
source_dir("R/00_concepts")
source_dir("R/01_extraction")
source_dir("R/02_algorithms")
source_dir("R/03_results")

#' Main Execution Function
#'
#' @param connection DatabaseConnector connection object
#' @param cdm_schema Schema containing CDM tables
#' @param vocabulary_schema Schema containing vocabulary tables (often same as CDM)
#' @param results_schema Schema for results (optional)
#' @param output_folder Folder for CSV output
#' @param min_age Minimum age (default: 15)
#' @param max_age Maximum age (default: 56)
#' @param hip_concepts_path Path to HIP concepts CSV
#' @param pps_concepts_path Path to PPS concepts CSV
#' @param matcho_limits_path Path to Matcho limits CSV
#'
#' @return Data frame of pregnancy episodes
#' @export
run_pregnancy_identification <- function(
  connection,
  cdm_schema,
  vocabulary_schema = cdm_schema,
  results_schema = NULL,
  output_folder = "output",
  min_age = 15,
  max_age = 56,
  hip_concepts_path = NULL,
  pps_concepts_path = NULL,
  matcho_limits_path = NULL
) {
  
  # Record start time
  start_time <- Sys.time()
  assign("start_time", start_time, envir = .GlobalEnv)  # For result saving
  
  message("========================================")
  message("OMOP Pregnancy Identification V2")
  message("Database-Agnostic Implementation")
  message("========================================")
  message(sprintf("Start time: %s", format(start_time, "%Y-%m-%d %H:%M:%S")))
  message(sprintf("Database: %s", attr(connection, "dbms")))
  message(sprintf("CDM Schema: %s", cdm_schema))
  message("")
  
  # Step 1: Load concepts
  message("Step 1: Loading concept definitions...")
  # Use load_concept_sets which loads all necessary files including matcho_outcome_limits
  concepts <- load_concept_sets(
    hip_path = hip_concepts_path,
    pps_path = pps_concepts_path,
    matcho_path = matcho_limits_path
  )
  
  # Enrich HIP concepts with domain information from database
  if (any(is.na(concepts$hip_concepts$domain_name))) {
    message("  Enriching concepts with domain information...")
    concepts$hip_concepts <- enrich_concepts_with_domains(
      concepts$hip_concepts,
      connection,
      vocabulary_schema
    )
  }
  
  # Validate concept coverage
  message("  Validating concept coverage...")
  validation <- validate_concept_coverage(
    concepts$hip_concepts,
    concepts$pps_concepts
  )
  
  if (!validation$is_valid) {
    warning("Concept validation found issues - proceeding anyway")
  }
  
  # Step 2: Extract cohort data (single database hit)
  message("\nStep 2: Extracting cohort data from database...")
  message("  Note: This is the only database operation")
  
  cohort_data <- extract_pregnancy_cohort(
    connection = connection,
    cdm_schema = cdm_schema,
    hip_concepts = concepts$hip_concepts,
    pps_concepts = concepts$pps_concepts,
    min_age = min_age,
    max_age = max_age,
    use_temp_tables = TRUE  # Use temp tables for large cohorts
  )
  
  # Report extraction statistics
  message(sprintf("  Extracted data for %d persons", 
                  n_distinct(cohort_data$persons$person_id)))
  message(sprintf("  Records by domain:"))
  message(sprintf("    - Conditions: %d", nrow(cohort_data$conditions)))
  message(sprintf("    - Procedures: %d", nrow(cohort_data$procedures)))
  message(sprintf("    - Observations: %d", nrow(cohort_data$observations)))
  message(sprintf("    - Measurements: %d", nrow(cohort_data$measurements)))
  message(sprintf("    - Gestational timing: %d", nrow(cohort_data$gestational_timing)))
  
  # Check if we have any data
  total_records <- sum(
    nrow(cohort_data$conditions),
    nrow(cohort_data$procedures),
    nrow(cohort_data$observations),
    nrow(cohort_data$measurements)
  )
  
  if (total_records == 0) {
    warning("No pregnancy-related records found in database")
    return(data.frame())
  }
  
  # Step 3: Run HIP algorithm (pure R)
  message("\nStep 3: Running HIP algorithm...")
  message("  Processing in pure R - no database operations")
  
  hip_episodes <- run_hip_algorithm(
    cohort_data = cohort_data,
    matcho_limits = concepts$matcho_limits,
    matcho_outcome_limits = concepts$matcho_outcome_limits
  )
  
  message(sprintf("  Identified %d HIP episodes", nrow(hip_episodes)))
  
  if (nrow(hip_episodes) > 0) {
    hip_summary <- hip_episodes %>%
      count(outcome_category) %>%
      arrange(desc(n))
    message("  HIP outcomes:")
    for (i in 1:min(5, nrow(hip_summary))) {
      message(sprintf("    - %s: %d", 
                      hip_summary$outcome_category[i],
                      hip_summary$n[i]))
    }
  }
  
  # Step 4: Run PPS algorithm (pure R)
  message("\nStep 4: Running PPS algorithm...")
  message("  Processing temporal patterns in pure R")
  
  pps_episodes <- run_pps_algorithm(
    cohort_data = cohort_data,
    pps_concepts = concepts$pps_concepts
  )
  
  message(sprintf("  Identified %d PPS episodes", nrow(pps_episodes)))
  
  if (nrow(pps_episodes) > 0) {
    pps_summary <- pps_episodes %>%
      count(outcome_category) %>%
      arrange(desc(n))
    message("  PPS outcomes:")
    for (i in 1:min(5, nrow(pps_summary))) {
      message(sprintf("    - %s: %d",
                      pps_summary$outcome_category[i],
                      pps_summary$n[i]))
    }
  }
  
  # Step 5: Merge episodes
  message("\nStep 5: Merging HIP and PPS episodes...")
  
  merged_episodes <- merge_pregnancy_episodes(
    hip_episodes = hip_episodes,
    pps_episodes = pps_episodes
  )
  
  message(sprintf("  Merged episode count: %d", nrow(merged_episodes)))
  
  # Report merging statistics
  if (nrow(merged_episodes) > 0) {
    merge_stats <- merged_episodes %>%
      count(algorithm_used) %>%
      arrange(desc(n))
    message("  Episodes by algorithm:")
    for (i in 1:nrow(merge_stats)) {
      message(sprintf("    - %s: %d (%.1f%%)",
                      merge_stats$algorithm_used[i],
                      merge_stats$n[i],
                      100 * merge_stats$n[i] / nrow(merged_episodes)))
    }
  }
  
  # Step 6: Refine dates with ESD algorithm
  message("\nStep 6: Refining episode dates with ESD algorithm...")
  
  final_episodes <- refine_episode_dates(
    episodes = merged_episodes,
    cohort_data = cohort_data,
    pps_concepts = concepts$pps_concepts
  )
  
  if (nrow(final_episodes) > 0) {
    precision_stats <- final_episodes %>%
      count(precision_category) %>%
      arrange(desc(n))
    message("  Date precision categories:")
    for (i in 1:min(5, nrow(precision_stats))) {
      message(sprintf("    - %s: %d (%.1f%%)",
                      precision_stats$precision_category[i],
                      precision_stats$n[i],
                      100 * precision_stats$n[i] / nrow(final_episodes)))
    }
  }
  
  # Step 7: Add confidence scores
  message("\nStep 7: Assigning confidence scores...")
  
  final_episodes <- assign_confidence_scores(final_episodes)
  
  if (nrow(final_episodes) > 0) {
    conf_stats <- final_episodes %>%
      count(confidence_score) %>%
      arrange(desc(n))
    message("  Confidence distribution:")
    for (i in 1:nrow(conf_stats)) {
      message(sprintf("    - %s: %d (%.1f%%)",
                      conf_stats$confidence_score[i],
                      conf_stats$n[i],
                      100 * conf_stats$n[i] / nrow(final_episodes)))
    }
  }
  
  # Step 8: Save results
  if (!is.null(output_folder) && nrow(final_episodes) > 0) {
    message(sprintf("\nStep 8: Saving results to %s", output_folder))
    
    save_pregnancy_results(
      episodes = final_episodes,
      connection = if (!is.null(results_schema)) connection else NULL,
      results_schema = results_schema,
      output_folder = output_folder,
      save_to_database = !is.null(results_schema)
    )
    
    # Also export analysis files
    export_for_analysis(
      episodes = final_episodes,
      output_folder = output_folder,
      formats = c("csv", "rds")
    )
  }
  
  # Calculate runtime
  end_time <- Sys.time()
  runtime <- difftime(end_time, start_time, units = "secs")
  
  message("\n========================================")
  message("SUMMARY")
  message("========================================")
  
  if (nrow(final_episodes) > 0) {
    message(sprintf("Total episodes: %d", nrow(final_episodes)))
    message(sprintf("Unique persons: %d", n_distinct(final_episodes$person_id)))
    message(sprintf("Episodes per person: %.2f", 
                    nrow(final_episodes) / n_distinct(final_episodes$person_id)))
    
    # Gestational age statistics
    gest_stats <- final_episodes %>%
      summarise(
        mean_gest = mean(gestational_age_days, na.rm = TRUE),
        median_gest = median(gestational_age_days, na.rm = TRUE),
        min_gest = min(gestational_age_days, na.rm = TRUE),
        max_gest = max(gestational_age_days, na.rm = TRUE)
      )
    
    message(sprintf("\nGestational age (days):"))
    message(sprintf("  Mean: %.1f", gest_stats$mean_gest))
    message(sprintf("  Median: %.1f", gest_stats$median_gest))
    message(sprintf("  Range: %.0f - %.0f", gest_stats$min_gest, gest_stats$max_gest))
  } else {
    message("No pregnancy episodes identified")
  }
  
  message(sprintf("\nTotal runtime: %.1f seconds", runtime))
  message("✅ Analysis complete!")
  
  return(final_episodes)
}

# Example usage (commented out)
# 
# # Create connection
# connection <- DatabaseConnector::createConnectionDetails(
#   dbms = "postgresql",
#   server = "localhost/cdm",
#   user = "user",
#   password = "password"
# )
# 
# conn <- DatabaseConnector::connect(connection)
# 
# # Run analysis
# episodes <- run_pregnancy_identification(
#   connection = conn,
#   cdm_schema = "cdm_53",
#   vocabulary_schema = "cdm_53",
#   output_folder = "output/",
#   min_age = 15,
#   max_age = 56
# )
# 
# DatabaseConnector::disconnect(conn)