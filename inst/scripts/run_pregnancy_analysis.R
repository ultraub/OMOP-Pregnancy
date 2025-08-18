#!/usr/bin/env Rscript

#' Run OMOP Pregnancy Analysis V2
#'
#' Main execution script that uses environment variables for configuration
#' and includes date correction for SQL Server epoch issues.

# Load required libraries
library(DatabaseConnector)
library(SqlRender)
library(dplyr)
library(lubridate)
library(readr)

# Load environment variables from .env file
if (file.exists(".env")) {
  env_lines <- readLines(".env", warn = FALSE)
  for (line in env_lines) {
    line <- trimws(line)
    if (nchar(line) > 0 && !startsWith(line, "#")) {
      parts <- strsplit(line, "=", fixed = TRUE)[[1]]
      if (length(parts) >= 2) {
        key <- trimws(parts[1])
        value <- trimws(paste(parts[-1], collapse = "="))
        # Remove quotes if present
        value <- gsub("^['\"]|['\"]$", "", value)
        do.call(Sys.setenv, setNames(list(value), key))
      }
    }
  }
  message("✓ Loaded environment variables from .env file")
} else {
  # Check for template files
  templates <- list.files("inst/templates", pattern = "^\\.env", full.names = TRUE)
  if (length(templates) > 0) {
    message("No .env file found. Available templates:")
    for (tmpl in templates) {
      message("  - ", basename(tmpl))
    }
    stop("Copy an appropriate template to .env and update with your settings")
  } else {
    stop("No .env file found. Please create one with your database configuration.")
  }
}

# Source all R files in the package
source_dir <- function(path) {
  if (!dir.exists(path)) {
    stop(sprintf("Directory %s does not exist", path))
  }
  files <- list.files(path, pattern = "\\.R$", recursive = TRUE, full.names = TRUE)
  for (file in files) {
    tryCatch({
      source(file)
    }, error = function(e) {
      warning(sprintf("Failed to load %s: %s", basename(file), e$message))
    })
  }
}

# Source all components
message("Loading R components...")
source_dir("R/00_connection")
source_dir("R/00_concepts")
source_dir("R/01_extraction")
source_dir("R/02_algorithms")
source_dir("R/03_results")
source_dir("R/03_utilities")

# Get database configuration from environment
# Support both SQL_ prefix (for compatibility) and DB_ prefix
dbms <- Sys.getenv("SQL_DBMS")
if (dbms == "") dbms <- Sys.getenv("DB_TYPE")
if (dbms == "") dbms <- Sys.getenv("OMOP_ENV")
if (dbms == "") dbms <- "sql server"
dbms <- tolower(dbms)

server <- Sys.getenv("SQL_SERVER")
if (server == "") server <- Sys.getenv("DB_SERVER")

database <- Sys.getenv("SQL_DATABASE")
if (database == "") database <- Sys.getenv("DB_DATABASE")

cdm_schema <- Sys.getenv("SQL_CDM_SCHEMA")
if (cdm_schema == "") cdm_schema <- Sys.getenv("CDM_SCHEMA", "dbo")

vocabulary_schema <- Sys.getenv("SQL_VOCABULARY_SCHEMA")
if (vocabulary_schema == "") vocabulary_schema <- Sys.getenv("VOCABULARY_SCHEMA", cdm_schema)

results_schema <- Sys.getenv("SQL_RESULTS_SCHEMA")
if (results_schema == "") results_schema <- Sys.getenv("RESULTS_SCHEMA", "")
if (results_schema == "") results_schema <- NULL

output_folder <- Sys.getenv("OUTPUT_FOLDER", "output")

# Check if using Windows authentication
use_windows_auth <- tolower(Sys.getenv("USE_WINDOWS_AUTH", "false")) %in% c("true", "1", "yes")

# Function to correct epoch conversion issues in dates
correct_epoch_dates <- function(episodes) {
  if (nrow(episodes) == 0) return(episodes)
  
  # Ensure date columns are Date objects
  episodes <- episodes %>%
    mutate(
      episode_start_date = as.Date(episode_start_date),
      episode_end_date = as.Date(episode_end_date)
    )
  
  # Check for epoch conversion issue
  episodes <- episodes %>%
    mutate(
      calc_gest_days = as.numeric(episode_end_date - episode_start_date),
      has_epoch_issue = (
        # Classic pattern: start after 1970, end before 1960
        (year(episode_start_date) > 1970 & year(episode_end_date) < 1960) |
        # Or: negative gestational age (end before start)
        (calc_gest_days < 0) |
        # Or: gestational age way off (>1000 days difference)
        (abs(calc_gest_days - gestational_age_days) > 1000)
      )
    )
  
  epoch_issue_count <- sum(episodes$has_epoch_issue, na.rm = TRUE)
  
  if (epoch_issue_count > 0) {
    message(sprintf("  Detected epoch conversion issue in %d episodes, applying correction...", 
                    epoch_issue_count))
    
    # Correct the episode_end_date by adding the epoch difference (70 years)
    episodes <- episodes %>%
      mutate(
        episode_end_date = if_else(
          has_epoch_issue,
          episode_end_date + 25569,  # Add 70 years (difference between 1900 and 1970 epochs)
          episode_end_date
        )
      )
    
    message("  Date correction applied")
  }
  
  # Clean up temporary columns
  episodes <- episodes %>%
    select(-calc_gest_days, -has_epoch_issue)
  
  return(episodes)
}

# Main execution
tryCatch({
  # Record start time
  start_time <- Sys.time()
  assign("start_time", start_time, envir = .GlobalEnv)
  
  message("\n========================================")
  message("OMOP Pregnancy Identification V2")
  message("Database-Agnostic Implementation")
  message("========================================")
  message(sprintf("Start time: %s", format(start_time, "%Y-%m-%d %H:%M:%S")))
  message(sprintf("Server: %s", server))
  message(sprintf("Database: %s", database))
  message(sprintf("CDM Schema: %s", cdm_schema))
  message("")
  
  # Connect to database using new flexible connection system
  message("Connecting to database...")
  connection <- create_connection_from_env(".env")
  
  # Get schema information from connection attributes (may include cross-database setup)
  if (!is.null(attr(connection, "cdm_schema"))) {
    cdm_schema <- attr(connection, "cdm_schema")
  }
  if (!is.null(attr(connection, "vocabulary_schema"))) {
    vocabulary_schema <- attr(connection, "vocabulary_schema")
  }
  if (!is.null(attr(connection, "results_schema"))) {
    results_schema <- attr(connection, "results_schema")
  }
  
  message("✓ Connected successfully")
  message(sprintf("  Database type: %s", attr(connection, "dbms")))
  message(sprintf("  CDM schema: %s", cdm_schema))
  
  # Step 1: Load concepts
  message("\nStep 1: Loading concept definitions...")
  concepts <- load_concept_sets()
  
  # Enrich HIP concepts with domain information
  if (any(is.na(concepts$hip_concepts$domain_name))) {
    message("  Enriching concepts with domain information...")
    concepts$hip_concepts <- enrich_concepts_with_domains(
      concepts$hip_concepts,
      connection,
      vocabulary_schema
    )
  }
  
  message(sprintf("  ✓ Loaded %d HIP concepts", nrow(concepts$hip_concepts)))
  message(sprintf("  ✓ Loaded %d PPS concepts", nrow(concepts$pps_concepts)))
  message(sprintf("  ✓ Loaded %d outcome categories", nrow(concepts$matcho_limits)))
  
  # Step 2: Extract cohort data
  message("\nStep 2: Extracting cohort data from database...")
  message("  Note: This is the only database operation")
  
  cohort_data <- extract_pregnancy_cohort(
    connection = connection,
    cdm_schema = cdm_schema,
    hip_concepts = concepts$hip_concepts,
    pps_concepts = concepts$pps_concepts,
    min_age = 15,
    max_age = 56,
    use_temp_tables = TRUE
  )
  
  # Report extraction statistics
  message(sprintf("\n  Extracted data for %d persons", 
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
    stop("Exiting: No data to process")
  }
  
  # Step 3: Run HIP algorithm
  message("\nStep 3: Running HIP algorithm...")
  hip_episodes <- run_hip_algorithm(
    cohort_data = cohort_data,
    matcho_limits = concepts$matcho_limits,
    matcho_outcome_limits = concepts$matcho_outcome_limits
  )
  
  message(sprintf("  ✓ Identified %d HIP episodes", nrow(hip_episodes)))
  
  if (nrow(hip_episodes) > 0) {
    hip_summary <- hip_episodes %>%
      count(outcome_category) %>%
      arrange(desc(n))
    message("  HIP outcomes:")
    print(hip_summary)
  }
  
  # Step 4: Run PPS algorithm
  message("\nStep 4: Running PPS algorithm...")
  pps_episodes <- run_pps_algorithm(
    cohort_data = cohort_data,
    pps_concepts = concepts$pps_concepts
  )
  
  message(sprintf("  ✓ Identified %d PPS episodes", nrow(pps_episodes)))
  
  if (nrow(pps_episodes) > 0) {
    pps_summary <- pps_episodes %>%
      count(outcome_category) %>%
      arrange(desc(n))
    message("  PPS outcomes:")
    print(pps_summary)
  }
  
  # Step 5: Merge episodes
  message("\nStep 5: Merging HIP and PPS episodes...")
  merged_episodes <- merge_pregnancy_episodes(
    hip_episodes = hip_episodes,
    pps_episodes = pps_episodes,
    cohort_data = cohort_data
  )
  
  message(sprintf("  ✓ Merged to %d unique episodes", nrow(merged_episodes)))
  
  # Step 6: Refine dates with ESD algorithm
  message("\nStep 6: Refining episode dates with ESD algorithm...")
  final_episodes <- calculate_estimated_start_dates(
    merged_episodes,
    cohort_data,
    concepts$pps_concepts
  )
  
  message(sprintf("  ✓ ESD refined %d episodes", nrow(final_episodes)))
  
  # Step 7: Add confidence scores
  message("\nStep 7: Assigning confidence scores...")
  final_episodes <- assign_confidence_scores(final_episodes)
  
  if ("confidence_score" %in% names(final_episodes)) {
    conf_summary <- final_episodes %>%
      count(confidence_score) %>%
      arrange(desc(n))
    message("  Confidence distribution:")
    print(conf_summary)
  }
  
  # Step 8: Correct date issues and save results
  message("\nStep 8: Validating and saving results...")
  
  # Apply date correction for SQL Server epoch issues
  final_episodes <- correct_epoch_dates(final_episodes)
  
  # Validate dates
  date_check <- final_episodes %>%
    mutate(
      calc_days = as.numeric(episode_end_date - episode_start_date),
      date_match = abs(calc_days - gestational_age_days) < 2
    )
  
  message(sprintf("  Date validation: %d/%d episodes have matching gestational ages",
                  sum(date_check$date_match), nrow(date_check)))
  
  # Create output directory if needed
  if (!dir.exists(output_folder)) {
    dir.create(output_folder, recursive = TRUE)
  }
  
  # Save episodes to CSV
  output_file <- file.path(output_folder, 
                          sprintf("pregnancy_episodes_%s.csv", Sys.Date()))
  write.csv(final_episodes, output_file, row.names = FALSE)
  message(sprintf("  ✓ Saved %d episodes to %s", nrow(final_episodes), output_file))
  
  # Save summary statistics
  summary_stats <- create_episode_summary(final_episodes)
  summary_file <- file.path(output_folder,
                           sprintf("pregnancy_summary_%s.rds", Sys.Date()))
  saveRDS(summary_stats, summary_file)
  message(sprintf("  ✓ Saved summary statistics to %s", summary_file))
  
  # Calculate runtime
  end_time <- Sys.time()
  runtime <- difftime(end_time, start_time, units = "secs")
  
  # Final summary
  message("\n========================================")
  message("SUMMARY")
  message("========================================")
  message(sprintf("Total episodes: %d", nrow(final_episodes)))
  message(sprintf("Unique persons: %d", n_distinct(final_episodes$person_id)))
  message(sprintf("Episodes per person: %.2f", 
                  nrow(final_episodes) / n_distinct(final_episodes$person_id)))
  
  # Outcome summary
  outcome_summary <- final_episodes %>%
    count(outcome_category) %>%
    arrange(desc(n))
  message("\nOutcomes:")
  print(outcome_summary)
  
  # Algorithm summary
  algorithm_summary <- final_episodes %>%
    count(algorithm_used) %>%
    arrange(desc(n))
  message("\nAlgorithms:")
  print(algorithm_summary)
  
  message(sprintf("\nTotal runtime: %.1f seconds", runtime))
  message("\n✅ Analysis complete!")
  
}, error = function(e) {
  message("\n❌ Error occurred:")
  message(e$message)
  traceback()
}, finally = {
  # Always disconnect from database
  if (exists("connection")) {
    tryCatch({
      DatabaseConnector::disconnect(connection)
      message("\n✓ Database connection closed")
    }, error = function(e) {
      warning("Failed to close database connection properly")
    })
  }
})