#' Run OMOP Pregnancy Identification V2
#'
#' Main function to identify pregnancy episodes using the simplified HIPPS algorithm.
#' This version prioritizes database agnosticism and maintainability.
#'
#' @param connection DatabaseConnector connection object
#' @param cdm_database_schema Schema containing CDM tables
#' @param results_database_schema Schema for writing results (optional)
#' @param output_folder Folder for saving results (optional)
#' @param min_age Minimum age for pregnancy identification (default: 15)
#' @param max_age Maximum age for pregnancy identification (default: 56)
#'
#' @return Data frame of pregnancy episodes with the following columns:
#'   - person_id: Person identifier
#'   - episode_number: Sequential episode number per person
#'   - episode_start_date: Estimated pregnancy start date
#'   - episode_end_date: Pregnancy outcome date
#'   - outcome_category: Type of outcome (LB, SB, AB, SA, ECT, DELIV, PREG)
#'   - gestational_age_days: Estimated gestational age at outcome
#'   - algorithm_used: HIP, PPS, or MERGED
#'   - confidence_score: High, Medium, or Low based on data quality
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Create connection to your OMOP CDM database
#' connection <- DatabaseConnector::createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "localhost/cdm",
#'   user = "user",
#'   password = "password"
#' )
#' 
#' # Run pregnancy identification
#' pregnancy_episodes <- run_pregnancy_identification(
#'   connection = connection,
#'   cdm_database_schema = "cdm_schema",
#'   output_folder = "output/"
#' )
#' }
run_pregnancy_identification <- function(
  connection,
  cdm_database_schema,
  results_database_schema = NULL,
  output_folder = NULL,
  min_age = 15,
  max_age = 56
) {
  
  # Start timer
  start_time <- Sys.time()
  
  # Validate inputs
  validate_connection(connection)
  validate_schema(cdm_database_schema)
  
  message("========================================")
  message("OMOP Pregnancy Identification V2")
  message("Database-agnostic implementation")
  message("========================================\n")
  
  # Step 1: Load concept sets from CSV files
  message("Step 1: Loading concept sets...")
  concepts <- load_concept_sets()
  message(sprintf("  - Loaded %d HIP concepts", nrow(concepts$hip_concepts)))
  message(sprintf("  - Loaded %d PPS concepts", nrow(concepts$pps_concepts)))
  
  # Step 2: Extract cohort data (single database operation)
  message("\nStep 2: Extracting data from database...")
  message("  This is the only database operation - all subsequent processing is local")
  
  cohort_data <- extract_pregnancy_cohort(
    connection = connection,
    cdm_schema = cdm_database_schema,
    hip_concepts = concepts$hip_concepts,
    pps_concepts = concepts$pps_concepts,
    min_age = min_age,
    max_age = max_age
  )
  
  message(sprintf("  - Extracted data for %d persons", 
                  n_distinct(cohort_data$persons$person_id)))
  message(sprintf("  - Total records: %d conditions, %d procedures, %d observations, %d measurements",
                  nrow(cohort_data$conditions),
                  nrow(cohort_data$procedures),
                  nrow(cohort_data$observations),
                  nrow(cohort_data$measurements)))
  
  # Step 3: Run HIP algorithm (pure R)
  message("\nStep 3: Running HIP algorithm (pure R)...")
  hip_episodes <- run_hip_algorithm(
    cohort_data = cohort_data,
    matcho_limits = concepts$matcho_limits
  )
  message(sprintf("  - Identified %d HIP episodes", nrow(hip_episodes)))
  
  # Step 4: Run PPS algorithm (pure R)
  message("\nStep 4: Running PPS algorithm (pure R)...")
  pps_episodes <- run_pps_algorithm(
    cohort_data = cohort_data,
    pps_concepts = concepts$pps_concepts
  )
  message(sprintf("  - Identified %d PPS episodes", nrow(pps_episodes)))
  
  # Step 5: Merge episodes (pure R)
  message("\nStep 5: Merging HIP and PPS episodes...")
  final_episodes <- merge_pregnancy_episodes(
    hip_episodes = hip_episodes,
    pps_episodes = pps_episodes
  )
  message(sprintf("  - Final episode count: %d", nrow(final_episodes)))
  
  # Step 6: Add estimated start dates
  message("\nStep 6: Calculating estimated start dates...")
  final_episodes <- calculate_episode_dates(
    episodes = final_episodes,
    gestational_data = cohort_data$gestational_timing
  )
  
  # Step 7: Quality scoring
  message("\nStep 7: Assigning confidence scores...")
  final_episodes <- assign_confidence_scores(final_episodes)
  
  # Print summary statistics
  print_summary_statistics(final_episodes)
  
  # Step 8: Optional - Save results
  if (!is.null(output_folder)) {
    message(sprintf("\nStep 8: Saving results to %s", output_folder))
    save_results(
      episodes = final_episodes,
      output_folder = output_folder,
      connection = connection,
      results_schema = results_database_schema
    )
  }
  
  # Calculate runtime
  end_time <- Sys.time()
  runtime <- difftime(end_time, start_time, units = "secs")
  message(sprintf("\n✅ Complete! Total runtime: %.1f seconds", runtime))
  
  return(final_episodes)
}

#' Validate database connection
#' @noRd
validate_connection <- function(connection) {
  if (!DBI::dbIsValid(connection)) {
    stop("Invalid database connection")
  }
  
  # Test connection with simple query
  tryCatch({
    DatabaseConnector::querySql(connection, "SELECT 1 AS test")
  }, error = function(e) {
    stop("Cannot connect to database: ", e$message)
  })
}

#' Validate schema exists
#' @noRd
validate_schema <- function(schema) {
  if (is.null(schema) || schema == "") {
    stop("CDM database schema must be specified")
  }
}

#' Print summary statistics
#' @noRd
print_summary_statistics <- function(episodes) {
  
  message("\n========================================")
  message("Summary Statistics")
  message("========================================")
  
  # Overall counts
  message(sprintf("Total episodes: %d", nrow(episodes)))
  message(sprintf("Unique persons: %d", n_distinct(episodes$person_id)))
  message(sprintf("Episodes per person: %.1f (mean)", 
                  nrow(episodes) / n_distinct(episodes$person_id)))
  
  # By outcome
  message("\nEpisodes by outcome:")
  outcome_counts <- episodes %>%
    count(outcome_category) %>%
    arrange(desc(n))
  
  for (i in 1:nrow(outcome_counts)) {
    message(sprintf("  %s: %d (%.1f%%)", 
                    outcome_counts$outcome_category[i],
                    outcome_counts$n[i],
                    100 * outcome_counts$n[i] / nrow(episodes)))
  }
  
  # By algorithm
  message("\nEpisodes by algorithm:")
  algo_counts <- episodes %>%
    count(algorithm_used) %>%
    arrange(desc(n))
  
  for (i in 1:nrow(algo_counts)) {
    message(sprintf("  %s: %d (%.1f%%)", 
                    algo_counts$algorithm_used[i],
                    algo_counts$n[i],
                    100 * algo_counts$n[i] / nrow(episodes)))
  }
  
  # By confidence
  message("\nEpisodes by confidence:")
  conf_counts <- episodes %>%
    count(confidence_score) %>%
    arrange(desc(n))
  
  for (i in 1:nrow(conf_counts)) {
    message(sprintf("  %s: %d (%.1f%%)", 
                    conf_counts$confidence_score[i],
                    conf_counts$n[i],
                    100 * conf_counts$n[i] / nrow(episodes)))
  }
}