#' Main Execution Functions for HIPPS Algorithm
#'
#' Primary interface for running the complete HIPPS pregnancy identification algorithm
#'
#' @name run_hipps
#' @import dplyr
#' @import tidyr
#' @import purrr
NULL

#' Run the complete HIPPS algorithm
#'
#' Main function to execute the HIPPS pregnancy identification algorithm
#' on an OMOP CDM database. This maintains the same execution flow as the
#' original All of Us implementation.
#'
#' @param connectionDetails DatabaseConnector connection details or NULL for All of Us
#' @param cdmDatabaseSchema Schema containing CDM tables
#' @param resultsDatabaseSchema Schema for writing results (optional)
#' @param outputFolder Folder for saving results
#' @param config Configuration list or path to config file
#' @param mode "generic" or "allofus"
#' @param conceptDir Directory containing concept set files
#' @param saveIntermediateResults Whether to save intermediate results
#'
#' @return List containing final pregnancy episodes and metadata
#' @export
#'
#' @examples
#' \dontrun{
#' # For generic OMOP CDM
#' connectionDetails <- createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "localhost/cdm"
#' )
#' 
#' results <- run_hipps(
#'   connectionDetails = connectionDetails,
#'   cdmDatabaseSchema = "cdm",
#'   outputFolder = "output/"
#' )
#' 
#' # For All of Us
#' results <- run_hipps(
#'   mode = "allofus",
#'   cdmDatabaseSchema = "aou_cdm",
#'   outputFolder = "output/"
#' )
#' }
run_hipps <- function(connectionDetails = NULL,
                     cdmDatabaseSchema,
                     resultsDatabaseSchema = NULL,
                     outputFolder = getwd(),
                     config = NULL,
                     mode = "generic",
                     conceptDir = NULL,
                     saveIntermediateResults = TRUE) {


  # Start timing
  start_time <- Sys.time()
  
  # Create output folder if it doesn't exist
  if (!dir.exists(outputFolder)) {
    dir.create(outputFolder, recursive = TRUE)
  }
  
  # Load configuration
  if (is.character(config)) {
    config <- load_config(config_file = config, mode = mode)
  } else if (is.null(config)) {
    config <- load_config(mode = mode)
  }
  
  # Validate configuration
  validate_config(config)
  
  # Create database connection
  message("Connecting to database...")
  con <- create_connection(
    connectionDetails = connectionDetails,
    mode = mode,
    cdmDatabaseSchema = cdmDatabaseSchema,
    resultsDatabaseSchema = resultsDatabaseSchema
  )
  
  # Validate connection
  validate_connection(con, cdmDatabaseSchema)
  
  # Get CDM version
  cdm_version <- get_cdm_version(con, cdmDatabaseSchema)
  message(paste("CDM version:", cdm_version))
  
  # Configure algorithm for CDM version
  config <- configure_algorithm(config, cdm_version)
  
  # Load concept sets
  message("Loading concept sets...")
  concept_sets <- load_concept_sets(conceptDir)
  
  # Get table references
  message("Getting CDM table references...")
  person_tbl <- get_cdm_table(con, "person", cdmDatabaseSchema)
  concept_tbl <- get_cdm_table(con, "concept", cdmDatabaseSchema)
  observation_tbl <- get_cdm_table(con, "observation", cdmDatabaseSchema)
  measurement_tbl <- get_cdm_table(con, "measurement", cdmDatabaseSchema)
  condition_occurrence_tbl <- get_cdm_table(con, "condition_occurrence", cdmDatabaseSchema)
  procedure_occurrence_tbl <- get_cdm_table(con, "procedure_occurrence", cdmDatabaseSchema)
  visit_occurrence_tbl <- get_cdm_table(con, "visit_occurrence", cdmDatabaseSchema)
  
  # Create temp tables for concept sets
  message("Creating temporary tables for concept sets...")
  HIP_concepts <- create_temp_table(con, concept_sets$HIP_concepts)
  
  # Debug: Test counting directly using our Spark-specific method
  if (attr(con, "dbms", exact = TRUE) %in% c("spark", "databricks")) {
    cat("\n[DEBUG] Testing direct count on HIP_concepts...\n")
    tryCatch({
      # Use our Spark-specific count function
      actual_table <- get_spark_table_name(HIP_concepts)
      cat("[DEBUG] Extracted table name: ", ifelse(is.null(actual_table), "NULL", actual_table), "\n")
      
      if (!is.null(actual_table)) {
        # Try direct count on the actual table name
        count_sql <- paste0("SELECT COUNT(*) AS cnt FROM ", actual_table)
        cat("[DEBUG] Direct count SQL: ", count_sql, "\n")
        count_result <- DatabaseConnector::querySql(con, count_sql)
        cat("[DEBUG] Direct count error: ", ifelse(is.null(count_result), "NULL result", paste("columns:", paste(names(count_result), collapse = ", "))), "\n")
        if (!is.null(count_result) && nrow(count_result) > 0) {
          cat("[DEBUG] HIP_concepts row count: ", count_result[[1]], "\n")
        }
      } else {
        # Try the fallback approach
        count_result <- spark_safe_count(con, HIP_concepts)
        cat("[DEBUG] spark_safe_count result: ", ifelse(is.na(count_result), "NA", as.character(count_result)), "\n")
      }
    }, error = function(e) {
      cat("[DEBUG] Direct count error: ", e$message, "\n")
    })
  }
  
  # Verify HIP_concepts was uploaded correctly
  verify_table_upload(HIP_concepts, "HIP_concepts")
  
  PPS_concepts <- create_temp_table(con, concept_sets$PPS_concepts)
  Matcho_outcome_limits <- concept_sets$Matcho_outcome_limits  # Keep local
  Matcho_term_durations <- create_temp_table(con, concept_sets$Matcho_term_durations)
  
  # Run HIP algorithm
  message("Running HIP algorithm...")
  hip_results <- run_hip_algorithm(
    procedure_occurrence_tbl = procedure_occurrence_tbl,
    measurement_tbl = measurement_tbl,
    observation_tbl = observation_tbl,
    condition_occurrence_tbl = condition_occurrence_tbl,
    person_tbl = person_tbl,
    HIP_concepts = HIP_concepts,
    Matcho_outcome_limits = Matcho_outcome_limits,
    Matcho_term_durations = Matcho_term_durations,
    config = config,
    connection = con
  )
  
  # Run PPS algorithm
  message("Running PPS algorithm...")
  pps_results <- run_pps_algorithm(
    condition_occurrence_tbl = condition_occurrence_tbl,
    procedure_occurrence_tbl = procedure_occurrence_tbl,
    observation_tbl = observation_tbl,
    measurement_tbl = measurement_tbl,
    visit_occurrence_tbl = visit_occurrence_tbl,
    PPS_concepts = PPS_concepts,
    person_tbl = person_tbl,
    config = config,
    connection = con
  )
  
  # Merge episodes
  message("Merging HIP and PPS episodes...")
  merged_episodes <- merge_episodes(
    hip_episodes = hip_results$episodes,
    pps_episodes = pps_results,
    initial_pregnant_cohort = hip_results$initial_cohort,
    config = config,
    connection = con
  )
  
  # Calculate estimated start dates
  message("Calculating estimated start dates...")
  final_episodes <- calculate_esd(
    merged_episodes = merged_episodes,
    concept_tbl = concept_tbl,
    condition_occurrence_tbl = condition_occurrence_tbl,
    observation_tbl = observation_tbl,
    measurement_tbl = measurement_tbl,
    procedure_occurrence_tbl = procedure_occurrence_tbl,
    PPS_concepts = PPS_concepts,
    Matcho_term_durations = Matcho_term_durations,
    config = config,
    connection = con
  )
  
  # Compute final results
  message("Computing final results...")
  # Always pass connection for consistency
  final_episodes_computed <- compute_table(final_episodes, connection = con)
  
  # Collect results if needed
  if (saveIntermediateResults) {
    message("Saving results...")
    results_local <- collect(final_episodes_computed)
    
    # Save results
    saveRDS(results_local, file.path(outputFolder, "pregnancy_episodes.rds"))
    write.csv(results_local, file.path(outputFolder, "pregnancy_episodes.csv"), row.names = FALSE)
  }
  
  # Generate summary statistics
  message("Generating summary statistics...")
  summary_stats <- generate_summary_statistics(final_episodes_computed)
  
  # Close connection
  close_connection(con)
  
  # Calculate runtime
  end_time <- Sys.time()
  runtime <- difftime(end_time, start_time, units = "mins")
  
  # Return results
  results <- list(
    episodes = final_episodes_computed,
    summary = summary_stats,
    config = config,
    cdm_version = cdm_version,
    runtime = runtime,
    mode = mode
  )
  
  message(paste("HIPPS algorithm completed in", round(runtime, 2), "minutes"))
  
  return(results)
}

#' Run HIP algorithm component
#'
#' @param procedure_occurrence_tbl Procedure table
#' @param measurement_tbl Measurement table
#' @param observation_tbl Observation table
#' @param condition_occurrence_tbl Condition table
#' @param person_tbl Person table
#' @param HIP_concepts HIP concepts
#' @param Matcho_outcome_limits Outcome limits
#' @param Matcho_term_durations Term durations
#' @param config Configuration
#'
#' @return HIP algorithm results
#' @export
run_hip_algorithm <- function(procedure_occurrence_tbl,
                            measurement_tbl,
                            observation_tbl,
                            condition_occurrence_tbl,
                            person_tbl,
                            HIP_concepts,
                            Matcho_outcome_limits,
                            Matcho_term_durations,
                            config,
                            connection = NULL) {
  
  # Get initial cohort
  initial_pregnant_cohort_df <- initial_pregnant_cohort(
    procedure_occurrence_tbl,
    measurement_tbl,
    observation_tbl,
    condition_occurrence_tbl,
    person_tbl,
    HIP_concepts,
    config = config,
    connection = connection
  )
  initial_pregnant_cohort_df <- compute_table(initial_pregnant_cohort_df, connection = connection)
  
  # Get outcome visits
  final_abortion_visits_df <- final_visits(
    initial_pregnant_cohort_df,
    Matcho_outcome_limits,
    c("AB", "SA"),
    connection = connection
  )
  final_abortion_visits_df <- compute_table(final_abortion_visits_df, connection = connection)
  
  final_delivery_visits_df <- final_visits(
    initial_pregnant_cohort_df,
    Matcho_outcome_limits,
    "DELIV",
    connection = connection
  )
  final_delivery_visits_df <- compute_table(final_delivery_visits_df, connection = connection)
  
  final_ectopic_visits_df <- final_visits(
    initial_pregnant_cohort_df,
    Matcho_outcome_limits,
    "ECT",
    connection = connection
  )
  final_ectopic_visits_df <- compute_table(final_ectopic_visits_df, connection = connection)
  
  final_stillbirth_visits_df <- final_visits(
    initial_pregnant_cohort_df,
    Matcho_outcome_limits,
    "SB",
    connection = connection
  )
  final_stillbirth_visits_df <- compute_table(final_stillbirth_visits_df, connection = connection)
  
  final_livebirth_visits_df <- final_visits(
    initial_pregnant_cohort_df,
    Matcho_outcome_limits,
    "LB",
    connection = connection
  )
  final_livebirth_visits_df <- compute_table(final_livebirth_visits_df, connection = connection)
  
  # Add episodes hierarchically
  add_stillbirth_df <- add_stillbirth(
    final_stillbirth_visits_df,
    final_livebirth_visits_df,
    Matcho_outcome_limits,
    connection = connection
  )
  
  add_ectopic_df <- add_ectopic(
    add_stillbirth_df,
    Matcho_outcome_limits,
    final_ectopic_visits_df,
    connection = connection
  )
  
  add_abortion_df <- add_abortion(
    add_ectopic_df,
    Matcho_outcome_limits,
    final_abortion_visits_df,
    connection = connection
  )
  
  add_delivery_df <- add_delivery(
    add_abortion_df,
    Matcho_outcome_limits,
    final_delivery_visits_df,
    connection = connection
  )
  
  # Calculate start dates
  calculate_start_df <- calculate_start(add_delivery_df, Matcho_term_durations, connection = connection)
  calculate_start_df <- compute_table(calculate_start_df, connection = connection)
  
  # Gestation-based episodes
  gestation_visits_df <- gestation_visits(initial_pregnant_cohort_df, connection = connection)
  gestation_episodes_df <- gestation_episodes(gestation_visits_df, connection = connection)
  get_min_max_gestation_df <- get_min_max_gestation(gestation_episodes_df, connection = connection)
  
  # Check if we have any gestation data
  gest_count <- safe_count(get_min_max_gestation_df)
  if (is.na(gest_count) || gest_count == 0) {
    cat("[WARNING] No gestation-based episodes found. Using outcome-based dating only.\n")
    # Create empty gestation data frame with required structure
    if (inherits(calculate_start_df, c("tbl_lazy", "tbl_sql"))) {
      # For database queries, create empty structure
      get_min_max_gestation_df <- calculate_start_df %>%
        head(0) %>%
        mutate(
          min_gestation_age_in_weeks = NA_real_,
          max_gestation_age_in_weeks = NA_real_,
          first_gest_week = NA_real_,
          last_gest_week = NA_real_
        )
    } else {
      # For local data, create empty data frame
      get_min_max_gestation_df <- data.frame(
        person_id = integer(),
        episode = integer(),
        min_gestation_age_in_weeks = numeric(),
        max_gestation_age_in_weeks = numeric(),
        first_gest_week = numeric(),
        last_gest_week = numeric()
      )
    }
  }
  
  # Combine episodes
  add_gestation_df <- add_gestation(calculate_start_df, get_min_max_gestation_df, buffer_days = 28, connection = connection)
  clean_episodes_df <- clean_episodes(add_gestation_df, buffer_days = 28, connection = connection)
  remove_overlaps_df <- remove_overlaps(clean_episodes_df, connection = connection)
  final_episodes_df <- final_episodes(remove_overlaps_df)
  
  # Add episode length
  HIP_episodes_df <- final_episodes_with_length(final_episodes_df, gestation_visits_df, connection = connection)
  HIP_episodes_df <- compute_table(HIP_episodes_df, connection = connection)
  
  return(list(
    episodes = HIP_episodes_df,
    initial_cohort = initial_pregnant_cohort_df
  ))
}

#' Generate summary statistics
#'
#' @param episodes Episode data
#'
#' @return Summary statistics
#' @export
generate_summary_statistics <- function(episodes) {
  
  # Total episodes
  n_episodes <- episodes %>%
    summarise(n = n()) %>%
    pull(n)
  
  # Unique persons
  n_persons <- episodes %>%
    distinct(person_id) %>%
    summarise(n = n()) %>%
    pull(n)
  
  # Episodes by outcome
  episodes_by_outcome <- episodes %>%
    group_by(category) %>%
    summarise(n = n()) %>%
    collect()
  
  # Episodes per person
  episodes_per_person <- episodes %>%
    group_by(person_id) %>%
    summarise(n_episodes = n()) %>%
    summarise(
      mean_episodes = mean(n_episodes, na.rm = TRUE),
      median_episodes = median(n_episodes, na.rm = TRUE),
      max_episodes = max(n_episodes, na.rm = TRUE)
    ) %>%
    collect()
  
  return(list(
    n_episodes = n_episodes,
    n_persons = n_persons,
    episodes_by_outcome = episodes_by_outcome,
    episodes_per_person = episodes_per_person
  ))
}

#' Run PPS algorithm component
#'
#' @param condition_occurrence_tbl Condition table
#' @param procedure_occurrence_tbl Procedure table
#' @param observation_tbl Observation table
#' @param measurement_tbl Measurement table
#' @param visit_occurrence_tbl Visit table
#' @param PPS_concepts PPS concepts
#' @param person_tbl Person table
#' @param config Configuration
#'
#' @return PPS algorithm results
#' @export
run_pps_algorithm <- function(condition_occurrence_tbl,
                            procedure_occurrence_tbl,
                            observation_tbl,
                            measurement_tbl,
                            visit_occurrence_tbl,
                            PPS_concepts,
                            person_tbl,
                            config,
                            connection = NULL) {
  
  # Extract connection if not provided
  if (is.null(connection)) {
    # Try to extract from one of the input tables
    if (inherits(condition_occurrence_tbl, c("tbl_lazy", "tbl_sql"))) {
      connection <- condition_occurrence_tbl$src$con
    } else if (inherits(procedure_occurrence_tbl, c("tbl_lazy", "tbl_sql"))) {
      connection <- procedure_occurrence_tbl$src$con
    } else if (inherits(observation_tbl, c("tbl_lazy", "tbl_sql"))) {
      connection <- observation_tbl$src$con
    }
  }
  
  # Validate we have a connection
  if (is.null(connection)) {
    stop("run_pps_algorithm: No database connection available")
  }
  
  # Get input GT concepts
  input_GT_concepts_df <- input_GT_concepts(
    condition_occurrence_tbl,
    procedure_occurrence_tbl,
    observation_tbl,
    measurement_tbl,
    visit_occurrence_tbl,
    PPS_concepts
  )
  
  # Get PPS episodes
  get_PPS_episodes_df <- get_PPS_episodes(
    input_GT_concepts_df,
    PPS_concepts,
    person_tbl,
    config = config,
    connection = connection
  )
  
  # Get episode date ranges
  PPS_episodes_df <- get_episode_max_min_dates(get_PPS_episodes_df, connection)
  PPS_episodes_df <- compute_table(PPS_episodes_df, connection = connection)
  
  return(list(
    episodes = PPS_episodes_df,
    episodes_detailed = get_PPS_episodes_df
  ))
}

#' Merge HIP and PPS episodes
#'
#' @param hip_episodes HIP episodes
#' @param pps_episodes PPS episodes and details
#' @param initial_pregnant_cohort Initial pregnant cohort
#' @param config Configuration
#'
#' @return Merged episodes
#' @export
merge_episodes <- function(hip_episodes,
                         pps_episodes,
                         initial_pregnant_cohort,
                         config,
                         connection = NULL) {
  
  # Check for NULL PPS episodes
  if (is.null(pps_episodes) || is.null(pps_episodes$episodes)) {
    warning("PPS episodes is NULL - returning only HIP episodes")
    return(hip_episodes)
  }
  
  # Get outcomes per episode
  outcomes_per_episode_df <- outcomes_per_episode(
    pps_episodes$episodes,
    pps_episodes$episodes_detailed,
    initial_pregnant_cohort
  )
  
  # Add outcomes to PPS episodes
  PPS_episodes_with_outcomes_df <- add_outcomes(
    outcomes_per_episode_df,
    pps_episodes$episodes
  )
  
  # Merge HIP and PPS episodes
  final_merged_episodes_df <- final_merged_episodes(
    hip_episodes,
    PPS_episodes_with_outcomes_df
  )
  
  # Remove duplicates
  final_merged_episodes_no_duplicates_df <- final_merged_episodes_no_duplicates(
    final_merged_episodes_df
  )
  
  # Add demographic details
  final_merged_episode_detailed_df <- final_merged_episode_detailed(
    final_merged_episodes_no_duplicates_df
  )
  final_merged_episode_detailed_df <- compute_table(final_merged_episode_detailed_df, connection = connection)
  
  return(final_merged_episode_detailed_df)
}

#' Calculate estimated start dates (ESD)
#'
#' @param merged_episodes Merged episodes
#' @param concept_tbl Concept table
#' @param condition_occurrence_tbl Condition table
#' @param observation_tbl Observation table
#' @param measurement_tbl Measurement table
#' @param procedure_occurrence_tbl Procedure table
#' @param PPS_concepts PPS concepts
#' @param Matcho_term_durations Term durations
#' @param config Configuration
#'
#' @return Episodes with estimated start dates
#' @export
calculate_esd <- function(merged_episodes,
                        concept_tbl,
                        condition_occurrence_tbl,
                        observation_tbl,
                        measurement_tbl,
                        procedure_occurrence_tbl,
                        PPS_concepts,
                        Matcho_term_durations,
                        config,
                        connection = NULL) {
  
  # Get timing concepts
  get_timing_concepts_df <- get_timing_concepts(
    concept_tbl,
    condition_occurrence_tbl,
    observation_tbl,
    measurement_tbl,
    procedure_occurrence_tbl,
    merged_episodes,
    PPS_concepts,
    config = config,
    connection = connection
  )
  
  # Get episodes with gestational timing info
  episodes_with_gestational_timing_info_df <- episodes_with_gestational_timing_info(
    get_timing_concepts_df
  )
  
  # Merge with metadata
  final_episodes <- merged_episodes_with_metadata(
    episodes_with_gestational_timing_info_df,
    merged_episodes,
    Matcho_term_durations
  )
  final_episodes <- compute_table(final_episodes, connection = connection)
  
  return(final_episodes)
}
