#' Save Pregnancy Episode Results
#'
#' Saves identified pregnancy episodes to database or file system.
#'
#' @param episodes Data frame of pregnancy episodes
#' @param connection DatabaseConnector connection (optional)
#' @param results_schema Schema for results tables (if saving to database)
#' @param output_folder Folder for CSV output (if saving to files)
#' @param save_to_database Logical, whether to save to database
#'
#' @return TRUE if successful
#' @export
save_pregnancy_results <- function(
  episodes,
  connection = NULL,
  results_schema = NULL,
  output_folder = NULL,
  save_to_database = FALSE
) {
  
  if (is.null(episodes) || nrow(episodes) == 0) {
    warning("No episodes to save")
    return(FALSE)
  }
  
  # Add metadata
  episodes_with_meta <- episodes %>%
    mutate(
      analysis_date = Sys.Date(),
      analysis_version = "2.0.0",
      runtime_seconds = as.numeric(Sys.time() - get("start_time", envir = .GlobalEnv))
    )
  
  if (save_to_database && !is.null(connection) && !is.null(results_schema)) {
    save_to_database_tables(episodes_with_meta, connection, results_schema)
  } else if (!is.null(output_folder)) {
    save_to_csv_files(episodes_with_meta, output_folder)
  } else {
    stop("Must specify either database connection or output folder")
  }
  
  return(TRUE)
}

#' Save results to database tables
#' @noRd
save_to_database_tables <- function(episodes, connection, results_schema) {
  
  message("Saving results to database...")
  
  # Create results table if it doesn't exist
  create_sql <- SqlRender::render("
    IF NOT EXISTS (
      SELECT * FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_SCHEMA = '@results_schema' 
      AND TABLE_NAME = 'pregnancy_episodes'
    )
    CREATE TABLE @results_schema.pregnancy_episodes (
      person_id BIGINT,
      episode_number INT,
      episode_start_date DATE,
      episode_end_date DATE,
      outcome_category VARCHAR(20),
      gestational_age_days INT,
      algorithm_used VARCHAR(20),
      analysis_date DATE,
      analysis_version VARCHAR(20)
    );
  ", results_schema = results_schema)
  
  DatabaseConnector::executeSql(connection, create_sql)
  
  # Insert episodes
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = paste(results_schema, "pregnancy_episodes", sep = "."),
    data = episodes,
    dropTableIfExists = FALSE,
    createTable = FALSE,
    tempTable = FALSE,
    progressBar = TRUE
  )
  
  message(sprintf("Saved %d episodes to database", nrow(episodes)))
}

#' Save results to CSV files
#' @noRd  
save_to_csv_files <- function(episodes, output_folder) {
  
  if (!dir.exists(output_folder)) {
    dir.create(output_folder, recursive = TRUE)
  }
  
  # Save main episodes file
  episodes_file <- file.path(output_folder, paste0(
    "pregnancy_episodes_", 
    format(Sys.Date(), "%Y%m%d"),
    ".csv"
  ))
  
  write.csv(episodes, episodes_file, row.names = FALSE)
  message(sprintf("Saved %d episodes to %s", nrow(episodes), episodes_file))
  
  # Create summary statistics
  summary_stats <- create_episode_summary(episodes)
  
  summary_file <- file.path(output_folder, paste0(
    "pregnancy_summary_",
    format(Sys.Date(), "%Y%m%d"),
    ".csv"
  ))
  
  write.csv(summary_stats, summary_file, row.names = FALSE)
  message(sprintf("Saved summary statistics to %s", summary_file))
}

#' Create Episode Summary Statistics
#'
#' Generates summary statistics for identified pregnancy episodes.
#'
#' @param episodes Data frame of pregnancy episodes
#'
#' @return Data frame with summary statistics
#' @export
create_episode_summary <- function(episodes) {
  
  summary_stats <- list()
  
  # Overall counts
  summary_stats$total_episodes <- nrow(episodes)
  summary_stats$unique_persons <- n_distinct(episodes$person_id)
  summary_stats$episodes_per_person <- summary_stats$total_episodes / 
                                       summary_stats$unique_persons
  
  # By outcome
  outcome_summary <- episodes %>%
    group_by(outcome_category) %>%
    summarise(
      count = n(),
      pct = n() / nrow(episodes) * 100,
      avg_gest_days = mean(gestational_age_days, na.rm = TRUE),
      .groups = "drop"
    )
  
  # By algorithm
  algorithm_summary <- episodes %>%
    group_by(algorithm_used) %>%
    summarise(
      count = n(),
      pct = n() / nrow(episodes) * 100,
      .groups = "drop"
    )
  
  
  # Gestational age distribution
  gest_summary <- episodes %>%
    summarise(
      min_gest_days = min(gestational_age_days, na.rm = TRUE),
      q1_gest_days = quantile(gestational_age_days, 0.25, na.rm = TRUE),
      median_gest_days = median(gestational_age_days, na.rm = TRUE),
      mean_gest_days = mean(gestational_age_days, na.rm = TRUE),
      q3_gest_days = quantile(gestational_age_days, 0.75, na.rm = TRUE),
      max_gest_days = max(gestational_age_days, na.rm = TRUE),
      sd_gest_days = sd(gestational_age_days, na.rm = TRUE)
    )
  
  # Temporal distribution
  temporal_summary <- episodes %>%
    mutate(
      year = year(episode_end_date)
    ) %>%
    group_by(year) %>%
    summarise(
      count = n(),
      .groups = "drop"
    )
  
  # Combine all summaries
  result <- list(
    overall = as.data.frame(summary_stats),
    by_outcome = outcome_summary,
    by_algorithm = algorithm_summary,
    gestational_age = gest_summary,
    by_year = temporal_summary
  )
  
  return(result)
}

#' Export Results for Analysis
#'
#' Exports pregnancy episodes in formats suitable for statistical analysis.
#'
#' @param episodes Data frame of pregnancy episodes
#' @param output_folder Folder for output files
#' @param formats Character vector of formats ("csv", "rds", "xlsx")
#'
#' @export
export_for_analysis <- function(
  episodes,
  output_folder,
  formats = c("csv", "rds")
) {
  
  if (!dir.exists(output_folder)) {
    dir.create(output_folder, recursive = TRUE)
  }
  
  base_name <- paste0("pregnancy_analysis_", format(Sys.Date(), "%Y%m%d"))
  
  # CSV format
  if ("csv" %in% formats) {
    csv_file <- file.path(output_folder, paste0(base_name, ".csv"))
    write.csv(episodes, csv_file, row.names = FALSE)
    message(sprintf("Exported CSV to %s", csv_file))
  }
  
  # RDS format (preserves data types)
  if ("rds" %in% formats) {
    rds_file <- file.path(output_folder, paste0(base_name, ".rds"))
    saveRDS(episodes, rds_file)
    message(sprintf("Exported RDS to %s", rds_file))
  }
  
  # Excel format (requires openxlsx)
  if ("xlsx" %in% formats) {
    if (requireNamespace("openxlsx", quietly = TRUE)) {
      xlsx_file <- file.path(output_folder, paste0(base_name, ".xlsx"))
      
      wb <- openxlsx::createWorkbook()
      openxlsx::addWorksheet(wb, "Episodes")
      openxlsx::writeData(wb, "Episodes", episodes)
      
      # Add summary sheet
      summary <- create_episode_summary(episodes)
      openxlsx::addWorksheet(wb, "Summary")
      openxlsx::writeData(wb, "Summary", summary$overall)
      
      openxlsx::saveWorkbook(wb, xlsx_file, overwrite = TRUE)
      message(sprintf("Exported Excel to %s", xlsx_file))
    } else {
      warning("Package 'openxlsx' not available for Excel export")
    }
  }
}

#' Create Cohort Definition from Episodes
#'
#' Creates an OHDSI cohort definition from pregnancy episodes.
#'
#' @param episodes Data frame of pregnancy episodes
#' @param cohort_id Cohort definition ID
#' @param cohort_name Name for the cohort
#'
#' @return Data frame in OHDSI cohort format
#' @export
create_cohort_from_episodes <- function(
  episodes,
  cohort_id = 1,
  cohort_name = "Pregnancy Episodes"
) {
  
  cohort <- episodes %>%
    select(
      subject_id = person_id,
      cohort_start_date = episode_start_date,
      cohort_end_date = episode_end_date
    ) %>%
    mutate(
      cohort_definition_id = cohort_id
    ) %>%
    arrange(subject_id, cohort_start_date)
  
  attr(cohort, "cohort_name") <- cohort_name
  attr(cohort, "cohort_id") <- cohort_id
  
  message(sprintf(
    "Created cohort '%s' with %d entries for %d subjects",
    cohort_name,
    nrow(cohort),
    n_distinct(cohort$subject_id)
  ))
  
  return(cohort)
}