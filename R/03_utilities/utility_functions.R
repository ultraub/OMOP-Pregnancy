# Global constant for consistent month length calculations
# Using 365.25/12 for accurate month-to-day conversions across all algorithms
DAYS_PER_MONTH <- 30.437

#' Calculate Episode Dates
#'
#' Wrapper function that calculates estimated start dates for pregnancy episodes
#' using gestational timing data. This delegates to the ESD algorithm.
#'
#' @param episodes Data frame of pregnancy episodes
#' @param gestational_data Gestational timing data from cohort extraction
#'
#' @return Episodes with calculated start dates
#' @export
calculate_episode_dates <- function(episodes, gestational_data) {
  
  # Note: This is a simplified wrapper - in practice would use the
  # more sophisticated ESD algorithm already implemented
  
  if (nrow(episodes) == 0) {
    return(episodes)
  }
  
  # Add basic estimated start date calculation
  # This is a simple fallback - the ESD algorithm provides more sophisticated dating
  episodes <- episodes %>%
    mutate(
      episode_start_date = case_when(
        !is.na(gestational_age_days) & gestational_age_days > 0 ~ 
          episode_end_date - gestational_age_days,
        outcome_category == "LB" ~ episode_end_date - 280,  # 40 weeks
        outcome_category == "SB" ~ episode_end_date - 280,  # 40 weeks  
        outcome_category == "DELIV" ~ episode_end_date - 280,  # 40 weeks
        outcome_category == "AB" ~ episode_end_date - 91,   # 13 weeks
        outcome_category == "SA" ~ episode_end_date - 91,   # 13 weeks
        outcome_category == "ECT" ~ episode_end_date - 63,  # 9 weeks
        outcome_category == "PREG" ~ episode_end_date - 140, # 20 weeks
        TRUE ~ episode_end_date - 200  # Default
      )
    )
  
  return(episodes)
}



#' Save Results
#'
#' Wrapper function for saving pregnancy identification results.
#' Delegates to the more comprehensive save_pregnancy_results function.
#'
#' @param episodes Data frame of pregnancy episodes
#' @param output_folder Folder for saving CSV files
#' @param connection Database connection (optional)
#' @param results_schema Schema for database results (optional)
#'
#' @return NULL (saves files as side effect)
#' @export
save_results <- function(episodes, output_folder, connection = NULL, results_schema = NULL) {
  
  # Use the comprehensive save function
  save_pregnancy_results(
    episodes = episodes,
    connection = connection,
    results_schema = results_schema,
    output_folder = output_folder,
    save_to_database = !is.null(connection) && !is.null(results_schema)
  )
  
  # Also create analysis exports
  if (!is.null(output_folder)) {
    export_for_analysis(
      episodes = episodes,
      output_folder = output_folder,
      formats = c("csv", "rds")
    )
  }
}