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


#' Safely Convert to Date Type
#'
#' Handles various date formats and SQL Server numeric dates.
#'
#' @param x Value to convert to Date
#' @return Date object
#' @export
safe_as_date <- function(x) {
  # If already Date, return as-is
  if (inherits(x, "Date")) {
    return(x)
  }
  
  # If POSIXct/POSIXlt, convert
  if (inherits(x, c("POSIXct", "POSIXlt"))) {
    return(as.Date(x))
  }
  
  # If numeric (SQL Server date), convert
  if (is.numeric(x)) {
    # Determine the likely date format based on the numeric range
    # SQL Server dates are days since 1900-01-01 (origin 1899-12-30 due to Excel bug)
    # Current dates (2020s) would be ~44000-45000 as SQL Server dates
    # Unix timestamps are seconds since 1970-01-01
    
    # Get the maximum non-NA value for checking
    max_val <- max(x[!is.na(x)], na.rm = TRUE)
    
    if (is.infinite(max_val) || is.na(max_val)) {
      # If no valid values, return NA dates
      return(as.Date(NA))
    }
    
    if (max_val < 50000) {
      # Likely SQL Server date (days since 1900)
      # 50000 days from 1900 = year 2036
      return(as.Date(x, origin = "1899-12-30"))
    } else if (max_val > 1000000) {
      # Likely Unix timestamp (seconds since 1970)
      return(as.Date(x/86400, origin = "1970-01-01"))
    } else {
      # In between - could be days since 1970
      # 50000 days from 1970 = year 2106
      return(as.Date(x, origin = "1970-01-01"))
    }
  }
  
  # If character, parse
  if (is.character(x)) {
    return(as.Date(x))
  }
  
  # Default: try as.Date
  return(as.Date(x))
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