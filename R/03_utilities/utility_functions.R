# Global constant for consistent month length calculations
# Using 30 days per month to match All of Us implementation
DAYS_PER_MONTH <- 30

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