#' Debug version of HIP algorithm functions with tracing
#'
#' This file contains debug versions of the gestation functions to trace the issue

#' Debug version of gestation_visits
gestation_visits_debug <- function(initial_pregnant_cohort_df, config = NULL) {
  message("DEBUG: gestation_visits - Starting")
  
  # Check input columns
  input_cols <- colnames(initial_pregnant_cohort_df)
  message("DEBUG: gestation_visits - Input columns: ", paste(input_cols, collapse = ", "))
  
  # Count input rows
  input_count <- initial_pregnant_cohort_df %>%
    count() %>%
    collect() %>%
    pull(n)
  message("DEBUG: gestation_visits - Input rows: ", input_count)
  
  # Get gestational age concept IDs from config or use defaults
  if (is.null(config)) {
    gestational_age_concepts <- c(3002209, 3048230, 3012266)
    max_gestational_weeks <- 44
  } else {
    gestational_age_concepts <- c(
      config$concepts$gestational_age_concepts$gestational_age_estimated,
      config$concepts$gestational_age_concepts$gestational_age_in_weeks,
      config$concepts$gestational_age_concepts$gestational_age
    )
    max_gestational_weeks <- ifelse(
      !is.null(config$algorithm_parameters$max_gestational_weeks),
      config$algorithm_parameters$max_gestational_weeks,
      44
    )
  }
  
  # Get records with gestation period
  gest_df <- initial_pregnant_cohort_df %>% filter(!is.na(gest_value))
  
  gest_count <- gest_df %>%
    count() %>%
    collect() %>%
    pull(n)
  message("DEBUG: gestation_visits - Records with gest_value: ", gest_count)
  
  # Get records with gestational age in weeks
  other_gest_df <- initial_pregnant_cohort_df %>%
    filter(
      concept_id %in% gestational_age_concepts,
      !is.na(value_as_number),
      value_as_number > 0, value_as_number <= max_gestational_weeks
    ) %>%
    mutate(gest_value = as.integer(value_as_number))
  
  other_count <- other_gest_df %>%
    count() %>%
    collect() %>%
    pull(n)
  message("DEBUG: gestation_visits - Records with gestational age concepts: ", other_count)
  
  # Combine tables
  all_gest_df <- union_all(gest_df, other_gest_df)
  
  # Check output columns
  output_cols <- colnames(all_gest_df)
  message("DEBUG: gestation_visits - Output columns: ", paste(output_cols, collapse = ", "))
  
  total_count <- all_gest_df %>%
    count() %>%
    collect() %>%
    pull(n)
  message("DEBUG: gestation_visits - Total output rows: ", total_count)
  
  return(all_gest_df)
}

#' Debug version of gestation_episodes
gestation_episodes_debug <- function(gestation_visits_df, min_days = 70, buffer_days = 28, config = NULL, connection = NULL) {
  message("DEBUG: gestation_episodes - Starting")
  
  # Check input columns
  input_cols <- colnames(gestation_visits_df)
  message("DEBUG: gestation_episodes - Input columns: ", paste(input_cols, collapse = ", "))
  
  # Check for visit_date column specifically
  if (!"visit_date" %in% input_cols) {
    message("WARNING: gestation_episodes - visit_date column is MISSING!")
  }
  
  # Extract connection if not provided
  if (is.null(connection) && inherits(gestation_visits_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- gestation_visits_df$src$con
  }
  
  # Check if there are any gestation visits
  visit_count <- gestation_visits_df %>%
    count() %>%
    collect() %>%
    pull(n)
  
  message("DEBUG: gestation_episodes - Input rows: ", visit_count)
  
  if (visit_count == 0) {
    message("DEBUG: gestation_episodes - No visits, returning empty result")
    # Return empty data frame with expected columns
    empty_result <- gestation_visits_df %>%
      head(0) %>%
      mutate(
        gest_week = integer(),
        episode = integer(),
        visit_date = as.Date(character())
      ) %>%
      select(person_id, visit_date, gest_week, episode)
    
    empty_cols <- colnames(empty_result)
    message("DEBUG: gestation_episodes - Empty result columns: ", paste(empty_cols, collapse = ", "))
    
    return(empty_result)
  }
  
  # Continue with regular processing...
  message("DEBUG: gestation_episodes - Processing ", visit_count, " visits")
  
  # Get max gestational weeks from config or use default
  max_gestational_weeks <- if (!is.null(config) && !is.null(config$algorithm_parameters$max_gestational_weeks)) {
    config$algorithm_parameters$max_gestational_weeks
  } else {
    44
  }
  
  # Process the data (simplified for debugging)
  df <- gestation_visits_df %>%
    filter(
      !is.na(visit_date),
      gest_value > 0 & gest_value <= max_gestational_weeks
    )
  
  # Check how many records pass the filter
  filtered_count <- df %>%
    count() %>%
    collect() %>%
    pull(n)
  
  message("DEBUG: gestation_episodes - After filtering: ", filtered_count, " records")
  
  if (filtered_count == 0) {
    message("DEBUG: gestation_episodes - All records filtered out, returning empty result")
    # Return empty result with proper structure
    empty_result <- gestation_visits_df %>%
      head(0) %>%
      mutate(
        gest_week = integer(),
        episode = integer()
      )
    
    # Make sure visit_date is included
    if ("visit_date" %in% colnames(gestation_visits_df)) {
      empty_result <- empty_result %>%
        mutate(visit_date = as.Date(character()))
    }
    
    return(empty_result)
  }
  
  # Continue with the rest of the function...
  # (simplified for debugging)
  result <- df %>%
    mutate(
      gest_week = gest_value,
      episode = 1  # Simplified - just assign episode 1 for debugging
    )
  
  output_cols <- colnames(result)
  message("DEBUG: gestation_episodes - Output columns: ", paste(output_cols, collapse = ", "))
  
  return(result)
}

#' Debug version of get_min_max_gestation
get_min_max_gestation_debug <- function(gestation_episodes_df) {
  message("DEBUG: get_min_max_gestation - Starting")
  
  # Check input columns
  input_cols <- colnames(gestation_episodes_df)
  message("DEBUG: get_min_max_gestation - Input columns: ", paste(input_cols, collapse = ", "))
  
  # Check for required columns
  required_cols <- c("person_id", "episode", "visit_date", "gest_week")
  missing_cols <- setdiff(required_cols, input_cols)
  if (length(missing_cols) > 0) {
    message("ERROR: get_min_max_gestation - Missing required columns: ", paste(missing_cols, collapse = ", "))
  }
  
  # Check if there are any gestation episodes
  episode_count <- gestation_episodes_df %>%
    count() %>%
    collect() %>%
    pull(n)
  
  message("DEBUG: get_min_max_gestation - Input rows: ", episode_count)
  
  if (episode_count == 0) {
    message("DEBUG: get_min_max_gestation - No episodes, returning empty result")
    # Return appropriate empty result...
    return(data.frame(
      person_id = numeric(),
      episode = numeric(),
      first_gest_week = numeric(),
      end_gest_date = as.Date(character()),
      end_gest_week = numeric(),
      min_gest_week = numeric(),
      min_gest_date = as.Date(character()),
      gest_week = numeric(),
      min_gest_date_2 = as.Date(character()),
      max_gest_week = numeric(),
      max_gest_date = as.Date(character())
    ))
  }
  
  message("DEBUG: get_min_max_gestation - Processing ", episode_count, " episodes")
  
  # Try to process first visit date
  tryCatch({
    message("DEBUG: get_min_max_gestation - Attempting to process first visit date...")
    new_first_df <- gestation_episodes_df %>%
      group_by(person_id, episode) %>%
      filter(visit_date == min(visit_date, na.rm = TRUE)) %>%
      summarise(first_gest_week = max(gest_week, na.rm = TRUE), .groups = "drop")
    
    first_count <- new_first_df %>%
      count() %>%
      collect() %>%
      pull(n)
    
    message("DEBUG: get_min_max_gestation - First visit processed: ", first_count, " records")
  }, error = function(e) {
    message("ERROR: get_min_max_gestation - Failed to process first visit: ", e$message)
    stop(e)
  })
  
  # Continue with simplified processing for debugging...
  message("DEBUG: get_min_max_gestation - Completed successfully")
  
  # Return a simplified result for debugging
  return(data.frame(
    person_id = numeric(),
    episode = numeric(),
    first_gest_week = numeric(),
    end_gest_date = as.Date(character()),
    end_gest_week = numeric(),
    min_gest_week = numeric(),
    min_gest_date = as.Date(character()),
    gest_week = numeric(),
    min_gest_date_2 = as.Date(character()),
    max_gest_week = numeric(),
    max_gest_date = as.Date(character())
  ))
}