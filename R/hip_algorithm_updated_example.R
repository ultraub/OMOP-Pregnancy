#' HIP (Hierarchy-based Inference of Pregnancy) Algorithm - SqlRender Updated Example
#'
#' This is an example showing how hip_algorithm.R would be updated to use the 
#' new SqlRender-based wrappers for cross-platform compatibility.
#' 
#' NOTE: This is a demonstration file showing the conversion pattern.
#' The actual hip_algorithm.R would need all occurrences updated similarly.
#'
#' @name hip_algorithm_updated_example
#' @import dplyr
#' @import lubridate
#' @import purrr
NULL

# Source the SQL functions for cross-platform support
# (This would be handled by package loading in production)

#' Get initial pregnant cohort - Updated with SqlRender
#' 
#' This example shows how the function would be updated to use SqlRender wrappers
#'
#' @param procedure_occurrence_tbl Procedure occurrence table reference
#' @param measurement_tbl Measurement table reference
#' @param observation_tbl Observation table reference
#' @param condition_occurrence_tbl Condition occurrence table reference
#' @param person_tbl Person table reference
#' @param HIP_concepts HIP concepts table reference
#' @param config Configuration list (optional, uses defaults if not provided)
#' @param connection Database connection for dialect detection
#'
#' @return Data frame with pregnancy-related concepts for eligible persons
#' @export
initial_pregnant_cohort_updated <- function(procedure_occurrence_tbl, measurement_tbl,
                                           observation_tbl, condition_occurrence_tbl,
                                           person_tbl, HIP_concepts, config = NULL,
                                           connection = NULL) {
  
  # Extract connection from table if not provided
  if (is.null(connection) && inherits(person_tbl, c("tbl_lazy", "tbl_sql"))) {
    connection <- person_tbl$src$con
  }
  
  # Get concepts specific for pregnancy from domain tables.
  observation_df <- observation_tbl %>%
    select(
      person_id,
      concept_id = observation_concept_id,
      visit_date = observation_date,
      value_as_number
    ) %>%
    inner_join(HIP_concepts, by = "concept_id", copy = TRUE) %>%
    select(person_id, concept_id, visit_date, value_as_number, concept_name, category, gest_value)
  
  measurement_df <- measurement_tbl %>%
    select(
      person_id,
      concept_id = measurement_concept_id,
      visit_date = measurement_date,
      value_as_number
    ) %>%
    inner_join(HIP_concepts, by = "concept_id", copy = TRUE) %>%
    select(person_id, concept_id, visit_date, value_as_number, concept_name, category, gest_value)
  
  procedure_df <- procedure_occurrence_tbl %>%
    select(
      person_id,
      concept_id = procedure_concept_id,
      visit_date = procedure_date
    ) %>%
    mutate(value_as_number = NA_real_) %>%
    inner_join(HIP_concepts, by = "concept_id", copy = TRUE) %>%
    select(person_id, concept_id, visit_date, value_as_number, concept_name, category, gest_value)
  
  condition_df <- condition_occurrence_tbl %>%
    select(
      person_id,
      concept_id = condition_concept_id,
      visit_date = condition_start_date
    ) %>%
    mutate(value_as_number = NA_real_) %>%
    inner_join(HIP_concepts, by = "concept_id", copy = TRUE) %>%
    select(person_id, concept_id, visit_date, value_as_number, concept_name, category, gest_value)
  
  # combine tables
  all_dfs <- list(measurement_df, procedure_df, observation_df, condition_df)
  union_df <- reduce(all_dfs, union_all)
  
  # Get gender concept ID from config or use default
  if (is.null(config)) {
    male_concept_id <- 8507
    min_age <- 15
    max_age <- 56
  } else {
    male_concept_id <- ifelse(
      !is.null(config$concepts$gender_concepts$active_male_id),
      config$concepts$gender_concepts$active_male_id,
      ifelse(
        config$mode == "allofus",
        45880669,
        8507
      )
    )
    min_age <- ifelse(!is.null(config$algorithm$hip$min_age), 
                      config$algorithm$hip$min_age, 15)
    max_age <- ifelse(!is.null(config$algorithm$hip$max_age), 
                      config$algorithm$hip$max_age, 56)
  }
  
  # get unique person ids for women of reproductive age
  person_df <- person_tbl %>%
    filter(
      gender_concept_id != male_concept_id
    ) %>%
    mutate(
      day_of_birth = if_else(is.na(day_of_birth), 1, day_of_birth),
      month_of_birth = if_else(is.na(month_of_birth), 1, month_of_birth),
      # UPDATED: Use SqlRender wrapper instead of hardcoded SQL Server syntax
      date_of_birth = sql_date_from_parts("year_of_birth", "month_of_birth", "day_of_birth", connection)
    ) %>%
    select(person_id, date_of_birth)
  
  # keep only person_ids of women of reproductive age at some visit
  union_df <- union_df %>%
    inner_join(person_df, by = "person_id") %>%
    mutate(
      # UPDATED: Use SqlRender wrapper for date difference
      date_diff = sql_date_diff("visit_date", "date_of_birth", "day", connection),
      age = date_diff / 365
    ) %>%
    filter(age >= min_age, age < max_age)
  
  # return the resulting dataframe
  distinct(union_df)
}

#' Example of updating final_visits function
#'
#' Shows how date operations would be updated throughout the algorithm
#'
#' @param initial_pregnant_cohort_df Initial cohort data frame
#' @param Matcho_outcome_limits Outcome limits table
#' @param categories Categories to filter
#' @param connection Database connection for dialect detection
#'
#' @return Data frame with final visits
#' @export
final_visits_updated <- function(initial_pregnant_cohort_df, Matcho_outcome_limits, 
                                categories, connection = NULL) {
  
  # Extract connection if not provided
  if (is.null(connection) && inherits(initial_pregnant_cohort_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- initial_pregnant_cohort_df$src$con
  }
  
  df <- initial_pregnant_cohort_df %>%
    filter(category %in% categories) %>%
    group_by(person_id, visit_date) %>%
    slice_min(order_by = concept_id, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    group_by(person_id) %>%
    arrange(visit_date, .by_group = TRUE) %>%
    # UPDATED: Use SqlRender wrapper for date difference
    mutate(days = sql_date_diff("visit_date", "lag(visit_date)", "day", connection)) %>%
    # Continue with rest of logic...
    filter(days > 90 | is.na(days))
  
  return(df)
}

#' Example of updating functions with DATEADD
#'
#' Shows how DATEADD operations would be converted
#'
#' @param data Input data frame
#' @param connection Database connection
#' @export
example_date_add_updated <- function(data, connection = NULL) {
  
  # Extract connection if not provided
  if (is.null(connection) && inherits(data, c("tbl_lazy", "tbl_sql"))) {
    connection <- data$src$con
  }
  
  data %>%
    mutate(
      # ORIGINAL: min_start_date = sql("DATEADD(day, -CAST(min_term AS INT), visit_date)")
      # UPDATED: Use SqlRender wrapper
      min_start_date = sql_date_add("visit_date", "-CAST(min_term AS INT)", "day", connection),
      
      # ORIGINAL: max_start_date = sql("DATEADD(day, -CAST(max_term AS INT), visit_date)")
      # UPDATED: Use SqlRender wrapper
      max_start_date = sql_date_add("visit_date", "-CAST(max_term AS INT)", "day", connection)
    )
}

#' Example of updating concat operations
#'
#' Shows how concat operations would be converted
#'
#' @param data Input data frame
#' @param connection Database connection
#' @export
example_concat_updated <- function(data, connection = NULL) {
  
  # Extract connection if not provided
  if (is.null(connection) && inherits(data, c("tbl_lazy", "tbl_sql"))) {
    connection <- data$src$con
  }
  
  data %>%
    mutate(
      # ORIGINAL: visit_id = sql("concat(person_id, visit_date)")
      # UPDATED: Use SqlRender wrapper
      visit_id = sql_concat("person_id", "visit_date", connection = connection),
      
      # ORIGINAL: gest_id = sql("concat(person_id, max_gest_date)")
      # UPDATED: Use SqlRender wrapper
      gest_id = sql_concat("person_id", "max_gest_date", connection = connection)
    )
}

#' Pattern for updating all date operations in the algorithm
#'
#' This function demonstrates the conversion pattern that would be applied
#' throughout the entire hip_algorithm.R file.
#'
#' @details
#' Conversion patterns:
#' 1. sql("DATEDIFF(day, date1, date2)") -> sql_date_diff("date2", "date1", "day", connection)
#' 2. sql("DATEFROMPARTS(year, month, day)") -> sql_date_from_parts("year", "month", "day", connection)
#' 3. sql("DATEADD(day, n, date)") -> sql_date_add("date", "n", "day", connection)
#' 4. sql("concat(field1, field2)") -> sql_concat("field1", "field2", connection = connection)
#'
#' The connection parameter should be passed through all functions or extracted
#' from the lazy table objects.
#'
#' @export
conversion_pattern_documentation <- function() {
  message("
  SqlRender Conversion Patterns for hip_algorithm.R:
  
  Total replacements needed: ~40 occurrences
  
  Files to update:
  - hip_algorithm.R (30+ date operations)
  - pps_algorithm.R (5+ date operations)  
  - Any other algorithm files
  
  Benefits:
  - Cross-platform compatibility (PostgreSQL, Oracle, BigQuery, etc.)
  - Maintains existing logic and performance
  - Follows OHDSI best practices
  - No changes to algorithm logic required
  
  Testing required:
  - Unit tests with mock connections for each dialect
  - Integration tests on actual databases if available
  - Validation that results match across platforms
  ")
}
