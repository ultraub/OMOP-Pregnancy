#' HIP (Hierarchy-based Inference of Pregnancy) Algorithm
#'
#' Implementation of the HIP algorithm for pregnancy episode identification.
#' This code maintains maximum similarity to the original All of Us implementation
#' while using the abstraction layer for database operations.
#'
#' @name hip_algorithm
#' @import dplyr
#' @import lubridate
#' @import purrr
NULL

# Note: SQL functions must be called with sql() directly in dplyr pipelines
# to ensure the SQL is properly generated rather than passed as function names

#' Get initial pregnant cohort
#' 
#' @param procedure_occurrence_tbl Procedure occurrence table reference
#' @param measurement_tbl Measurement table reference
#' @param observation_tbl Observation table reference
#' @param condition_occurrence_tbl Condition occurrence table reference
#' @param person_tbl Person table reference
#' @param HIP_concepts HIP concepts table reference
#' @param config Configuration list (optional, uses defaults if not provided)
#' @param connection Database connection (optional, for dialect detection)
#'
#' @return Data frame with pregnancy-related concepts for eligible persons
#' @export
initial_pregnant_cohort <- function(procedure_occurrence_tbl, measurement_tbl,
                                    observation_tbl, condition_occurrence_tbl,
                                    person_tbl, HIP_concepts, config = NULL,
                                    connection = NULL) {
  # Get concepts specific for pregnancy from domain tables.
  
  # Determine if HIP_concepts is already in database or needs to be copied
  is_lazy_query <- inherits(HIP_concepts, "tbl_lazy") || inherits(HIP_concepts, "tbl_sql")
  
  if (is_lazy_query) {
    # HIP_concepts is in database, perform joins directly
    # suffix parameter required for handling column name conflicts
    observation_df <- observation_tbl %>%
      select(
        person_id,
        concept_id = observation_concept_id,
        visit_date = observation_date,
        value_as_number
      ) %>%
      inner_join(HIP_concepts, by = "concept_id", suffix = c(".x", ".y")) %>%
      select(person_id, concept_id, visit_date, value_as_number, concept_name, category, gest_value)
    
    measurement_df <- measurement_tbl %>%
      select(
        person_id,
        concept_id = measurement_concept_id,
        visit_date = measurement_date,
        value_as_number
      ) %>%
      inner_join(HIP_concepts, by = "concept_id", suffix = c(".x", ".y")) %>%
      select(person_id, concept_id, visit_date, value_as_number, concept_name, category, gest_value)
    
    procedure_df <- procedure_occurrence_tbl %>%
      select(
        person_id,
        concept_id = procedure_concept_id,
        visit_date = procedure_date
      ) %>%
      mutate(value_as_number = NA_real_) %>%  # Add missing column for union
      inner_join(HIP_concepts, by = "concept_id", suffix = c(".x", ".y")) %>%
      select(person_id, concept_id, visit_date, value_as_number, concept_name, category, gest_value)
    
    # filter condition table
    condition_df <- condition_occurrence_tbl %>%
      select(
        person_id,
        concept_id = condition_concept_id,
        visit_date = condition_start_date
      ) %>%
      mutate(value_as_number = NA_real_) %>%  # Add missing column for union
      inner_join(HIP_concepts, by = "concept_id", suffix = c(".x", ".y")) %>%
      select(person_id, concept_id, visit_date, value_as_number, concept_name, category, gest_value)
  } else {
    # HIP_concepts is local, copy to database for join
    observation_df <- observation_tbl %>%
      select(
        person_id,
        concept_id = observation_concept_id,
        visit_date = observation_date,
        value_as_number
      ) %>%
      inner_join(HIP_concepts, by = "concept_id", copy = TRUE, suffix = c(".x", ".y")) %>%
      select(person_id, concept_id, visit_date, value_as_number, concept_name, category, gest_value)
    
    measurement_df <- measurement_tbl %>%
      select(
        person_id,
        concept_id = measurement_concept_id,
        visit_date = measurement_date,
        value_as_number
      ) %>%
      inner_join(HIP_concepts, by = "concept_id", copy = TRUE, suffix = c(".x", ".y")) %>%
      select(person_id, concept_id, visit_date, value_as_number, concept_name, category, gest_value)
    
    procedure_df <- procedure_occurrence_tbl %>%
      select(
        person_id,
        concept_id = procedure_concept_id,
        visit_date = procedure_date
      ) %>%
      mutate(value_as_number = NA_real_) %>%  # Add missing column for union
      inner_join(HIP_concepts, by = "concept_id", copy = TRUE, suffix = c(".x", ".y")) %>%
      select(person_id, concept_id, visit_date, value_as_number, concept_name, category, gest_value)
    
    # filter condition table
    condition_df <- condition_occurrence_tbl %>%
      select(
        person_id,
        concept_id = condition_concept_id,
        visit_date = condition_start_date
      ) %>%
      mutate(value_as_number = NA_real_) %>%  # Add missing column for union
      inner_join(HIP_concepts, by = "concept_id", copy = TRUE, suffix = c(".x", ".y")) %>%
      select(person_id, concept_id, visit_date, value_as_number, concept_name, category, gest_value)
  }
  
  # combine tables
  all_dfs <- list(measurement_df, procedure_df, observation_df, condition_df)
  union_df <- reduce(all_dfs, union_all)
  
  # Debug: Check data integrity after union
  if (inherits(union_df, c("tbl_lazy", "tbl_sql"))) {
    # For database queries, check counts
    total_count <- safe_count(union_df)
    cat("[DEBUG] initial_pregnant_cohort: Total records after union:", 
        ifelse(is.na(total_count), "unable to count", as.character(total_count)), "\n")
    
    # Check how many records have non-NULL gest_value
    gest_count <- union_df %>% 
      filter(!is.na(gest_value)) %>% 
      safe_count()
    cat("[DEBUG] initial_pregnant_cohort: Records with non-NULL gest_value:", 
        ifelse(is.na(gest_count), "unable to count", as.character(gest_count)), "\n")
    
    # Check how many records have non-NULL value_as_number
    value_count <- union_df %>% 
      filter(!is.na(value_as_number)) %>% 
      safe_count()
    cat("[DEBUG] initial_pregnant_cohort: Records with non-NULL value_as_number:", 
        ifelse(is.na(value_count), "unable to count", as.character(value_count)), "\n")
  }
  
  # Get gender concept ID from config or use default
  if (is.null(config)) {
    # Use default OMOP standard concept for male
    male_concept_id <- 8507
    min_age <- 15
    max_age <- 56
  } else {
    # Use configured concept IDs
    male_concept_id <- ifelse(
      !is.null(config$concepts$gender_concepts$active_male_id),
      config$concepts$gender_concepts$active_male_id,
      ifelse(
        config$mode == "allofus",
        45880669,  # All of Us male concept
        8507  # Standard OMOP male concept
      )
    )
    min_age <- ifelse(!is.null(config$algorithm$hip$min_age), 
                      config$algorithm$hip$min_age, 15)
    max_age <- ifelse(!is.null(config$algorithm$hip$max_age), 
                      config$algorithm$hip$max_age, 56)
  }
  
  # Extract connection from table if not provided
  if (is.null(connection) && inherits(person_tbl, c("tbl_lazy", "tbl_sql"))) {
    connection <- person_tbl$src$con
  }
  
  # Generate SQL expressions for date operations using the connection
  # These need to be created outside the mutate context
  date_from_parts_sql <- sql_date_from_parts("year_of_birth", "month_of_birth", "day_of_birth", connection)
  
  # get unique person ids for women of reproductive age
  person_df <- person_tbl %>%
    filter(
      # Exclude males (configurable concept ID)
      # Standard OMOP: 8507 (Male)
      # All of Us: 45880669 (Male)
      gender_concept_id != male_concept_id
    ) %>%
    mutate(
      day_of_birth = if_else(is.na(day_of_birth), 1, day_of_birth),
      month_of_birth = if_else(is.na(month_of_birth), 1, month_of_birth),
      # Use the pre-generated SQL expression
      date_of_birth = !!date_from_parts_sql
    ) %>%
    select(person_id, date_of_birth)
  
  # Generate date diff SQL expression
  date_diff_sql <- sql_date_diff("visit_date", "date_of_birth", "day", connection)
  
  # keep only person_ids of women of reproductive age at some visit
  union_df <- union_df %>%
    inner_join(person_df, by = "person_id", suffix = c(".x", ".y")) %>%
    mutate(
      date_diff = !!date_diff_sql,
      age = date_diff / 365
    ) %>%
    filter(age >= min_age, age < max_age)
  
  # return the resulting dataframe
  # For Spark compatibility, avoid implicit compute in distinct()
  union_df %>% distinct()
}

#' Identify final visits for pregnancy outcomes
#'
#' Note here that for SA and AB, if there is an episode that contains concepts for both,
#' only one will be (essentially randomly) chosen
#'
#' @param initial_pregnant_cohort_df Initial cohort data frame
#' @param Matcho_outcome_limits Outcome limits table
#' @param categories Categories to filter
#'
#' @return Data frame with final visits
#' @export
final_visits <- function(initial_pregnant_cohort_df, Matcho_outcome_limits, categories, connection = NULL) {
  # Extract connection if not provided
  if (is.null(connection) && inherits(initial_pregnant_cohort_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- initial_pregnant_cohort_df$src$con
  }
  
  # Pre-compute SQL expressions outside the pipeline
  days_sql <- sql_translate("DATEDIFF(day, prev_date, visit_date)", connection)
  
  df <- initial_pregnant_cohort_df %>%
    filter(category %in% categories) %>%
    # only keep one obs per person-date -- they're all in the same category
    group_by(person_id, visit_date) %>%
    # slicing by minimum concept id just a choice to make the code work
    # could have also done something like filter(row_number() == 1) but doesn't
    # work on databases
    slice_min(order_by = concept_id, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    group_by(person_id) %>%
    arrange(visit_date, .by_group = TRUE) %>%
    # Create a new column called "days" that calculates the number of days between each visit for each person.
    mutate(prev_date = lag(visit_date)) %>%
    mutate(days = !!days_sql) %>%
    ungroup()
  
  temp_df <- df
  
  # get minimum days between outcomes
  for (i in 1:length(categories)) {
    # get relevant rows from Matcho limits
    Matcho_limits_for_category <- Matcho_outcome_limits %>%
      filter(first_preg_category == categories[i] & outcome_preg_category == categories[i])
    
    # filter based on the days for same type of outcome
    # keep the visit dates that are separated by at least min_days
    # if days is missing then it is the first episode so keep this
    temp_df <- temp_df %>%
      filter(is.na(days) | days >= Matcho_limits_for_category$min_days)
  }
  
  temp_df
}

#' Add stillbirth episodes to livebirth episodes
#'
#' @param final_stillbirth_visits_df Stillbirth visits
#' @param final_livebirth_visits_df Livebirth visits
#' @param Matcho_outcome_limits Outcome limits
#'
#' @return Combined episodes
#' @export
add_stillbirth <- function(final_stillbirth_visits_df, final_livebirth_visits_df, Matcho_outcome_limits, connection = NULL) {
  # Extract connection if not provided
  if (is.null(connection) && inherits(final_stillbirth_visits_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- final_stillbirth_visits_df$src$con
  }
  
  # Pre-compute SQL expressions outside the pipeline
  after_days_sql <- sql_translate("DATEDIFF(day, prev_date, visit_date)", connection)
  before_days_sql <- sql_translate("DATEDIFF(day, visit_date, next_date)", connection)
  
  # Add stillbirth visits to livebirth visits table.
  
  # get minimum days between outcomes
  before_min <- Matcho_outcome_limits %>%
    filter(first_preg_category == "LB" & outcome_preg_category == "SB") %>%
    pull(min_days)
  
  after_min <- Matcho_outcome_limits %>%
    filter(first_preg_category == "SB" & outcome_preg_category == "LB") %>%
    pull(min_days)
  
  # pull out the stillbirth episodes again, but first figure out if it's plausible
  # that they happened relative to a live birth
  final_temp_df <- union_all(final_stillbirth_visits_df, final_livebirth_visits_df) %>%
    select(-any_of(c("gest_value", "value_as_number"))) %>%
    group_by(person_id) %>%
    arrange(visit_date, .by_group = TRUE) %>%
    mutate(
      # get previous category if available
      previous_category = lag(category),
      # get difference in days with previous episode start date
      prev_date = lag(visit_date)) %>%
    mutate(after_days = !!after_days_sql,
      next_category = lead(category),
      # and next episode start date
      next_date = lead(visit_date)) %>%
    mutate(before_days = !!before_days_sql
    ) %>%
    filter(category == "SB") %>%
    filter(
      # it's the only episode
      (is.na(before_days) & is.na(after_days)) |
        # the previous category was a stillbirth and there's no next category
        # (those were already checked)
        (previous_category != "LB" & is.na(next_category)) |
        # same but opposite
        (next_category != "LB" & is.na(previous_category)) |
        (previous_category != "LB" & next_category != "LB") |
        # the last episode was a live birth and this one happens after the minimum
        (previous_category == "LB" & after_days >= before_min & is.na(next_category)) |
        # the next episode is a live birth and happens after the minimum
        (next_category == "LB" & before_days >= after_min & is.na(previous_category)) |
        # or surrounded by two live births spaced sufficiently
        (next_category == "LB" & before_days >= after_min & previous_category == "LB" & after_days >= before_min)
    ) %>%
    ungroup()
  
  # combine with livebirth table and drop columns
  final_df <- union_all(final_livebirth_visits_df, final_temp_df) %>%
    select(-previous_category, -next_category, -before_days, -after_days) %>%
    distinct()
  
  return(final_df)
}

#' Add ectopic episodes
#'
#' @param add_stillbirth_df Previous episodes
#' @param Matcho_outcome_limits Outcome limits
#' @param final_ectopic_visits_df Ectopic visits
#'
#' @return Combined episodes
#' @export
add_ectopic <- function(add_stillbirth_df, Matcho_outcome_limits, final_ectopic_visits_df, connection = NULL) {
  # Extract connection if not provided
  if (is.null(connection) && inherits(add_stillbirth_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- add_stillbirth_df$src$con
  }
  
  # Pre-compute SQL expressions outside the pipeline
  after_days_sql <- sql_translate("DATEDIFF(day, prev_date, visit_date)", connection)
  before_days_sql <- sql_translate("DATEDIFF(day, visit_date, next_date)", connection)
  # get minimum days between outcomes
  # minimum number of days that ECT can follow LB and SB; LB and SB have the same days
  before_min <- Matcho_outcome_limits %>%
    filter(first_preg_category == "LB" & outcome_preg_category == "ECT") %>%
    pull(min_days)
  # minimum number of days that LB can follow ECT
  after_min_lb <- Matcho_outcome_limits %>%
    filter(first_preg_category == "ECT" & outcome_preg_category == "LB") %>%
    pull(min_days)
  # minimum number of days that SB can follow ECT
  after_min_sb <- Matcho_outcome_limits %>%
    filter(first_preg_category == "ECT" & outcome_preg_category == "SB") %>%
    pull(min_days)
  
  # get difference in days with subsequent visit
  final_temp_df <- union_all(add_stillbirth_df, select(final_ectopic_visits_df, -any_of(c("gest_value", "value_as_number")))) %>%
    group_by(person_id) %>%
    arrange(visit_date, .by_group = TRUE) %>%
    mutate(
      # get previous category if available
      previous_category = lag(category),
      # get difference in days with previous episode start date
      prev_date = lag(visit_date)) %>%
    mutate(after_days = !!after_days_sql,
      next_category = lead(category),
      # and next episode start date
      next_date = lead(visit_date)) %>%
    mutate(before_days = !!before_days_sql
    ) %>%
    # filter to ectopic visits
    filter(category == "ECT") %>%
    filter(
      # it's the only episode
      (is.na(before_days) & is.na(after_days)) |
        # the previous category was ectopic and there's no next category
        # (those were already checked)
        # or some configuration
        (!previous_category %in% c("LB", "SB") & is.na(next_category)) |
        (!next_category %in% c("LB", "SB") & is.na(previous_category)) |
        (!previous_category %in% c("LB", "SB") & !next_category %in% c("LB", "SB")) |
        # the last episode was a delivery and this one happens after the minimum
        (previous_category %in% c("LB", "SB") & after_days >= before_min & is.na(next_category)) |
        # there was no previous category and the next live birth happens after the minimum
        (next_category == "LB" & before_days >= after_min_lb & is.na(previous_category)) |
        (next_category == "SB" & before_days >= after_min_sb & is.na(previous_category)) |
        # surrounded by each, appropriately spaced
        (next_category == "LB" & before_days >= after_min_lb & previous_category %in% c("LB", "SB") & after_days >= before_min) |
        (next_category == "SB" & before_days >= after_min_sb & previous_category %in% c("LB", "SB") & after_days >= before_min)
    ) %>%
    ungroup()
  
  final_df <- union_all(add_stillbirth_df, final_temp_df) %>%
    select(-previous_category, -next_category, -before_days, -after_days) %>%
    distinct()
  
  final_df
}

#' Add abortion episodes
#'
#' @param add_ectopic_df Previous episodes
#' @param Matcho_outcome_limits Outcome limits
#' @param final_abortion_visits_df Abortion visits
#'
#' @return Combined episodes
#' @export
add_abortion <- function(add_ectopic_df, Matcho_outcome_limits, final_abortion_visits_df, connection = NULL) {
  # Extract connection if not provided
  if (is.null(connection) && inherits(add_ectopic_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- add_ectopic_df$src$con
  }
  
  # Pre-compute SQL expressions outside the pipeline
  after_days_sql <- sql_translate("DATEDIFF(day, prev_date, visit_date)", connection)
  before_days_sql <- sql_translate("DATEDIFF(day, visit_date, next_date)", connection)
  # Add abortion visits - SA and AB are treated the same.
  
  # get minimum days between outcomes
  # minimum number of days that ECT can follow LB and SB; LB and SB have the same days
  before_min_lb <- Matcho_outcome_limits %>%
    filter(first_preg_category == "LB" & outcome_preg_category == "AB") %>%
    pull(min_days)
  
  before_min_ect <- Matcho_outcome_limits %>%
    filter(first_preg_category == "ECT" & outcome_preg_category == "AB") %>%
    pull(min_days)
  # minimum number of days that LB can follow ECT
  after_min_lb <- Matcho_outcome_limits %>%
    filter(first_preg_category == "AB" & outcome_preg_category == "LB") %>%
    pull(min_days)
  # minimum number of days that SB can follow ECT
  after_min_sb <- Matcho_outcome_limits %>%
    filter(first_preg_category == "AB" & outcome_preg_category == "SB") %>%
    pull(min_days)
  
  after_min_ect <- Matcho_outcome_limits %>%
    filter(first_preg_category == "AB" & outcome_preg_category == "ECT") %>%
    pull(min_days)
  
  # get difference in days with subsequent visit
  final_temp_df <- union_all(add_ectopic_df, select(final_abortion_visits_df, -any_of(c("gest_value", "value_as_number")))) %>%
    mutate(temp_category = ifelse(category == "SA", "AB", category)) %>%
    group_by(person_id) %>%
    arrange(visit_date, .by_group = TRUE) %>%
    mutate(
      # get previous category if available
      previous_category = lag(temp_category),
      # get difference in days with previous episode start date
      prev_date = lag(visit_date)) %>%
    mutate(after_days = !!after_days_sql,
      next_category = lead(temp_category),
      # and next episode start date
      next_date = lead(visit_date)) %>%
    mutate(before_days = !!before_days_sql
    ) %>%
    filter(temp_category == "AB") %>%
    filter(
      # don't have to worry about limits
      (is.na(before_days) & is.na(after_days)) |
        (!previous_category %in% c("LB", "SB", "ECT") & is.na(next_category)) |
        (!next_category %in% c("LB", "SB", "ECT") & is.na(previous_category)) |
        (!previous_category %in% c("LB", "SB", "ECT") & !next_category %in% c("LB", "SB", "ECT")) |
        
        # the last episode was a delivery and this one happens after the minimum
        (previous_category %in% c("LB", "SB") & after_days >= before_min_lb & is.na(next_category)) |
        (next_category == "LB" & before_days >= after_min_lb & is.na(previous_category)) |
        (next_category == "SB" & before_days >= after_min_sb & is.na(previous_category)) |
        (next_category == "LB" & previous_category %in% c("LB", "SB") & before_days >= after_min_lb & after_days >= before_min_lb) |
        (next_category == "SB" & previous_category %in% c("LB", "SB") & before_days >= after_min_sb & after_days >= before_min_lb) |
        (previous_category == "ECT" & after_days >= before_min_ect & is.na(next_category)) |
        (next_category == "ECT" & before_days >= after_min_ect & is.na(previous_category)) |
        (next_category == "ECT" & previous_category == "ECT" & before_days >= after_min_ect & after_days >= before_min_ect) |
        (next_category == "ECT" & previous_category %in% c("LB", "SB") & before_days >= after_min_ect & after_days >= before_min_lb) |
        (next_category == "LB" & previous_category == "ECT" & before_days >= after_min_lb & after_days >= before_min_ect) |
        (next_category == "SB" & previous_category == "ECT" & before_days >= after_min_sb & after_days >= before_min_ect)
    ) %>%
    ungroup()
  
  final_df <- union_all(add_ectopic_df, final_temp_df) %>%
    select(-previous_category, -next_category, -before_days, -after_days, -temp_category) %>%
    distinct()
  
  return(final_df)
}

#' Add delivery episodes
#'
#' @param add_abortion_df Previous episodes
#' @param Matcho_outcome_limits Outcome limits
#' @param final_delivery_visits_df Delivery visits
#'
#' @return Combined episodes
#' @export
add_delivery <- function(add_abortion_df, Matcho_outcome_limits, final_delivery_visits_df, connection = NULL) {
  # Extract connection if not provided
  if (is.null(connection) && inherits(add_abortion_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- add_abortion_df$src$con
  }
  
  # Pre-compute SQL expressions outside the pipeline
  after_days_sql <- sql_translate("DATEDIFF(day, prev_date, visit_date)", connection)
  before_days_sql <- sql_translate("DATEDIFF(day, visit_date, next_date)", connection)
  #  Add delivery record only visits
  
  # get minimum days between outcomes
  # minimum number of days that DELIV can follow LB and SB; LB and SB have the same days
  before_min_lb <- Matcho_outcome_limits %>%
    filter(first_preg_category == "LB" & outcome_preg_category == "DELIV") %>%
    pull(min_days)
  
  before_min_ect <- Matcho_outcome_limits %>%
    filter(first_preg_category == "ECT" & outcome_preg_category == "DELIV") %>%
    pull(min_days)
  # minimum number of days that LB can follow
  # to add: if there's LB or SB outcome before then, they should be given this delivery date
  after_min_lb <- Matcho_outcome_limits %>%
    filter(first_preg_category == "DELIV" & outcome_preg_category == "LB") %>%
    pull(min_days)
  # minimum number of days that SB can follow
  after_min_sb <- Matcho_outcome_limits %>%
    filter(first_preg_category == "DELIV" & outcome_preg_category == "SB") %>%
    pull(min_days)
  
  after_min_ect <- Matcho_outcome_limits %>%
    filter(first_preg_category == "DELIV" & outcome_preg_category == "ECT") %>%
    pull(min_days)
  
  # get difference in days with subsequent visit
  final_temp_df <- union_all(add_abortion_df, select(final_delivery_visits_df, -any_of(c("gest_value", "value_as_number")))) %>%
    mutate(temp_category = ifelse(category == "SA", "AB", category)) %>%
    group_by(person_id) %>%
    arrange(visit_date, .by_group = TRUE) %>%
    mutate(
      # get previous category if available
      previous_category = lag(temp_category),
      # get difference in days with previous episode start date
      prev_date = lag(visit_date)) %>%
    mutate(after_days = !!after_days_sql,
      next_category = lead(temp_category),
      # and next episode start date
      next_date = lead(visit_date)) %>%
    mutate(before_days = !!before_days_sql
    )
  
  # have the deliveries and all othe others
  # add this: want to move the LB or SB date earlier if there's an earlier delivery date
  add_abortion_df_rev <- final_temp_df %>%
    mutate(visit_date = if_else(
      !is.na(previous_category) &
        previous_category == "DELIV" & category %in% c("LB", "SB") &
        after_days < after_min_sb,
      lag(visit_date), visit_date
    )) %>%
    filter(category != "DELIV") %>%
    ungroup()
  
  final_temp_df <- final_temp_df %>%
    filter(category == "DELIV") %>%
    filter(
      # don't need to worry about timing
      (is.na(before_days) & is.na(after_days)) |
        (!previous_category %in% c("LB", "SB", "ECT", "AB") & is.na(next_category)) |
        (!next_category %in% c("LB", "SB", "ECT", "AB") & is.na(previous_category)) |
        (!previous_category %in% c("LB", "SB", "ECT", "AB") & !next_category %in% c("LB", "SB", "ECT", "AB")) |
        # timing
        (previous_category %in% c("LB", "SB") & after_days >= before_min_lb & is.na(next_category)) |
        (next_category == "LB" & before_days >= after_min_lb & is.na(previous_category)) |
        (next_category == "SB" & before_days >= after_min_sb & is.na(previous_category)) |
        (next_category == "LB" & previous_category %in% c("LB", "SB") & before_days >= after_min_lb & after_days >= before_min_lb) |
        (next_category == "SB" & previous_category %in% c("LB", "SB") & before_days >= after_min_sb & after_days >= before_min_lb) |
        (previous_category %in% c("ECT", "AB") & after_days >= before_min_ect & is.na(next_category)) |
        (next_category %in% c("ECT", "AB") & before_days >= after_min_ect & is.na(previous_category)) |
        (next_category %in% c("ECT", "AB") & previous_category %in% c("ECT", "AB") & before_days >= after_min_ect & after_days >= before_min_ect) |
        (next_category %in% c("ECT", "AB") & previous_category %in% c("LB", "SB") & before_days >= after_min_ect & after_days >= before_min_lb) |
        (next_category == "LB" & previous_category %in% c("ECT", "AB") & before_days >= after_min_lb & after_days >= before_min_ect) |
        (next_category == "SB" & previous_category %in% c("ECT", "AB") & before_days >= after_min_sb & after_days >= before_min_ect)
    ) %>%
    ungroup()
  
  final_df <- union_all(add_abortion_df_rev, final_temp_df) %>%
    select(-previous_category, -next_category, -before_days, -after_days, -temp_category) %>%
    distinct()
  
  # Try to get counts with better error handling
  tryCatch({
    counts <- final_df %>%
      count(category) %>%
      safe_collect()
    
    cat("Total preliminary episodes:\n")
    if (!is.null(counts) && nrow(counts) > 0) {
      for (i in 1:nrow(counts)) {
        cat(sprintf("  %s: %d\n", counts$category[i], counts$n[i]))
      }
    } else {
      cat("  Unable to retrieve episode counts (collection failed)\n")
    }
  }, error = function(e) {
    cat("Total preliminary episodes:\n")
    cat("  Unable to retrieve episode counts due to error: ", e$message, "\n")
  })
  
  return(final_df)
}

#' Calculate pregnancy start dates
#'
#' @param add_delivery_df Episodes with outcomes
#' @param Matcho_term_durations Term duration limits
#'
#' @return Episodes with calculated start dates
#' @export
calculate_start <- function(add_delivery_df, Matcho_term_durations, connection = NULL) {
  # Extract connection if not provided
  if (is.null(connection) && inherits(add_delivery_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- add_delivery_df$src$con
  }
  
  # Pre-compute SQL expressions outside the pipeline
  min_start_date_sql <- sql_translate("DATEADD(day, -CAST(min_term AS INT), visit_date)", connection)
  max_start_date_sql <- sql_translate("DATEADD(day, -CAST(max_term AS INT), visit_date)", connection)
  
  # Estimate start of pregnancies based on outcome type.
  
  # join tables
  term_df <- add_delivery_df %>%
    left_join(Matcho_term_durations, by = "category", copy = TRUE, suffix = c(".x", ".y")) %>%
    # based only on the outcome, when did pregnancy start
    # calculate latest start start date
    mutate(
      min_start_date = !!min_start_date_sql,
      # calculate earliest start date
      max_start_date = !!max_start_date_sql
    )
  
  return(term_df)
}

#' Get visits with gestational age information
#'
#' @param initial_pregnant_cohort_df Initial cohort
#' @param config Configuration list (optional)
#'
#' @return Visits with gestational weeks
#' @export
gestation_visits <- function(initial_pregnant_cohort_df, config = NULL, connection = NULL) {
  # Filter to visits with gestation period.
  
  # Extract connection from lazy query if not provided
  if (is.null(connection) && inherits(initial_pregnant_cohort_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- initial_pregnant_cohort_df$src$con
  }
  
  # Get gestational age concept IDs from config or use defaults
  if (is.null(config)) {
    # Default standard OMOP concepts for gestational age
    gestational_age_concepts <- c(3002209, 3048230, 3012266)
    max_gestational_weeks <- 44
  } else {
    # Use configured concept IDs
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
  
  # Debug: Check input data
  input_count <- safe_count(initial_pregnant_cohort_df)
  cat("[DEBUG] gestation_visits: Input has", 
      ifelse(is.na(input_count), "unknown", as.character(input_count)), "total records\n")
  
  # Get records with gestation period from HIP_concepts (where gest_value is populated)
  # For Databricks, we need to handle NULL values carefully
  gest_df <- initial_pregnant_cohort_df %>% 
    filter(!is.na(gest_value) & gest_value > 0 & gest_value <= max_gestational_weeks)
  
  gest_count <- safe_count(gest_df)
  cat("[DEBUG] gestation_visits: Found", 
      ifelse(is.na(gest_count), "unknown", as.character(gest_count)), 
      "records with populated gest_value\n")
  
  # Get records with gestational age in weeks from measurement table
  # This is crucial: for gestational age concepts that have NA in gest_value (HIP_concepts),
  # we need to use the actual measurement value from value_as_number
  other_gest_df <- initial_pregnant_cohort_df %>%
    filter(
      concept_id %in% gestational_age_concepts,
      # Handle both NULL and 0 for gest_value
      (is.na(gest_value) | gest_value == 0),
      !is.na(value_as_number),
      # also filter out 0 -- this is an error
      value_as_number > 0, 
      value_as_number <= max_gestational_weeks
    ) %>%
    mutate(gest_value = as.integer(value_as_number))
  
  other_count <- safe_count(other_gest_df)
  cat("[DEBUG] gestation_visits: Found", 
      ifelse(is.na(other_count), "unknown", as.character(other_count)), 
      "records with value_as_number for gestational age\n")
  
  # Combine tables - this ensures we get both:
  # 1. Records with gest_value from HIP_concepts
  # 2. Records with value_as_number for gestational age measurements
  all_gest_df <- union_all(gest_df, other_gest_df)
  
  # Check if result is empty and ensure column structure is preserved
  result_count <- safe_count(all_gest_df)
  
  if (is.na(result_count) || result_count == 0) {
    # Return empty result with preserved column structure
    # Check if we're working with a database query or local data
    if (inherits(initial_pregnant_cohort_df, c("tbl_lazy", "tbl_sql"))) {
      # Pre-compute SQL expressions for NULL values
      null_int_sql <- sql_translate("CAST(NULL AS INT)", connection)
      null_date_sql <- sql_translate("CAST(NULL AS DATE)", connection)
      null_bigint_sql <- sql_translate("CAST(NULL AS BIGINT)", connection)
      
      # For database queries, use SQL-compatible NULL casting
      empty_result <- initial_pregnant_cohort_df %>%
        head(0) %>%
        mutate(gest_value = !!null_int_sql)
      
      # Make sure key columns are present
      if (!"visit_date" %in% colnames(empty_result)) {
        empty_result <- empty_result %>%
          mutate(visit_date = !!null_date_sql)
      }
      if (!"person_id" %in% colnames(empty_result)) {
        empty_result <- empty_result %>%
          mutate(person_id = !!null_bigint_sql)
      }
    } else {
      # For local data frames, use R functions
      empty_result <- initial_pregnant_cohort_df %>%
        head(0) %>%
        mutate(gest_value = integer())
      
      if (!"visit_date" %in% colnames(empty_result)) {
        empty_result <- empty_result %>%
          mutate(visit_date = as.Date(character()))
      }
      if (!"person_id" %in% colnames(empty_result)) {
        empty_result <- empty_result %>%
          mutate(person_id = integer())
      }
    }
    
    return(empty_result)
  }
  
  # Debug: Check columns being returned
  return_cols <- colnames(all_gest_df)
  cat("[DEBUG] gestation_visits: Returning columns:", paste(return_cols, collapse=", "), "\n")
  
  # Verify visit_date is present
  if (!"visit_date" %in% return_cols) {
    cat("[WARNING] gestation_visits: visit_date column is missing!\n")
  }
  
  # Check row count
  row_count <- safe_count(all_gest_df)
  if (!is.na(row_count)) {
    cat("[DEBUG] gestation_visits: Returning", row_count, "rows\n")
  } else {
    cat("[DEBUG] gestation_visits: Unable to determine row count\n")
  }
  
  return(all_gest_df)
}

#' Identify gestation-based episodes
#'
#' @param gestation_visits_df Visits with gestational age
#' @param min_days Minimum days between episodes (default 70)
#' @param buffer_days Buffer days for calculations (default 28)
#' @param config Configuration list (optional)
#'
#' @return Gestation episodes
#' @export
gestation_episodes <- function(gestation_visits_df, min_days = 70, buffer_days = 28, config = NULL, connection = NULL) {
  # Extract connection if not provided
  if (is.null(connection) && inherits(gestation_visits_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- gestation_visits_df$src$con
  }
  # minimum number of days to be new distinct episode
  # number of days to use as a buffer
  
  # Debug: Log input structure
  cat("\n[DEBUG] gestation_episodes: Starting with", 
      ifelse(inherits(gestation_visits_df, c("tbl_lazy", "tbl_sql")), "database", "local"),
      "data frame\n")
  
  # Check columns in input
  input_cols <- colnames(gestation_visits_df)
  cat("[DEBUG] gestation_episodes: Input columns:", paste(input_cols, collapse=", "), "\n")
  
  # Check if there are any gestation visits
  visit_count <- safe_count(gestation_visits_df)
  
  if (!is.na(visit_count) && visit_count > 0) {
    cat("[DEBUG] gestation_episodes: Found", visit_count, "gestation visits\n")
  } else {
    if (is.na(visit_count)) {
      cat("[DEBUG] gestation_episodes: Unable to determine visit count (NA returned)\n")
    } else {
      cat("[DEBUG] gestation_episodes: No gestation visits found (count = 0)\n")
    }
    visit_count <- 0  # Treat as 0 for the purpose of the check below
  }
  
  if (is.na(visit_count) || visit_count == 0) {
    cat("[DEBUG] gestation_episodes: Returning empty result with required columns\n")
    # Return empty data frame with expected columns if no gestation visits
    # Create an empty result with all required columns
    # Ensure we have all the columns that get_min_max_gestation expects
    
    # Check if we're working with a database table
    if (inherits(gestation_visits_df, c("tbl_lazy", "tbl_sql"))) {
      # Pre-compute SQL expressions for NULL values
      null_int_sql <- sql_translate("CAST(NULL AS INT)", connection)
      null_date_sql <- sql_translate("CAST(NULL AS DATE)", connection)
      null_bigint_sql <- sql_translate("CAST(NULL AS BIGINT)", connection)
      
      # For database tables, create proper empty structure
      empty_result <- gestation_visits_df %>%
        head(0)
      
      # Add required columns if they don't exist
      if (!"gest_week" %in% colnames(empty_result)) {
        empty_result <- empty_result %>%
          mutate(gest_week = !!null_int_sql)
      }
      if (!"episode" %in% colnames(empty_result)) {
        empty_result <- empty_result %>%
          mutate(episode = !!null_int_sql)
      }
      if (!"visit_date" %in% colnames(empty_result)) {
        empty_result <- empty_result %>%
          mutate(visit_date = !!null_date_sql)
      }
      if (!"person_id" %in% colnames(empty_result)) {
        empty_result <- empty_result %>%
          mutate(person_id = !!null_bigint_sql)
      }
      
      # Select only the columns we need
      empty_result <- empty_result %>%
        select(any_of(c("person_id", "visit_date", "gest_week", "episode")))
    } else {
      # For local data frames
      empty_result <- data.frame(
        person_id = integer(),
        visit_date = as.Date(character()),
        gest_week = integer(),
        episode = integer()
      )
    }
    
    return(empty_result)
  }
  
  # Define pregnancy episode per patient by gestational records.
  #
  # Any record with a negative change or no change in the gestational age in
  # weeks from the previous record is flagged as the start of a potential
  # episode. This record is then checked if there is at least a separation of
  # 70 days from the previous record. The number of days, 70, was determined
  # by taking the minimum outcome limit in days from Matcho et al. (56) and
  # adding a buffer of 14 days. If the record is not at least 70 days from the
  # previous record, it is no longer flagged as the start of an episode.
  #
  # For all records with a positive change in the gestational age in weeks
  # from the previous record is then checked if the date difference in days is
  # greater than the difference in days between the record's gestational age
  # in weeks and the previous record's gestational age in weeks with a buffer
  # of 28 days. The buffer of 28 days was determined by taking the minimum
  # retry period in days from Matcho et al. (14) and adding 14 days as a
  # buffer. If the date difference in days is greater than the difference in
  # days between the record's gestational age in weeks and the previous
  # record's gestational age in weeks with the buffer, then this record is
  # flagged as a start of a new episode.
  
  # Get max gestational weeks from config or use default
  max_gestational_weeks <- if (!is.null(config) && !is.null(config$algorithm_parameters$max_gestational_weeks)) {
    config$algorithm_parameters$max_gestational_weeks
  } else {
    44
  }
  
  # Pre-compute SQL expression for date difference
  date_diff_sql <- sql_translate("DATEDIFF(day, prev_date, visit_date)", connection)
  
  # Pre-compute CASE WHEN expressions to avoid IIF function issues in Spark
  # For new_diff: if (date_diff < min_days & week_diff <= 0) then 1 else week_diff
  new_diff_sql <- sql_case_when(
    paste0("date_diff < ", min_days, " AND week_diff <= 0"),
    "1",
    "week_diff",
    connection
  )
  
  # For new_diff2: if (date_diff >= day_diff & week_diff > 0) then -1 else new_diff
  new_diff2_sql <- sql_case_when(
    "date_diff >= day_diff AND week_diff > 0",
    "-1",
    "new_diff",
    connection
  )
  
  # For episode indicator: if (new_diff2 <= 0 or row_index = 1) then 1 else 0
  episode_indicator_sql <- sql_case_when(
    "new_diff2 <= 0 OR row_index = 1",
    "1",
    "0",
    connection
  )
  
  # SQL Server-specific path: collect simple data and compute locally
  # This avoids SQL Server's limitations with complex window functions
  dbms <- tryCatch({
    attr(connection, "dbms", exact = TRUE)
  }, error = function(e) NULL)
  
  # Check if we're on SQL Server and the data size is manageable
  if (!is.null(dbms) && dbms == "sql server" && visit_count < 100000) {
    cat("[DEBUG] gestation_episodes: SQL Server detected, using local computation to avoid window function issues\n")
    
    # First filter and collect the simple data
    df_local <- gestation_visits_df %>%
      filter(
        !is.na(visit_date),
        gest_value > 0 & gest_value <= max_gestational_weeks
      ) %>%
      collect()  # Collect the simple data locally
    
    if (!is.null(df_local) && nrow(df_local) > 0) {
      cat("[DEBUG] gestation_episodes: Collected", nrow(df_local), "rows locally for processing\n")
      
      # Do all computations locally in R
      df_processed <- df_local %>%
        # keep max gest_value if two or more gestational records share same date
        group_by(person_id, visit_date) %>%
        mutate(gest_week = floor(max(gest_value))) %>%  # Use floor to match SQL behavior
        ungroup() %>%
        # filter out rows that are not the max gest_value at visit_date
        filter(floor(gest_value) == gest_week) %>%
        # add column for gestation period in days
        mutate(gest_day = gest_week * 7) %>%
        group_by(person_id) %>%
        arrange(visit_date, .by_group = TRUE) %>%
        mutate(
          # get previous gestation week
          prev_week = lag(gest_week, 1),
          # get previous date
          prev_date = lag(visit_date, 1),
          # calculate difference between gestation weeks
          week_diff = gest_week - prev_week,
          # calculate number of days between gestation weeks with buffer
          day_diff = week_diff * 7 + buffer_days,
          # get difference in days between visit date and previous date
          date_diff = as.numeric(visit_date - prev_date),
          # Apply episode logic using R's ifelse
          new_diff = ifelse(date_diff < min_days & week_diff <= 0, 1, week_diff),
          new_diff2 = ifelse(date_diff >= day_diff & week_diff > 0, -1, new_diff),
          # create new columns, index and episode
          row_index = row_number(),
          # count as an episode if first row or conditions met
          episode_indicator = (row_index == 1) | (new_diff2 <= 0),
          episode = cumsum(as.integer(episode_indicator))
        ) %>%
        ungroup() %>%
        # Select only the required columns and ensure proper types
        select(person_id, visit_date, gest_week, episode) %>%
        mutate(
          gest_week = as.integer(gest_week),
          episode = as.integer(episode)
        )
      
      cat("[DEBUG] gestation_episodes: Processed", nrow(df_processed), "episodes locally\n")
      
      # Upload back as temp table
      result <- create_temp_table(df_processed, connection = connection)
      
      cat("[DEBUG] gestation_episodes: Uploaded result as temp table\n")
      
      # Return early, skipping all the SQL-based processing
      return(result)
    }
  }
  
  # Standard SQL-based path for non-SQL Server databases
  # filter out any empty visit dates
  df <- gestation_visits_df %>%
    filter(
      !is.na(visit_date),
      # remove any records with incorrect gestational weeks (i.e. 9999999)
      gest_value > 0 & gest_value <= max_gestational_weeks
    ) %>%
    # keep max gest_value if two or more gestational records share same date
    group_by(person_id, visit_date) %>%
    mutate(gest_week = max(gest_value)) %>%
    ungroup() %>%
    # filter out rows that are not the max gest_value at visit_date
    filter(gest_value == gest_week) %>%
    # add column for gestation period in days
    mutate(gest_day = gest_week * 7) %>%
    group_by(person_id) %>%
    arrange(visit_date, .by_group = TRUE) %>%
    mutate(
      # get previous gestation week
      prev_week = lag(gest_week, 1),
      # get previous date
      prev_date = lag(visit_date, 1),
      # calculate difference between gestation weeks
      week_diff = gest_week - prev_week,
      # calculate number of days between gestation weeks with buffer
      day_diff = week_diff * 7 + buffer_days,
      # get difference in days between visit date and previous date
      date_diff = !!date_diff_sql,
      # check if any negative or zero number in week_diff column corresponds to a new pregnancy episode
      # assume it does if the difference in actual dates is larger than the minimum
      # change to 1 (arbitrary positive number) if not;
      # new_diff = 1 if the next obs has lower gest week and the difference in dates
      # is smaller than the minimum number of days between pregnancies
      # week_diff is negative if at a lower gestational age now
      # Use pre-computed CASE WHEN instead of if_else to avoid IIF function in Spark
      new_diff = !!new_diff_sql,
      # check if any positive number in week_diff column (so at a higher gestational age next time)
      # has a date_diff >= day_diff
      # that means that the difference in time is greater than the difference
      # in gestational age + buffer
      # may correspond to new pregnancy episode, if so change to -1 (negative number)
      # Use pre-computed CASE WHEN instead of if_else to avoid IIF function in Spark
      new_diff2 = !!new_diff2_sql,
      # create new columns, index and episode; any zero or negative number in newdiff2 column indicates a new episode
      row_index = row_number(),
      # count as an episodes if first row or
      # difference in gest age is negative and date_diff large enough for it to be a new pregnancy or
      # difference in gest age is positive but that difference is larger than the difference in dates
      # Use pre-computed CASE WHEN instead of ifelse to avoid IIF function in Spark
      episode_indicator = !!episode_indicator_sql,
      episode = as.integer(cumsum(episode_indicator)),
      episode_chr = as.character(episode) # for grouping
    ) %>%
    ungroup()
  
  # Debug: Check final columns before returning
  final_cols <- colnames(df)
  cat("[DEBUG] gestation_episodes: Final columns before selection:", paste(final_cols, collapse=", "), "\n")
  
  # HYBRID FIX for SQL Server: Select columns FIRST, then materialize the simple result
  # This avoids trying to materialize complex window functions while preventing day_diff reference errors
  if (inherits(df, c("tbl_lazy", "tbl_sql"))) {
    # Get connection
    conn <- if (!is.null(connection)) {
      connection
    } else if (!is.null(df$src$con)) {
      df$src$con
    } else {
      NULL
    }
    
    # First, select only the needed columns
    # This resolves all complex SQL into just 4 simple columns
    cat("[DEBUG] gestation_episodes: Selecting required columns first\n")
    result <- df %>%
      select(person_id, visit_date, gest_week, episode)
    
    # Now materialize just these 4 simple columns
    # SQL Server can handle this because it's not complex
    cat("[DEBUG] gestation_episodes: Materializing simplified 4-column result\n")
    result <- tryCatch({
      compute_table(result, connection = conn)
    }, error = function(e) {
      cat("[DEBUG] gestation_episodes: compute_table failed, trying local collection:", e$message, "\n")
      # If even simple materialization fails, collect locally
      result_local <- tryCatch({
        result %>% collect()
      }, error = function(e2) {
        cat("[DEBUG] gestation_episodes: Direct collection also failed:", e2$message, "\n")
        # Last resort: try collecting the original df and selecting locally
        df_local <- df %>% collect()
        if (!is.null(df_local) && nrow(df_local) > 0) {
          df_local %>% select(person_id, visit_date, gest_week, episode)
        } else {
          stop("Failed to collect gestation episodes: ", e2$message)
        }
      })
      
      if (!is.null(result_local) && nrow(result_local) > 0) {
        cat("[DEBUG] gestation_episodes: Re-uploading", nrow(result_local), "rows as temp table\n")
        create_temp_table(result_local, connection = conn)
      } else {
        stop("Failed to materialize gestation episodes: ", e$message)
      }
    })
    
    cat("[DEBUG] gestation_episodes: Successfully materialized result\n")
  } else {
    # For local data frames, just select the columns
    result <- df %>%
      select(person_id, visit_date, gest_week, episode)
  }
  
  # Debug: Verify we have the required columns
  result_cols <- colnames(result)
  cat("[DEBUG] gestation_episodes: Returning columns:", paste(result_cols, collapse=", "), "\n")
  
  # Final count for debugging
  result_count <- safe_count(result)
  if (!is.na(result_count)) {
    cat("[DEBUG] gestation_episodes: Returning", result_count, "rows\n")
  } else {
    cat("[DEBUG] gestation_episodes: Unable to determine row count\n")
  }
  
  return(result)
}

#' Get min and max gestational ages
#'
#' @param gestation_episodes_df Gestation episodes
#'
#' @return Episodes with min/max gestational ages
#' @export
get_min_max_gestation <- function(gestation_episodes_df, connection = NULL) {
  # Get the min and max gestational age in weeks and the corresponding visit
  # dates per pregnancy episode.
  #
  # Also get the first and last visits and their corresponding gestational age
  # in weeks per pregnancy episode.
  #
  # For minimum gestational age in weeks, the first and last occurrence and
  # their dates were obtained.
  
  # Debug: Log input structure
  cat("\n[DEBUG] get_min_max_gestation: Starting with", 
      ifelse(inherits(gestation_episodes_df, c("tbl_lazy", "tbl_sql")), "database", "local"),
      "data frame\n")
  
  # Check columns in input
  input_cols <- colnames(gestation_episodes_df)
  cat("[DEBUG] get_min_max_gestation: Input columns:", paste(input_cols, collapse=", "), "\n")
  
  # Check for required columns
  required_cols <- c("person_id", "episode", "visit_date", "gest_week")
  missing_cols <- setdiff(required_cols, input_cols)
  if (length(missing_cols) > 0) {
    cat("[ERROR] get_min_max_gestation: Missing required columns:", paste(missing_cols, collapse=", "), "\n")
    cat("[ERROR] get_min_max_gestation: Available columns are:", paste(input_cols, collapse=", "), "\n")
  }
  
  # SMART FIX: Only collect and re-upload if necessary
  if (inherits(gestation_episodes_df, c("tbl_lazy", "tbl_sql"))) {
    # Check if this is already a computed temp table
    # temp tables typically have names like "temp_xxxxx" or are from compute_table
    query_str <- tryCatch({
      as.character(gestation_episodes_df$lazy_query$x)
    }, error = function(e) "")
    
    is_temp_table <- any(grepl("temp_|#temp", query_str, ignore.case = TRUE))
    
    # Also check if this came from gestation_episodes which already materialized
    # Use any() to ensure we get a single logical value, not a vector
    is_from_gestation_episodes <- any(grepl("gestation_episodes: Successfully materialized", 
                                            capture.output(print(gestation_episodes_df)), 
                                            ignore.case = TRUE))
    
    if (is_temp_table || is_from_gestation_episodes) {
      cat("[DEBUG] get_min_max_gestation: Input is already materialized, skipping collection\n")
      # Data is already in a temp table, no need to collect and re-upload
    } else {
      # Only collect if we have a complex lazy query that needs breaking
      cat("[DEBUG] get_min_max_gestation: Complex query detected, attempting collection\n")
      
      # Get connection first
      connection <- gestation_episodes_df$src$con
      
      # Try arrow_safe_collect for large datasets
      if (exists("arrow_safe_collect")) {
        temp_data <- tryCatch({
          arrow_safe_collect(gestation_episodes_df, 
                           threshold = 100000, 
                           connection = connection)
        }, error = function(e) {
          cat("[DEBUG] get_min_max_gestation: arrow_safe_collect failed:", e$message, "\n")
          # Fall back to regular safe_collect
          gestation_episodes_df %>% safe_collect()
        })
      } else {
        temp_data <- gestation_episodes_df %>% safe_collect()
      }
      
      # Check if collection returned empty or NULL
      if (is.null(temp_data) || nrow(temp_data) == 0) {
        cat("[DEBUG] get_min_max_gestation: Collection returned empty data (0 rows)\n")
        
        # Create empty data frame with expected columns
        empty_result <- data.frame(
          person_id = integer(),
          episode = integer(),
          max_gest_week = integer(),
          max_gest_date = as.Date(character()),
          min_gest_week = integer(),
          min_gest_date = as.Date(character()),
          min_gest_week_2 = integer(),
          min_gest_date_2 = as.Date(character()),
          stringsAsFactors = FALSE
        )
        
        return(empty_result)
      }
      
      cat("[DEBUG] get_min_max_gestation: Collected", nrow(temp_data), "rows\n")
      
      # Re-upload as a clean temp table only if we have data
      gestation_episodes_df <- create_temp_table(temp_data, connection = connection)
      cat("[DEBUG] get_min_max_gestation: Re-uploaded as clean temp table\n")
    }
  }
  
  # Check if there are any gestation episodes
  episode_count <- safe_count(gestation_episodes_df)
  
  if (!is.na(episode_count)) {
    cat("[DEBUG] get_min_max_gestation: Working with", episode_count, "gestation episodes\n")
  } else {
    cat("[DEBUG] get_min_max_gestation: Unable to determine episode count, treating as 0\n")
    episode_count <- 0  # Treat NA as 0 for safety
  }
  
  if (is.na(episode_count) || episode_count == 0) {
    # Return empty database table with expected columns if no gestation episodes
    # Check if we're working with a database table
    if (inherits(gestation_episodes_df, c("tbl_lazy", "tbl_sql"))) {
      # Return an empty database table with the right structure
      empty_result <- gestation_episodes_df %>%
        head(0) %>%
        mutate(
          first_gest_week = numeric(),
          end_gest_date = as.Date(character()),
          end_gest_week = numeric(),
          min_gest_week = numeric(),
          min_gest_date = as.Date(character()),
          gest_week = numeric(),
          min_gest_date_2 = as.Date(character()),
          max_gest_week = numeric(),
          max_gest_date = as.Date(character())
        ) %>%
        select(person_id, episode, first_gest_week, end_gest_date, end_gest_week,
               min_gest_week, min_gest_date, gest_week, min_gest_date_2,
               max_gest_week, max_gest_date)
      return(empty_result)
    } else {
      # Return a local data frame
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
  }
  
  ############ First Visit Date ############
  
  # identify first visit for each pregnancy episode
  # and get max gestation week at first visit date
  # Note: gestation_episodes_df is now a clean temp table after collect/re-upload
  
  # Get min visit date per episode
  min_visits <- gestation_episodes_df %>%
    group_by(person_id, episode) %>%
    summarise(min_visit_date = min(visit_date, na.rm = TRUE), .groups = "drop") %>%
    compute_table(connection = connection)
  
  # Then join back to get gest_week at that date
  new_first_df <- gestation_episodes_df %>%
    inner_join(min_visits, by = c("person_id", "episode"), suffix = c(".x", ".y")) %>%
    filter(visit_date == min_visit_date) %>%
    group_by(person_id, episode) %>%
    summarise(first_gest_week = max(gest_week, na.rm = TRUE), .groups = "drop") %>%
    compute_table(connection = connection)
  
  ############ Min Gestation Week ############
  
  # identify minimum gestation week for each pregnancy episode
  # First get min gest_week per episode
  min_weeks <- gestation_episodes_df %>%
    group_by(person_id, episode) %>%
    summarise(min_gest_week = min(gest_week, na.rm = TRUE), .groups = "drop") %>%
    compute_table(connection = connection)
  
  # Then join back to get all records with that min week
  temp_min_df <- gestation_episodes_df %>%
    inner_join(min_weeks, by = c("person_id", "episode"), suffix = c(".x", ".y")) %>%
    filter(gest_week == min_gest_week) %>%
    compute_table(connection = connection)
  
  # get range of time when that gestational week was recorded
  # get first occurrence of min gestation week
  new_min_df <- temp_min_df %>%
    mutate(min_gest_week = gest_week) %>%
    group_by(person_id, episode, min_gest_week) %>%
    summarize(min_gest_date = min(visit_date, na.rm = TRUE), .groups = "drop") %>%
    compute_table(connection = connection)
  
  # get last occurrence of min gestation week
  second_min_df <- temp_min_df %>%
    group_by(person_id, episode, gest_week) %>% # = min_gest_week
    summarize(min_gest_date_2 = max(visit_date, na.rm = TRUE), .groups = "drop") %>%
    compute_table(connection = connection)
  
  ############ End Visit Date ############
  
  # identify end visit for each pregnancy episode
  # keep in mind this could be a month after pregnancy actually ended...
  # First get max visit date per episode
  max_visits <- gestation_episodes_df %>%
    group_by(person_id, episode) %>%
    summarise(max_visit_date = max(visit_date, na.rm = TRUE), .groups = "drop") %>%
    compute_table(connection = connection)
  
  # Then join back to get records at that date
  temp_end_df <- gestation_episodes_df %>%
    inner_join(max_visits, by = c("person_id", "episode"), suffix = c(".x", ".y")) %>%
    filter(visit_date == max_visit_date) %>%
    mutate(end_gest_date = visit_date)
  
  # get max gestation week at end visit date
  new_end_df <- temp_end_df %>%
    group_by(person_id, episode, end_gest_date) %>%
    summarize(end_gest_week = max(gest_week, na.rm = TRUE), .groups = "drop") %>%
    compute_table(connection = connection)
  
  ############ Max Gestation Week ############
  
  # identify max gestation week for each pregnancy episode
  # First get max gest_week per episode
  max_weeks <- gestation_episodes_df %>%
    group_by(person_id, episode) %>%
    summarise(max_gest_week = max(gest_week, na.rm = TRUE), .groups = "drop") %>%
    compute_table(connection = connection)
  
  # Then join back to get all records with that max week
  temp_max_df <- gestation_episodes_df %>%
    inner_join(max_weeks, by = c("person_id", "episode"), suffix = c(".x", ".y")) %>%
    filter(gest_week == max_gest_week) %>%
    compute_table(connection = connection)
  
  # get first occurrence of max gestation week
  new_max_df <- temp_max_df %>%
    group_by(person_id, episode, max_gest_week) %>%
    summarize(max_gest_date = min(visit_date, na.rm = TRUE), .groups = "drop") %>%
    compute_table(connection = connection)
  
  # max_gest_date can be later than min_gest_date_2 (only one GA but multiple dates)
  
  ############ Join tables ############
  
  # join first and end tables (all already computed)
  all_df <- new_first_df %>%
    inner_join(new_end_df, by = c("person_id", "episode"), suffix = c(".x", ".y")) %>%
    inner_join(new_min_df, by = c("person_id", "episode"), suffix = c(".x", ".y")) %>%
    inner_join(second_min_df, by = c("person_id", "episode"), suffix = c(".x", ".y")) %>%
    inner_join(new_max_df, by = c("person_id", "episode"), suffix = c(".x", ".y"))
  
  return(all_df)
}

#' Add gestation episodes to outcome episodes
#'
#' @param calculate_start_df Outcome episodes
#' @param get_min_max_gestation_df Gestation episodes
#'
#' @return Combined episodes
#' @export
add_gestation <- function(calculate_start_df, get_min_max_gestation_df, buffer_days = 28, connection = NULL) {
  # Extract connection if not provided
  if (is.null(connection) && inherits(calculate_start_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- calculate_start_df$src$con
  }
  
  # Check if gestation data is empty
  gest_count <- if (inherits(get_min_max_gestation_df, c("tbl_lazy", "tbl_sql"))) {
    safe_count(get_min_max_gestation_df)
  } else {
    nrow(get_min_max_gestation_df)
  }
  
  if (is.na(gest_count) || gest_count == 0) {
    cat("[DEBUG] add_gestation: No gestation episodes to add, returning outcome-only episodes\n")
    # Return just the outcome episodes with episode numbering
    return(calculate_start_df %>%
      group_by(person_id) %>%
      arrange(visit_date, .by_group = TRUE) %>%
      mutate(episode = row_number()) %>%
      ungroup())
  }
  
  # Pre-compute all SQL expressions outside pipelines
  visit_id_sql <- sql_translate("CONCAT(CAST(person_id AS VARCHAR), CAST(visit_date AS VARCHAR))", connection)
  gest_id_sql <- sql_translate("CONCAT(CAST(person_id AS VARCHAR), CAST(max_gest_date AS VARCHAR))", connection)
  max_gest_start_sql <- sql_translate("DATEADD(day, -CAST((max_gest_week * 7) AS INT), max_gest_date)", connection)
  min_gest_start_sql <- sql_translate("DATEADD(day, -CAST((min_gest_week * 7) AS INT), min_gest_date)", connection)
  gest_diff_sql <- sql_translate("DATEDIFF(day, min_gest_start_date, max_gest_start_date)", connection)
  
  # Add gestation-based episodes. Any gestation-based episode that overlaps with an outcome-based
  # episode is removed as a distinct episode.
  # add unique id for each outcome visit
  calculate_start_df <- calculate_start_df %>%
    # visit date is the first outcome date for the hierarchically chosen outcome
    mutate(visit_id = !!visit_id_sql)
  
  # add unique id for each gestation visit
  # FIXED: Split complex mutate into multiple steps to avoid column reassignment issues
  get_min_max_gestation_df <- get_min_max_gestation_df %>%
    # First mutate: Create basic columns and initial date calculations
    mutate(
      gest_id = !!gest_id_sql,
      # add column for gestation period in days for largest gestation week on record
      max_gest_day = (max_gest_week * 7),
      # add column for gestation period in days for smallest gestation week on record
      min_gest_day = (min_gest_week * 7),
      # get date of estimated start date based on max gestation week on record
      # max_gest_date is the first occurrence of the maximum gestational week
      max_gest_start_date_initial = !!max_gest_start_sql,
      # get date of estimated start date based on min gestation week on record
      # min_gest_date is the first occurence of the min gestational week
      min_gest_start_date_initial = !!min_gest_start_sql
    ) %>%
    # Second mutate: Determine which date is earlier/later
    mutate(
      # which one is earlier (will become max_gest_start_date)
      max_gest_start_date = if_else(
        max_gest_start_date_initial > min_gest_start_date_initial,
        min_gest_start_date_initial, max_gest_start_date_initial
      ),
      # which one is later (will become min_gest_start_date)
      min_gest_start_date = if_else(
        max_gest_start_date_initial > min_gest_start_date_initial,
        max_gest_start_date_initial, min_gest_start_date_initial
      )
    ) %>%
    # Third mutate: Calculate the difference and create final structure
    mutate(
      # Store the earlier date for later reference
      max_gest_start_date_further = max_gest_start_date,
      # get difference in days between estimated start dates
      gest_start_date_diff = !!gest_diff_sql
    )
  
  # CRITICAL: Materialize data before overlaps join to ensure it works correctly
  # The overlaps join may not translate properly with lazy queries
  if (inherits(calculate_start_df, c("tbl_lazy", "tbl_sql"))) {
    calculate_start_df <- calculate_start_df %>% compute_table(connection = connection)
  }
  if (inherits(get_min_max_gestation_df, c("tbl_lazy", "tbl_sql"))) {
    get_min_max_gestation_df <- get_min_max_gestation_df %>% compute_table(connection = connection)
  }
  
  # Debug: Check counts before join
  outcome_count <- safe_count(calculate_start_df)
  gest_count <- safe_count(get_min_max_gestation_df)
  
  cat("\n=== DEBUG: Overlap Join Analysis ===\n")
  if (!is.na(outcome_count)) {
    cat(sprintf("Outcome-based episodes: %d\n", as.integer(outcome_count)))
  } else {
    cat("Outcome-based episodes: Unable to count\n")
  }
  if (!is.na(gest_count)) {
    cat(sprintf("Gestation-based episodes: %d\n", as.integer(gest_count)))
  } else {
    cat("Gestation-based episodes: Unable to count\n")
  }
  
  # join both tables to find overlaps
  # First do a Cartesian join on person_id, then filter for overlaps
  # This ensures we get all person matches before checking date overlaps
  both_df_candidates <- inner_join(calculate_start_df, get_min_max_gestation_df,
    by = "person_id",
    suffix = c(".x", ".y")
  )
  
  # Debug: Check candidate count
  candidate_count <- safe_count(both_df_candidates)
  if (!is.na(candidate_count)) {
    cat(sprintf("Candidate pairs (same person): %d\n", as.integer(candidate_count)))
  } else {
    cat("Candidate pairs (same person): Unable to count\n")
  }
  
  # Pre-compute SQL expressions for the overlap filtering
  gest_at_outcome_sql <- sql_translate("DATEDIFF(day, max_gest_start_date, visit_date)", connection)
  days_diff_sql <- sql_translate("DATEDIFF(day, max_gest_date, visit_date)", connection)
  
  # Now filter for overlapping date ranges
  both_df <- both_df_candidates %>%
    filter(
      # Check if the two date ranges overlap
      # Range 1: [max_start_date, visit_date]
      # Range 2: [max_gest_start_date, max_gest_date]
      # They overlap if start1 <= end2 AND end1 >= start2
      max_start_date <= max_gest_date & visit_date >= max_gest_start_date
    ) %>%
    # Check for any gestation-based episodes that overlap with more than one outcome-based
    # episode and keep only those episodes where the gestation-based end date is closest to the
    # outcome date.
    mutate(
      # add -- these are changed anyway so if there are multiple similar overlaps, choose
      # the one with the better term duration
      # visit date should be the first visit date at which there's an outcome
      gest_at_outcome = !!gest_at_outcome_sql,
      # we want it to be under the max
      is_under_max = ifelse(gest_at_outcome <= max_term, 1, 0),
      # and over the min, ie both = 1
      is_over_min = ifelse(gest_at_outcome >= min_term, 1, 0),
      days_diff = !!days_diff_sql,
      days_diff = if_else(is_over_min == 1 | is_under_max == 1 | days_diff < -buffer_days, 10000, days_diff)
    ) %>%
    group_by(visit_id) %>%
    slice_min(order_by = abs(days_diff), n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    group_by(gest_id) %>%
    slice_min(order_by = abs(days_diff), n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    compute_table(connection = connection)
  
  # Debug: Check overlapping episodes count
  overlap_count <- safe_count(both_df)
  if (!is.na(overlap_count)) {
    cat(sprintf("After overlap filter - Overlapping episodes: %d\n", as.integer(overlap_count)))
  } else {
    cat("After overlap filter - Overlapping episodes: Unable to count\n")
  }
  cat("=== END DEBUG ===\n\n")
  
  # Get IDs from overlapping episodes for exclusion
  overlapping_visit_ids <- both_df %>% 
    select(visit_id) %>% 
    distinct() %>%
    compute_table(connection = connection)
  
  overlapping_gest_ids <- both_df %>% 
    select(gest_id) %>% 
    distinct() %>%
    compute_table(connection = connection)
  
  # only outcome-based episodes
  # Pre-compute NULL casts for union compatibility
  null_varchar_sql <- sql_translate("CAST(NULL AS VARCHAR(255))", connection)
  null_date_sql <- sql_translate("CAST(NULL AS DATE)", connection)
  null_int_sql <- sql_translate("CAST(NULL AS INT)", connection)
  
  # Add missing gest_id column as NULL with proper type casting for union compatibility
  just_outcome_df <- calculate_start_df %>%
    anti_join(overlapping_visit_ids, by = "visit_id") %>%
    mutate(gest_id = !!null_varchar_sql)
  
  # only gestation-based episodes
  # Add missing visit_id column as NULL with proper type casting for union compatibility
  just_gestation_df <- get_min_max_gestation_df %>%
    anti_join(overlapping_gest_ids, by = "gest_id") %>%
    mutate(
      category = "PREG",
      # visit date becomes
      visit_date = max_gest_date,
      visit_id = !!null_varchar_sql
    )
  
  # Instead of selecting essential columns, ensure all dataframes have ALL columns
  # First, materialize the overlapping episodes
  both_df_mat <- both_df %>% compute_table(connection = connection)
  
  # Add ALL missing columns from both_df to the other dataframes
  # This ensures union compatibility without losing any columns
  
  # For outcome-only episodes: add all gestation-related columns as NULL
  just_outcome_df_full <- just_outcome_df %>%
    mutate(
      # These columns come from gestation episodes
      max_gest_date = !!null_date_sql,
      min_gest_start_date = !!null_date_sql,
      max_gest_start_date = !!null_date_sql,
      gest_start_date_diff = !!null_int_sql,
      is_under_max = !!null_int_sql,
      is_over_min = !!null_int_sql,
      gest_at_outcome = !!null_int_sql,
      days_diff = !!null_int_sql
    ) %>%
    compute_table(connection = connection)
  
  # For gestation-only episodes: add all outcome-related columns as NULL
  just_gestation_df_full <- just_gestation_df %>%
    mutate(
      # These columns come from outcome episodes
      min_start_date = !!null_date_sql,
      max_start_date = !!null_date_sql,
      is_under_max = !!null_int_sql,
      is_over_min = !!null_int_sql,
      gest_at_outcome = !!null_int_sql,
      gest_start_date_diff = !!null_int_sql,
      days_diff = !!null_int_sql
    ) %>%
    compute_table(connection = connection)
  
  # Debug: Check component counts
  both_count <- safe_count(both_df_mat)
  outcome_only_count <- safe_count(just_outcome_df_full)
  gestation_only_count <- safe_count(just_gestation_df_full)
  
  cat("\n=== DEBUG: Union Components ===\n")
  if (!is.na(both_count)) {
    cat(sprintf("Overlapping episodes: %d\n", as.integer(both_count)))
  } else {
    cat("Overlapping episodes: Unable to count\n")
  }
  if (!is.na(outcome_only_count)) {
    cat(sprintf("Outcome-only episodes: %d\n", as.integer(outcome_only_count)))
  } else {
    cat("Outcome-only episodes: Unable to count\n")
  }
  if (!is.na(gestation_only_count)) {
    cat(sprintf("Gestation-only episodes: %d\n", as.integer(gestation_only_count)))
  } else {
    cat("Gestation-only episodes: Unable to count\n")
  }
  if (!is.na(both_count) && !is.na(outcome_only_count) && !is.na(gestation_only_count)) {
    cat(sprintf("Expected total: %d\n", as.integer(both_count) + as.integer(outcome_only_count) + as.integer(gestation_only_count)))
  } else {
    cat("Expected total: Unable to calculate\n")
  }
  
  # Pre-compute SQL for days_diff recalculation
  days_diff_recalc_sql <- sql_translate("DATEDIFF(day, max_gest_date, visit_date)", connection)
  
  all_df <- reduce(
    list(
      both_df_mat,
      just_outcome_df_full,
      just_gestation_df_full
    ),
    union_all
  ) %>%
    select(-all_of("episode")) %>%
    group_by(person_id) %>%
    arrange(visit_date, .by_group = TRUE) %>%
    mutate(episode = row_number()) %>%
    ungroup() %>%
    # recalculate since I overwrote
    mutate(days_diff = !!days_diff_recalc_sql) %>%
    compute_table(connection = connection)
  
  # Debug: Check after union
  tryCatch({
    all_df_count <- all_df %>% safe_count()
    cat(sprintf("After union total episodes: %d\n", as.integer(all_df_count)))
  }, error = function(e) {
    cat("After union total episodes: Unable to count\n")
  })
  cat("=== END DEBUG ===\n\n")
  
  counts <- count(all_df,
    gestation_based = !is.na(gest_id),
    outcome_based = !is.na(visit_id)
  ) %>%
    safe_collect()
  
  # Debug: Check what's in the counts
  cat("\n=== DEBUG: Count Results ===\n")
  print(counts)
  cat("=== END DEBUG ===\n\n")
  
  # Helper function to safely get count for a specific combination
  # Note: The counts dataframe has character columns "1" and "0", not booleans
  get_count <- function(df, gest_val, outcome_val) {
    # Convert boolean values to character for comparison
    gest_str <- if(gest_val) "1" else "0"
    outcome_str <- if(outcome_val) "1" else "0"
    
    result <- df %>% 
      filter(gestation_based == gest_str, outcome_based == outcome_str) %>% 
      pull(n)
    if (length(result) == 0) 0L else as.integer(result)
  }
  
  cat("Total number of outcome-based episodes:",
    tally(calculate_start_df) %>% pull(n) %>% as.integer(),
    "Total number of gestation-based episodes:",
    tally(get_min_max_gestation_df) %>% pull(n) %>% as.integer(),
    "Total number of only outcome-based episodes after merging:",
    get_count(counts, FALSE, TRUE),
    "Total number of only gestation-based episodes after merging:",
    get_count(counts, TRUE, FALSE),
    "Total number of episodes with both after merging:",
    get_count(counts, TRUE, TRUE),
    sep = "\n"
  )
  return(all_df)
}

#' Clean episodes
#'
#' @param add_gestation_df Combined episodes
#'
#' @return Cleaned episodes
#' @export
clean_episodes <- function(add_gestation_df, buffer_days = 28, connection = NULL) {
  # Extract connection if not provided
  if (is.null(connection) && inherits(add_gestation_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- add_gestation_df$src$con
  }
  
  # FIXED: Collect and re-upload to avoid window function issues
  # This function uses row_number() which can cause issues with DatabaseConnector
  if (inherits(add_gestation_df, c("tbl_lazy", "tbl_sql"))) {
    temp_data <- add_gestation_df %>% safe_collect()
    add_gestation_df <- create_temp_table(temp_data, connection = connection)
  }
  
  # Clean up episodes by removing duplicate episodes and reclassifying outcome-based episodes
  # as gestation-based episodes if the outcome containing gestational info does not fall within
  # the term durations defined by Matcho et al.
  final_df <- add_gestation_df
  
  # remove any outcomes where the gestational age based on max_gest_date is over the max term duration defined by Matcho et al.
  over_max_df <- final_df %>%
    # it has both an outcome and a gestation but is over the max
    filter(!is.na(gest_id) & !is.na(visit_id) & is_under_max == 0) %>%
    mutate(
      removed_category = category,
      category = "PREG",
      visit_date = max_gest_date,
      removed_outcome = 1
    )
  
  # filter out these episodes from main table
  final_df <- final_df %>%
    filter(!(!is.na(gest_id) & !is.na(visit_id) & is_under_max == 0)) %>%
    mutate(
      removed_outcome = 0
    )
  
  cat("Total number of episodes over maximum term duration:",
    tally(over_max_df) %>% pull(n) %>% as.integer(),
    sep = "\n"
  )
  
  # join episodes with new values back to main table
  final_df <- union_all(final_df, over_max_df)
  
  ###### remove any outcomes where the gestational age based on max_gest_date is under the min term duration defined by Matcho et al. ######
  
  # filter to episodes with max_gest_date is under the min term duration and where the number of days between the visit_date and
  # max_gest_date is negative with buffer
  under_min_df <- final_df %>%
    filter(!is.na(gest_id) & !is.na(visit_id) & is_over_min == 0 & days_diff < -buffer_days) %>%
    mutate(
      removed_category = category,
      category = "PREG",
      visit_date = max_gest_date,
      removed_outcome = 1
    ) %>%
    compute_table(connection = connection)
  
  # filter out these episodes from main table
  final_df <- final_df %>%
    filter(!(!is.na(gest_id) & !is.na(visit_id) & is_over_min == 0 & days_diff < -buffer_days))
  
  cat("Total number of episodes under minimum term duration:",
    tally(under_min_df) %>% pull(n) %>% as.integer(),
    sep = "\n"
  )
  
  # join episodes with new values to main table
  final_df <- union_all(final_df, under_min_df)
  
  ###### remove any outcomes where the difference between max_gest_date in days is negative ######
  # filter to episodes with max_gest_date is after the outcome visit_date with buffer
  neg_days_df <- final_df %>%
    filter(!is.na(gest_id) & !is.na(visit_id) & days_diff < -buffer_days) %>%
    mutate(
      removed_category = category,
      category = "PREG",
      visit_date = max_gest_date,
      removed_outcome = 1
    )
  
  cat("Total number of episodes with negative number of days between outcome and max_gest_date:",
    tally(neg_days_df) %>% pull(n) %>% as.integer(),
    sep = "\n"
  )
  
  # Pre-compute SQL expressions for quality check columns
  gest_at_outcome_sql <- sql_translate("DATEDIFF(day, max_gest_start_date, visit_date)", connection)
  min_gest_date_diff_sql <- sql_translate("DATEDIFF(day, min_gest_date, min_gest_date_2)", connection)
  date_diff_max_end_sql <- sql_translate("DATEDIFF(day, end_gest_date, max_gest_date)", connection)
  
  # filter out these episodes from main table
  final_df <- final_df %>%
    filter(!(!is.na(gest_id) & !is.na(visit_id) & days_diff < -buffer_days)) %>%
    # join episodes with new values to main table
    union_all(neg_days_df) %>%
    ###### add columns for quality check ######
    # get new gestational age at visit date
    mutate(
      gest_at_outcome = !!gest_at_outcome_sql,
      min_gest_date_diff = !!min_gest_date_diff_sql,
      date_diff_max_end = !!date_diff_max_end_sql
    ) %>%
    # redo column for episode
    group_by(person_id) %>%
    arrange(visit_date, .by_group = TRUE) %>%
    mutate(episode = row_number()) %>%
    ungroup()
  
  return(final_df)
}

#' Remove overlapping episodes
#'
#' @param clean_episodes_df Cleaned episodes
#'
#' @return Episodes without overlaps
#' @export
remove_overlaps <- function(clean_episodes_df, connection = NULL) {
  # Extract connection if not provided
  if (is.null(connection) && inherits(clean_episodes_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- clean_episodes_df$src$con
  }
  
  # FIXED: Collect and re-upload to avoid window function issues
  # This function uses lag() which can cause issues with DatabaseConnector
  if (inherits(clean_episodes_df, c("tbl_lazy", "tbl_sql"))) {
    temp_data <- clean_episodes_df %>% safe_collect()
    clean_episodes_df <- create_temp_table(temp_data, connection = connection)
  }
  
  # Identify episodes that overlap and keep only the latter episode if the previous episode is PREG.
  # If the latter episode doesn't have gestational info, redefine the start date to be the
  # previous episode end date plus the retry period.
  df <- clean_episodes_df
  
  # Pre-compute SQL expressions for date differences
  prev_date_diff_gest_sql <- sql_translate("DATEDIFF(day, prev_date, max_gest_start_date)", connection)
  prev_date_diff_max_sql <- sql_translate("DATEDIFF(day, prev_date, max_start_date)", connection)
  
  df <- df %>%
    group_by(person_id) %>%
    arrange(visit_date, .by_group = TRUE) %>%
    # get previous date
    mutate(
      prev_date = lag(visit_date),
      # get previous category
      prev_category = lag(category),
      # get previous retry period
      prev_retry = lag(retry),
      # get previous gest_id
      prev_gest_id = lag(gest_id),
      # get difference in days between start date and previous visit date
      # us gestation-based date if available
      prev_date_diff = ifelse(!is.na(max_gest_start_date),
        !!prev_date_diff_gest_sql,
        !!prev_date_diff_max_sql
      ),
      # if the difference in days is negative, indicate overlap of episodes
      has_overlap = ifelse(prev_date_diff < 0, 1, 0)
    ) %>%
    ungroup()
  
  # overlapped episodes
  overlap_df <- df %>%
    filter(has_overlap == 1 & prev_category == "PREG")
  
  # get list of gest_ids to remove
  # Use anti_join instead of %in% to avoid issues with scientific notation
  gest_ids_to_remove <- overlap_df %>%
    distinct(prev_gest_id) %>%
    rename(gest_id = prev_gest_id) %>%
    mutate(category = "PREG")
  
  # Pre-compute SQL expressions for the final calculations
  # These were already computed above, reuse them
  # prev_date_diff_gest_sql and prev_date_diff_max_sql already defined
  
  # Pre-compute SQL for date add with retry period
  dateadd_retry_sql <- sql_translate("DATEADD(day, CAST(prev_retry AS INT), prev_date)", connection)
  
  # Pre-compute SQL for gest_at_outcome calculation
  gest_at_outcome_est_sql <- sql_translate("DATEDIFF(day, estimated_start_date, visit_date)", connection)
  
  # remove episodes that overlap using anti_join
  final_df <- df %>%
    anti_join(gest_ids_to_remove, by = c("gest_id", "category")) %>%
    group_by(person_id) %>%
    arrange(visit_date, .by_group = TRUE) %>%
    # recalculate
    # get previous date
    mutate(
      prev_date = lag(visit_date),
      # get previous category
      prev_category = lag(category),
      # get previous retry period
      prev_retry = lag(retry),
      # get previous gest_id
      prev_gest_id = lag(gest_id),
      # get difference in days between start date and previous visit date
      # us gestation-based date if available
      prev_date_diff = ifelse(!is.na(max_gest_start_date),
        !!prev_date_diff_gest_sql,
        !!prev_date_diff_max_sql
      ),
      # if the difference in days is negative, indicate overlap of episodes
      has_overlap = ifelse(prev_date_diff < 0, 1, 0),
      # get estimated start date
      estimated_start_date = case_when(
        # if there's an overlap and a retry period from the earlier episodes
        # and the last episode was not preg (or else would be in gest_id_list)
        # start date = last visit date + retry period
        has_overlap == 1 & !is.na(prev_retry) ~ !!dateadd_retry_sql,
        is.na(max_gest_start_date) ~ max_start_date,
        TRUE ~ max_gest_start_date
      )
    ) %>%
    # CRITICAL: Separate mutate to avoid column alias issues in SQL Server
    mutate(
      # get estimated gestational age in days at outcome_visit_date using estimated_start_date
      gest_at_outcome = !!gest_at_outcome_est_sql,
      # add column to check if gest_at_outcome is less than or equal to max_term, 1 indicates yes
      is_under_max = ifelse(gest_at_outcome <= max_term, 1, 0),
      # add column to check if gest_at_outcome is greater than or equal to min_term, 1 indicates yes
      is_over_min = ifelse(gest_at_outcome >= min_term, 1, 0)
    ) %>%
    # redo column for episode
    group_by(person_id) %>%
    arrange(visit_date, .by_group = TRUE) %>%
    mutate(
      episode = row_number(),
      # check that there are no more overlapping episodes
      prev_date = lag(visit_date),
      preg_gest_id = lag(gest_id)
    ) %>%
    ungroup()
  
  # Pre-compute SQL for final prev_date_diff calculations
  prev_date_diff_est_sql <- sql_translate("DATEDIFF(day, prev_date, estimated_start_date)", connection)
  prev_date_diff_max_start_sql <- sql_translate("DATEDIFF(day, prev_date, max_start_date)", connection)
  
  final_df <- final_df %>%
    mutate(
      # Recalculate prev_date_diff using the estimated_start_date
      # If estimated_start_date is NULL, use max_start_date as fallback
      prev_date_diff = case_when(
        !is.na(estimated_start_date) ~ !!prev_date_diff_est_sql,
        !is.na(max_start_date) ~ !!prev_date_diff_max_start_sql,
        TRUE ~ NA_real_
      ),
      # checked, there are no remaining
      has_overlap = ifelse(prev_date_diff < 0, 1, 0)
    )
  
  # if there are any remaining episodes with gestational age in weeks at
  # outcome date not within the term durations, reclassify as PREG
  temp_df <- final_df %>%
    filter(!is.na(max_gest_week) & !is.na(concept_name) & is_over_min == 0) %>%
    mutate(
      removed_category = category,
      category = "PREG",
      visit_date = max_gest_date,
      removed_outcome = 1
    )
  
  final_df <- final_df %>%
    filter(!(!is.na(max_gest_week) & !is.na(concept_name) & is_over_min == 0)) %>%
    union_all(temp_df) %>%
    compute_table(connection = connection)
  
  removed_count <- safe_count(final_df %>% filter(removed_outcome == 1))
  if (!is.na(removed_count)) {
    cat("Total number of episodes with removed outcome:", as.integer(removed_count), sep = "\n")
  } else {
    cat("Total number of episodes with removed outcome: Unable to count\n")
  }
  return(final_df)
}

#' Finalize episodes
#'
#' @param remove_overlaps_df Episodes without overlaps
#'
#' @return Final episodes
#' @export
final_episodes <- function(remove_overlaps_df) {
  # Keep subset of columns with episode start and end as well as category.
  # select columns and drop duplicates
  remove_overlaps_df %>%
    distinct(person_id, category, visit_date, estimated_start_date, episode)
}

#' Calculate episode length
#'
#' @param final_episodes_df Final episodes
#' @param gestation_visits_df Gestation visits
#'
#' @return Episodes with calculated length
#' @export
final_episodes_with_length <- function(final_episodes_df, gestation_visits_df, connection = NULL) {
  # Extract connection if not provided
  if (is.null(connection) && inherits(final_episodes_df, c("tbl_lazy", "tbl_sql"))) {
    connection <- final_episodes_df$src$con
  }
  # Find the first gestation record within an episode and calculate the episode
  # length based on the date of the first gestation record and the visit date.
  
  df <- final_episodes_df
  
  # select columns and rename column
  gest_df <- gestation_visits_df %>%
    select(person_id, gest_value, visit_date) %>%
    rename(gest_date = visit_date) %>%
    compute_table(connection = connection)
  
  merged <- gest_df %>%
    right_join(df,
      by = join_by(
        person_id,
        between(gest_date, estimated_start_date, visit_date)
      ),
      suffix = c(".x", ".y")
    ) %>%
    group_by(person_id, episode) %>%
    slice_min(gest_date, n = 1) %>%
    # keep max gest_value if two or more gestation records share same date
    ungroup() %>%
    compute_table(connection = connection) %>%
    group_by(person_id, episode) %>%
    slice_max(gest_value, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    distinct() %>%
    # flag episodes with gestational info
    mutate(gest_flag = ifelse(is.na(gest_date), NA, "yes"))
  
  # Pre-compute SQL expression for episode length calculation
  episode_length_sql <- sql_translate("DATEDIFF(day, gest_date, visit_date)", connection)
  
  # get episode length if there is a gestation record date, otherwise impute 1
  final_df <- merged %>%
    mutate(episode_length = if_else(!is.na(gest_date),
      !!episode_length_sql, 1
    ))
  
  # if an episode length is 0, change to 1
  final_df <- final_df %>%
    mutate(episode_length = if_else(episode_length == 0, 1, episode_length)) %>%
    select(-gest_value) %>%
    distinct()
  
  return(final_df)
}
