#' PPS (Pregnancy Progression Signature) Algorithm
#'
#' Implementation of the PPS algorithm for pregnancy episode identification
#' using temporal sequence analysis. This code maintains maximum similarity 
#' to the original All of Us implementation.
#'
#' @name pps_algorithm
#' @import dplyr
#' @import lubridate
#' @import purrr
NULL

# SQL functions are now loaded through package NAMESPACE
# The sql_date_diff, sql_date_from_parts, sql_date_add, and sql_concat
# functions are exported from sql_functions.R and available when the package is loaded

#' Input gestational timing concepts from OMOP CDM tables
#'
#' @param condition_occurrence_tbl Condition occurrence table
#' @param procedure_occurrence_tbl Procedure occurrence table
#' @param observation_tbl Observation table
#' @param measurement_tbl Measurement table
#' @param visit_occurrence_tbl Visit occurrence table
#' @param PPS_concepts PPS concepts table
#'
#' @return Combined data frame with pregnancy-related concepts
#' @export
input_GT_concepts <- function(condition_occurrence_tbl, procedure_occurrence_tbl,
                              observation_tbl, measurement_tbl, visit_occurrence_tbl, PPS_concepts) {
  rename_cols <- function(top_concepts, df, start_date_col, id_col) {
    # Use !! for programmatic renaming with column names as strings
    is_lazy <- inherits(top_concepts, c("tbl_lazy", "tbl_sql"))
    
    if (is_lazy) {
      # Concepts already in database - join directly
      top_concepts_df <- df %>%
        rename("domain_concept_start_date" = !!start_date_col, 
               "domain_concept_id" = !!id_col) %>%
        inner_join(top_concepts, by = "domain_concept_id", suffix = c(".x", ".y")) %>%
        select(person_id, domain_concept_start_date, domain_concept_id) %>%
        distinct()
    } else {
      # Local concepts - copy to database for join
      top_concepts_df <- df %>%
        rename("domain_concept_start_date" = !!start_date_col, 
               "domain_concept_id" = !!id_col) %>%
        inner_join(top_concepts, by = "domain_concept_id", copy = TRUE, suffix = c(".x", ".y")) %>%
        select(person_id, domain_concept_start_date, domain_concept_id) %>%
        distinct()
    }
    return(top_concepts_df)
  }
  
  c_o <- rename_cols(PPS_concepts, condition_occurrence_tbl, "condition_start_date", "condition_concept_id")
  p_o <- rename_cols(PPS_concepts, procedure_occurrence_tbl, "procedure_date", "procedure_concept_id")
  o_df <- rename_cols(PPS_concepts, observation_tbl, "observation_date", "observation_concept_id")
  m_df <- rename_cols(PPS_concepts, measurement_tbl, "measurement_date", "measurement_concept_id")
  v_o <- rename_cols(PPS_concepts, visit_occurrence_tbl, "visit_start_date", "visit_concept_id")
  
  top_preg_related_concepts <- list(c_o, p_o, o_df, m_df, v_o) %>%
    reduce(union_all)
  
  return(top_preg_related_concepts)
}

#' Compare records for temporal consistency
#'
#' @param personlist Person's concept list
#' @param i Index of current record
#'
#' @return Logical indicating if records are consistent with pregnancy progression
#' @export
records_comparison <- function(personlist, i) {
  # for the below: t = time (actual), c = concept (expected)
  # first do the comparisons to the records PREVIOUS to record i
  
  # Iterate through the previous records
  for (j in 1:(i - 1)) {
    # Obtain the difference in actual dates of the consecutive patient records
    delta_t <- as.numeric(difftime(personlist$domain_concept_start_date[i], personlist$domain_concept_start_date[i - j], units = "days") / 30)
    # Obtain the max expected month difference based on clinician knowledge of the two concepts (allow two extra months for leniency)
    adjConceptMonths_MaxExpectedDelta <- personlist$max_month[i] - personlist$min_month[i - j] + 2
    # Obtain the min expected month difference based on clinician knowledge of the two concepts (allow two extra months for leniency)
    adjConceptMonths_MinExpectedDelta <- personlist$min_month[i] - personlist$max_month[i - j] - 2
    # Save a boolean indicating whether the actual date difference falls within the max and min expected date differences for the consecutive concepts
    agreement_t_c <- (adjConceptMonths_MaxExpectedDelta >= delta_t) & (delta_t >= adjConceptMonths_MinExpectedDelta)
    
    # If there is agreement between the concepts, update the return_agreement_t_c variable
    if (agreement_t_c == TRUE) {
      return(TRUE) # return early -- only needs to be true once
    }
  }
  # Next, do the comparisons to the records SURROUNDING record i
  len_to_start <- i - 1
  len_to_end <- nrow(personlist) - i
  bridge_len <- min(len_to_start, len_to_end)
  if (bridge_len == 0) {
    return(FALSE)
  } # no records surrounding
  
  # Iterate through the bridge records around record i, in case record i was an outlier
  for (s in seq_len(len_to_start)) {
    for (e in seq_len(len_to_end)) {
      # Obtain the time difference in months between the bridge records
      bridge_delta_t <- as.numeric(difftime(personlist$domain_concept_start_date[i + e],
        personlist$domain_concept_start_date[i - s],
        units = "days"
      ) / 30)
      # Obtain the max and min expected month differences based on clinician knowledge of the bridge concepts (allow two extra months for leniency)
      bridge_adjConceptMonths_MaxExpectedDelta <- personlist$max_month[i + e] - personlist$min_month[i - s] + 2
      bridge_adjConceptMonths_MinExpectedDelta <- personlist$min_month[i + e] - personlist$max_month[i - s] - 2
      # Check if there is agreement between the bridge concepts
      bridge_agreement_t_c <- (bridge_adjConceptMonths_MaxExpectedDelta >= bridge_delta_t) &
        (bridge_delta_t >= bridge_adjConceptMonths_MinExpectedDelta)
      # If there is agreement between the bridge concepts, update the return_agreement_t_c variable
      if (bridge_agreement_t_c == TRUE) {
        return(TRUE) # return early -- only needs to be true once
      }
    }
  }
  
  # Return the final agreement status between the concepts
  return(FALSE)
}

#' Assign episode numbers to pregnancy concepts
#'
#' @param personlist Person's concept list
#' @param ... Additional arguments
#'
#' @return Person list with episode numbers assigned
#' @export
assign_episodes <- function(personlist, ...) {
  if (nrow(personlist) == 1) {
    personlist$person_episode_number <- 1
    return(personlist)
  }
  
  # Filter to plausible pregnancy timelines and concept month sequences, and number by episode to get person_episode_number
  
  # Initialize variables for episode numbering and storing episode information
  # # Treat the first record as belonging to the first episode
  person_episode_number <- 1
  person_episodes <- 1
  person_episode_chr <- "1"
  person_episode_dates <- list()
  
  # Add the date of the current record to the corresponding episode in person_episode_dates
  person_episode_dates[[person_episode_chr]] <- c(personlist$domain_concept_start_date[1])
  
  for (i in 2:nrow(personlist)) {
    # Calculate the time difference in months between the current record and the previous record
    delta_t <- as.numeric(difftime(personlist$domain_concept_start_date[i],
      personlist$domain_concept_start_date[i - 1],
      units = "days"
    ) / 30)
    
    # Perform the checks to determine whether this is a continuation of an episode or the start of a new episode
    agreement_t_c <- records_comparison(personlist, i)
    
    # If there is no agreement between the concepts and the time difference is greater than 2 months,
    # change to 1 month, ie retry period
    # increment the person_episode_number to indicate a new episode
    if ((!agreement_t_c) && (delta_t > 1)) {
      person_episode_number <- person_episode_number + 1
    } else if (delta_t > 10) {
      # If the time difference is greater than 10 months, increment the person_episode_number to indicate a new episode
      person_episode_number <- person_episode_number + 1
    }
    
    # Append the person_episode_number to the person_episodes list
    person_episodes <- c(person_episodes, person_episode_number)
    
    person_episode_chr <- as.character(person_episode_number)
    
    # Check if the person_episode_number is already in the person_episode_dates list
    if (!(person_episode_chr %in% names(person_episode_dates))) {
      person_episode_dates[[person_episode_chr]] <- personlist$domain_concept_start_date[i]
    } else {
      # Add the date of the current record to the corresponding episode in person_episode_dates
      person_episode_dates[[person_episode_chr]] <- c(
        person_episode_dates[[person_episode_chr]],
        personlist$domain_concept_start_date[i]
      )
    }
  }
  
  # - Check that all the episodes are < 12 mo in length (the 9-10 mo of pregnancy plus the few months of delivery concept ramblings).
  # In the case that any have to be removed, loop through the episodes of the patient again and renumber the remaining episodes
  episodes_to_remove <- c()
  for (episode in names(person_episode_dates)) {
    len_of_episode <- as.numeric(difftime(person_episode_dates[[episode]][length(person_episode_dates[[episode]])],
      person_episode_dates[[episode]][1],
      units = "days"
    ) / 30)
    if (len_of_episode > 12) {
      episodes_to_remove <- c(episodes_to_remove, episode)
    }
  }
  new_person_episodes <- ifelse(as.character(person_episodes) %in% episodes_to_remove, 0, person_episodes)
  numUniqueNonZero <- sum(unique(new_person_episodes) != 0)
  nonZeroNewList <- 1:numUniqueNonZero
  nonZeroOrigList <- unique(new_person_episodes)[unique(new_person_episodes) != 0]
  new_person_episodes <- nonZeroNewList[match(new_person_episodes, nonZeroOrigList)]
  personlist$person_episode_number <- new_person_episodes
  
  return(personlist)
}

#' Get PPS episodes from concepts
#'
#' @param input_GT_concepts_df Gestational timing concepts
#' @param PPS_concepts PPS concepts table
#' @param person_tbl Person table
#' @param config Configuration list (optional)
#' @param connection Database connection (optional, for dialect detection)
#'
#' @return Data frame with PPS episodes
#' @export
get_PPS_episodes <- function(input_GT_concepts_df, PPS_concepts, person_tbl, config = NULL, connection = NULL) {
  # Extract connection if not provided
  if (is.null(connection) && inherits(person_tbl, c("tbl_lazy", "tbl_sql"))) {
    connection <- person_tbl$src$con
  }
  
  # Get configuration parameters or use defaults
  if (is.null(config)) {
    male_concept_id <- 8507  # Standard OMOP male concept
    min_age <- 15
    max_age <- 56
  } else {
    male_concept_id <- ifelse(
      !is.null(config$concepts$gender_concepts$active_male_id),
      config$concepts$gender_concepts$active_male_id,
      ifelse(config$mode == "allofus", 45880669, 8507)
    )
    min_age <- ifelse(!is.null(config$algorithm$hip$min_age), 
                      config$algorithm$hip$min_age, 15)
    max_age <- ifelse(!is.null(config$algorithm$hip$max_age), 
                      config$algorithm$hip$max_age, 56)
  }
  
  # Materialize intermediate result to avoid aliasing issues
  person_subset <- person_tbl %>%
    select(person_id, gender_concept_id, year_of_birth, day_of_birth, month_of_birth)
  
  # First, filter and join with PPS concepts, then compute to temp table
  patients_with_concepts <- filter(input_GT_concepts_df, !is.na(domain_concept_start_date)) %>%
    left_join(PPS_concepts, by = "domain_concept_id", copy = TRUE, suffix = c(".x", ".y"))
  
  # Check if we're in a database context and compute if needed
  if (inherits(patients_with_concepts, "tbl_sql")) {
    patients_with_concepts <- compute(patients_with_concepts)
  }
  
  patients_with_preg_concepts <- patients_with_concepts %>%
    inner_join(person_subset, by = "person_id", suffix = c(".x", ".y")) %>%
    mutate(
      day_of_birth = if_else(is.na(day_of_birth), 1, day_of_birth),
      month_of_birth = if_else(is.na(month_of_birth), 1, month_of_birth)
    ) %>%
    mutate(
      # Use direct SQL for date construction to avoid function call issues
      date_of_birth = sql("DATEFROMPARTS(year_of_birth, month_of_birth, day_of_birth)")
    ) %>%
    mutate(
      # Use direct SQL for date difference to avoid function call issues
      date_diff = sql("DATEDIFF(day, date_of_birth, domain_concept_start_date)"),
      age = date_diff / 365
    ) %>%
    # women of reproductive age
    filter(
      gender_concept_id != male_concept_id,
      age >= min_age,
      age < max_age
    ) %>%
    select(-year_of_birth, -month_of_birth, -day_of_birth, -date_diff, -gender_concept_id)
  
  # OBTAIN ALL RELEVANT INPUT PATIENTS AND SAVE GT INFORMATION PER CONCEPT TO A LOOKUP DICTIONARY
  # First we save the women that have gestational timing concepts, and save the gestational timing information for each concept.
  # We add the concepts and their gestational timing months ([min,max]) during pregnancy to a dictionary (hash) in
  # concept key: month value list format e.g. {2211756: [4,8], 2101830: [2,2]...}
  
  
  # SAVE EXPECTED GESTATIONAL TIMING MONTH INFORMATION FOR EACH OF THE PATIENT RECORDS
  # Looping over each person with pregnancy concepts, order their concepts by date of each record, and for each concept ID in order, loop through the
  # keys of the dictionary and compare to the concept ID, if there's a match, save the month value(s) to a list for the record date. You'll end up with
  # record date: list of matching months, save this to a new dictionary with record dates as the keys. Where no match occurs, put NA
  #   person_dates_dict <- split(person_dates_df$list_col, person_dates_df$person_id)
  
  # The database will handle pagination automatically
  person_dates_df <- collect(patients_with_preg_concepts) %>%
    group_by(person_id) %>%
    arrange(domain_concept_start_date)
  
  res <- person_dates_df %>%
    group_modify(assign_episodes)
  
  return(res)
}

#' Get episode maximum and minimum dates
#'
#' @param get_PPS_episodes_df PPS episodes data frame
#'
#' @return Data frame with episode date ranges
#' @export
get_episode_max_min_dates <- function(get_PPS_episodes_df) {
  df <- get_PPS_episodes_df %>%
    filter(!is.na(person_episode_number)) %>%
    group_by(person_id, person_episode_number) %>%
    summarise(
      # first time pregnancy concept appears
      episode_min_date = min(domain_concept_start_date),
      # last time a pregnancy concept appears
      episode_max_date = max(domain_concept_start_date),
      # add the number of unique gestational timing concepts per episode
      n_GT_concepts = n_distinct(domain_concept_id)
    ) %>%
    ungroup() %>%
    mutate(
      # Use SQL DATEADD for date arithmetic after grouping
      episode_max_date_plus_two_months = sql("DATEADD(month, 2, episode_max_date)")
    ) %>%
    group_by(person_id, person_episode_number) %>%
    ungroup()
  
  return(df)
}
