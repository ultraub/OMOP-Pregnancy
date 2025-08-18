#' Run PPS Algorithm V2 (Aligned with All of Us)
#'
#' Implements the Pregnancy Progression Signatures (PPS) algorithm
#' with retry logic and bridging record analysis as in All of Us.
#'
#' @param cohort_data List containing extracted cohort data
#' @param pps_concepts Data frame with PPS concepts and gestational timing
#'
#' @return Data frame of PPS-identified pregnancy episodes
#' @export
run_pps_algorithm <- function(cohort_data, pps_concepts) {
  
  # Get gestational timing data
  timing_data <- cohort_data$gestational_timing
  
  if (is.null(timing_data) || nrow(timing_data) == 0) {
    return(data.frame())
  }
  
  # Step 1: Prepare timing data with age filtering
  eligible_timing <- prepare_pps_data(timing_data, cohort_data$persons)
  
  if (nrow(eligible_timing) == 0) {
    return(data.frame())
  }
  
  # Step 2: Assign episodes with retry and bridging logic
  episodes_raw <- assign_pps_episodes(eligible_timing)
  
  # Step 3: Calculate episode boundaries
  episode_boundaries <- calculate_pps_boundaries(episodes_raw)
  
  # Step 4: Identify outcomes for each episode
  episodes_with_outcomes <- identify_pps_outcomes(
    episode_boundaries, 
    cohort_data,
    eligible_timing
  )
  
  # Step 5: Validate and finalize episodes
  final_episodes <- validate_pps_episodes(episodes_with_outcomes)
  
  # Add algorithm identifier
  final_episodes$algorithm_used <- "PPS"
  
  return(final_episodes)
}

#' Prepare PPS data with age filtering V2
#' @noRd
prepare_pps_data <- function(timing_data, persons_data) {
  
  # Calculate age at event for each record
  timing_with_age <- timing_data %>%
    left_join(
      persons_data %>% 
        select(person_id, year_of_birth, month_of_birth, day_of_birth),
      by = "person_id"
    ) %>%
    mutate(
      # Handle missing birth components
      month_of_birth = ifelse(is.na(month_of_birth), 1, month_of_birth),
      day_of_birth = ifelse(is.na(day_of_birth), 1, day_of_birth),
      
      # Calculate age at event
      birth_date = as.Date(paste(year_of_birth, month_of_birth, day_of_birth, sep = "-")),
      age_at_event = as.numeric(event_date - birth_date) / 365.25,
      
      # Filter to reproductive age (15-55)
      is_eligible = age_at_event >= 15 & age_at_event < 56
    ) %>%
    filter(is_eligible) %>%
    arrange(person_id, event_date)
  
  return(timing_with_age)
}

#' Assign PPS episodes with retry and bridging logic V2
#' @noRd
assign_pps_episodes <- function(timing_data) {
  
  # Process each person separately
  episodes <- timing_data %>%
    group_by(person_id) %>%
    group_modify(~assign_person_episodes(.x)) %>%
    ungroup()
  
  return(episodes)
}

#' Assign episodes for a single person with bridging logic
#' @noRd
assign_person_episodes <- function(personlist) {
  
  n_records <- nrow(personlist)
  
  # If only one record, it's episode 1
  if (n_records == 1) {
    personlist$person_episode_number <- 1
    return(personlist)
  }
  
  # Initialize episode assignment
  person_episodes <- numeric(n_records)
  person_episodes[1] <- 1
  current_episode <- 1
  
  # Iterate through records to assign episodes
  for (i in 2:n_records) {
    
    # Calculate time difference from previous record (in months)
    delta_t <- as.numeric(
      personlist$event_date[i] - personlist$event_date[i-1]
    ) / 30.44  # Average days per month
    
    # Check temporal consistency with retry logic
    agreement_t_c <- records_comparison(personlist, i)
    
    # Apply decision logic with 1-month retry period (from All of Us)
    if (!agreement_t_c && delta_t > 1) {
      # No agreement and > 1 month gap - start new episode
      current_episode <- current_episode + 1
    } else if (delta_t > 10) {
      # Gap > 10 months - always start new episode
      current_episode <- current_episode + 1
    }
    
    person_episodes[i] <- current_episode
  }
  
  personlist$person_episode_number <- person_episodes
  
  # Filter out episodes that are too long (>12 months)
  valid_episodes <- personlist %>%
    group_by(person_episode_number) %>%
    mutate(
      episode_start = as.Date(min(event_date)),
      episode_end = as.Date(max(event_date)),
      episode_length_months = as.numeric(episode_end - episode_start) / 30.44,
      is_valid = episode_length_months <= 12
    ) %>%
    filter(is_valid) %>%
    ungroup() %>%
    select(-episode_start, -episode_end, -episode_length_months, -is_valid)
  
  # Renumber episodes after filtering
  if (nrow(valid_episodes) > 0) {
    valid_episodes <- valid_episodes %>%
      mutate(
        person_episode_number = as.integer(factor(person_episode_number))
      )
  }
  
  return(valid_episodes)
}

#' Records comparison with bridging logic (from All of Us)
#' @noRd
records_comparison <- function(personlist, i) {
  
  # Check temporal consistency with previous records
  for (j in 1:(i - 1)) {
    # Obtain the difference in actual dates (in months)
    delta_t <- as.numeric(
      difftime(personlist$event_date[i], personlist$event_date[i - j], units = "days")
    ) / 30
    
    # Get expected month differences with 2-month leniency
    adjConceptMonths_MaxExpectedDelta <- personlist$max_month[i] - personlist$min_month[i - j] + 2
    adjConceptMonths_MinExpectedDelta <- personlist$min_month[i] - personlist$max_month[i - j] - 2
    
    # Check if actual falls within expected range
    agreement_t_c <- (adjConceptMonths_MaxExpectedDelta >= delta_t) & 
                     (delta_t >= adjConceptMonths_MinExpectedDelta)
    
    if (agreement_t_c == TRUE) {
      return(TRUE)  # Return early if agreement found
    }
  }
  
  # Check bridging records (surrounding record i)
  len_to_start <- i - 1
  len_to_end <- nrow(personlist) - i
  
  if (len_to_start == 0 || len_to_end == 0) {
    return(FALSE)  # No surrounding records
  }
  
  # Iterate through bridge records
  for (s in seq_len(len_to_start)) {
    for (e in seq_len(len_to_end)) {
      # Get time difference between bridge records
      bridge_delta_t <- as.numeric(
        difftime(personlist$event_date[i + e], personlist$event_date[i - s], units = "days")
      ) / 30
      
      # Get expected differences for bridge
      bridge_MaxExpectedDelta <- personlist$max_month[i + e] - personlist$min_month[i - s] + 2
      bridge_MinExpectedDelta <- personlist$min_month[i + e] - personlist$max_month[i - s] - 2
      
      # Check bridge agreement
      bridge_agreement <- (bridge_MaxExpectedDelta >= bridge_delta_t) &
                         (bridge_delta_t >= bridge_MinExpectedDelta)
      
      if (bridge_agreement == TRUE) {
        return(TRUE)
      }
    }
  }
  
  return(FALSE)
}

#' Calculate episode boundaries V2
#' @noRd
calculate_pps_boundaries <- function(episodes_raw) {
  
  boundaries <- episodes_raw %>%
    group_by(person_id, person_episode_number) %>%
    summarise(
      # Episode boundaries
      episode_min_date = as.Date(min(event_date)),
      episode_max_date = as.Date(max(event_date)),
      
      # Gestational timing info
      earliest_ga_min = min(min_month, na.rm = TRUE),
      latest_ga_max = max(max_month, na.rm = TRUE),
      
      # Concept counts
      n_GT_concepts = n_distinct(concept_id),
      n_records = n(),
      
      .groups = "drop"
    ) %>%
    mutate(
      # Clean up infinite values
      earliest_ga_min = ifelse(is.infinite(earliest_ga_min), NA_real_, earliest_ga_min),
      latest_ga_max = ifelse(is.infinite(latest_ga_max), NA_real_, latest_ga_max),
      
      # Calculate expected pregnancy end date
      # Based on last concept + remaining pregnancy time
      expected_end_date = case_when(
        !is.na(latest_ga_max) ~ episode_max_date + (10 - latest_ga_max) * 30,
        TRUE ~ episode_max_date + 60  # Default 2 months
      )
    )
  
  return(boundaries)
}

#' Identify outcomes for PPS episodes V2
#' @noRd
identify_pps_outcomes <- function(episode_boundaries, cohort_data, timing_data) {
  
  # Combine all outcome records
  outcome_records <- bind_rows(
    cohort_data$conditions,
    cohort_data$procedures,
    cohort_data$observations,
    cohort_data$measurements
  ) %>%
    filter(category %in% c("LB", "SB", "DELIV", "ECT", "AB", "SA")) %>%
    select(person_id, outcome_date = event_date, outcome_category = category)
  
  if (nrow(outcome_records) == 0) {
    # No outcomes found, episodes are PREG only
    return(episode_boundaries %>%
           mutate(
             outcome_category = "PREG",
             outcome_date = expected_end_date
           ))
  }
  
  # Calculate lookback and lookahead windows (All of Us logic)
  boundaries_with_windows <- episode_boundaries %>%
    group_by(person_id) %>%
    arrange(person_episode_number) %>%
    mutate(
      # Next episode start date
      next_episode_start = lead(episode_min_date),
      
      # Lookback window: 14 days before last concept
      lookback_date = episode_max_date - 14,
      
      # Lookahead window: minimum of next episode or expected end
      lookahead_date = pmin(
        coalesce(next_episode_start - 1, as.Date("2999-01-01")),
        expected_end_date,
        na.rm = TRUE
      )
    ) %>%
    ungroup()
  
  # Join outcomes within windows
  episodes_with_outcomes <- boundaries_with_windows %>%
    left_join(
      outcome_records,
      by = "person_id",
      relationship = "many-to-many"
    ) %>%
    filter(
      # Outcome must be within window
      outcome_date >= lookback_date,
      outcome_date <= lookahead_date
    ) %>%
    group_by(person_id, person_episode_number) %>%
    # Use Matcho hierarchy to select outcome
    arrange(
      factor(outcome_category, levels = c("LB", "SB", "DELIV", "ECT", "AB", "SA")),
      outcome_date
    ) %>%
    slice(1) %>%  # Take highest priority outcome
    ungroup()
  
  # Add episodes without outcomes
  episodes_no_outcome <- boundaries_with_windows %>%
    anti_join(
      episodes_with_outcomes,
      by = c("person_id", "person_episode_number")
    ) %>%
    mutate(
      outcome_category = "PREG",
      outcome_date = expected_end_date
    )
  
  # Combine
  all_episodes <- bind_rows(
    episodes_with_outcomes,
    episodes_no_outcome
  ) %>%
    select(-next_episode_start, -lookback_date, -lookahead_date) %>%
    arrange(person_id, person_episode_number)
  
  return(all_episodes)
}

#' Validate PPS episodes V2
#' @noRd
validate_pps_episodes <- function(episodes) {
  
  validated <- episodes %>%
    mutate(
      # Calculate estimated start date
      episode_start_date = as.Date(case_when(
        # Use gestational timing if available
        !is.na(earliest_ga_min) ~ as.Date(episode_min_date) - (earliest_ga_min * 30),
        # Otherwise assume start is 3 months before first concept
        TRUE ~ as.Date(episode_min_date) - 90
      )),
      
      # End date is outcome date
      episode_end_date = as.Date(outcome_date),
      
      # Calculate gestational age
      gestational_age_days = as.numeric(episode_end_date - episode_start_date)
    ) %>%
    filter(
      # Remove implausible episodes
      gestational_age_days > 0,
      gestational_age_days <= 320,  # ~45 weeks
      episode_start_date <= Sys.Date(),
      episode_end_date >= episode_start_date
    ) %>%
    # Renumber episodes
    group_by(person_id) %>%
    mutate(episode_number = row_number()) %>%
    ungroup() %>%
    select(
      person_id,
      episode_number,
      episode_start_date,
      episode_end_date,
      outcome_category,
      gestational_age_days,
      n_GT_concepts,
      n_records
    )
  
  return(validated)
}