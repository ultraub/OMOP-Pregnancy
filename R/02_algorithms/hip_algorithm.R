#' Run HIP Algorithm V2 (Aligned with All of Us)
#'
#' Implements the Hierarchical Identification of Pregnancy (HIP) algorithm
#' with outcome-specific spacing as in the All of Us implementation.
#'
#' @param cohort_data List containing extracted cohort data
#' @param matcho_limits Data frame with outcome hierarchy and term limits
#' @param matcho_outcome_limits Data frame with min_days between outcome pairs
#'
#' @return Data frame of HIP-identified pregnancy episodes
#' @export
run_hip_algorithm <- function(cohort_data, matcho_limits, matcho_outcome_limits) {
  
  # Combine all domain data into single frame
  all_records <- bind_rows(
    cohort_data$conditions,
    cohort_data$procedures,
    cohort_data$observations,
    cohort_data$measurements
  ) %>%
    filter(!is.na(event_date)) %>%
    arrange(person_id, event_date)
  
  # Step 1: Get initial pregnant cohort (all pregnancy-related visits)
  initial_cohort <- all_records %>%
    filter(category %in% c("LB", "SB", "DELIV", "ECT", "AB", "SA", "PREG"))
  
  if (nrow(initial_cohort) == 0) {
    return(data.frame())
  }
  
  # Step 2: Process outcomes hierarchically following All of Us approach
  # Start with Live Births
  lb_episodes <- process_outcome_category(
    initial_cohort, 
    categories = "LB",
    matcho_outcome_limits
  )
  
  # Add Stillbirths
  sb_episodes <- process_outcome_category(
    initial_cohort,
    categories = "SB", 
    matcho_outcome_limits
  )
  
  # Combine LB and SB, checking spacing
  lb_sb_episodes <- add_stillbirth_episodes(
    lb_episodes,
    sb_episodes,
    matcho_outcome_limits
  )
  
  # Add Ectopic pregnancies
  ect_episodes <- process_outcome_category(
    initial_cohort,
    categories = "ECT",
    matcho_outcome_limits
  )
  
  lb_sb_ect_episodes <- add_ectopic_episodes(
    lb_sb_episodes,
    ect_episodes,
    matcho_outcome_limits
  )
  
  # Add Abortions (AB and SA together)
  ab_sa_episodes <- process_outcome_category(
    initial_cohort,
    categories = c("AB", "SA"),
    matcho_outcome_limits
  )
  
  all_outcome_episodes <- add_abortion_episodes(
    lb_sb_ect_episodes,
    ab_sa_episodes,
    matcho_outcome_limits
  )
  
  # Add Delivery episodes
  deliv_episodes <- process_outcome_category(
    initial_cohort,
    categories = "DELIV",
    matcho_outcome_limits
  )
  
  final_episodes <- add_delivery_episodes(
    all_outcome_episodes,
    deliv_episodes,
    matcho_outcome_limits
  )
  
  # Step 3: Add gestational age information
  episodes_with_gest <- add_gestational_age_info(final_episodes, all_records)
  
  # Step 4: Calculate estimated start dates
  episodes_with_dates <- calculate_hip_start_dates(episodes_with_gest, matcho_limits)
  
  # Step 5: Validate and finalize
  validated_episodes <- validate_hip_episodes(episodes_with_dates)
  
  # Add algorithm identifier
  validated_episodes$algorithm_used <- "HIP"
  
  return(validated_episodes)
}

#' Process a specific outcome category
#' @noRd
process_outcome_category <- function(initial_cohort, categories, matcho_outcome_limits) {
  
  # Filter to the specific categories
  category_records <- initial_cohort %>%
    filter(category %in% categories)
  
  if (nrow(category_records) == 0) {
    return(data.frame())
  }
  
  # Get minimum days for same-category spacing
  if (length(categories) == 1) {
    min_days <- matcho_outcome_limits %>%
      filter(
        first_preg_category == categories[1],
        outcome_preg_category == categories[1]
      ) %>%
      pull(min_days)
  } else {
    # For AB/SA, use the minimum spacing
    min_days <- 56  # Default for AB/SA combinations
  }
  
  if (length(min_days) == 0) {
    min_days <- 180  # Default fallback
  }
  
  # Group by person and identify episodes based on spacing
  episodes <- category_records %>%
    group_by(person_id) %>%
    arrange(event_date) %>%
    mutate(
      days_since_last = as.numeric(event_date - lag(event_date)),
      new_episode = is.na(days_since_last) | days_since_last >= min_days,
      episode_id = cumsum(new_episode)
    ) %>%
    group_by(person_id, episode_id) %>%
    summarise(
      outcome_date = max(safe_as_date(event_date)),
      outcome_category = first(category),
      n_visits = n(),
      .groups = "drop"
    ) %>%
    ungroup() %>%
    mutate(
      outcome_date = safe_as_date(outcome_date)
    )
  
  return(episodes)
}

#' Add stillbirth episodes with proper spacing
#' @noRd
add_stillbirth_episodes <- function(lb_episodes, sb_episodes, matcho_outcome_limits) {
  
  if (nrow(sb_episodes) == 0) {
    return(lb_episodes)
  }
  
  if (nrow(lb_episodes) == 0) {
    return(sb_episodes)
  }
  
  # Get minimum days between LB and SB
  before_min <- matcho_outcome_limits %>%
    filter(first_preg_category == "LB", outcome_preg_category == "SB") %>%
    pull(min_days)
  
  after_min <- matcho_outcome_limits %>%
    filter(first_preg_category == "SB", outcome_preg_category == "LB") %>%
    pull(min_days)
  
  # Combine and check spacing
  combined <- bind_rows(lb_episodes, sb_episodes) %>%
    group_by(person_id) %>%
    arrange(outcome_date) %>%
    mutate(
      prev_category = lag(outcome_category),
      next_category = lead(outcome_category),
      days_after = as.numeric(safe_as_date(outcome_date) - safe_as_date(lag(outcome_date))),
      days_before = as.numeric(safe_as_date(lead(outcome_date)) - safe_as_date(outcome_date))
    )
  
  # Filter SB episodes that don't meet spacing requirements
  valid_sb <- combined %>%
    filter(
      outcome_category == "SB",
      # Keep if isolated or properly spaced
      (is.na(prev_category) & is.na(next_category)) |
      (prev_category != "LB" | days_after >= before_min) |
      (next_category != "LB" | days_before >= after_min)
    )
  
  # Combine valid SB with all LB
  result <- bind_rows(
    combined %>% filter(outcome_category == "LB"),
    valid_sb
  ) %>%
    select(-prev_category, -next_category, -days_after, -days_before) %>%
    arrange(person_id, outcome_date)
  
  return(result)
}

#' Add ectopic episodes with proper spacing
#' @noRd
add_ectopic_episodes <- function(lb_sb_episodes, ect_episodes, matcho_outcome_limits) {
  
  if (nrow(ect_episodes) == 0) {
    return(lb_sb_episodes)
  }
  
  # Get minimum days for ECT spacing
  ect_after_lb <- matcho_outcome_limits %>%
    filter(first_preg_category == "LB", outcome_preg_category == "ECT") %>%
    pull(min_days)
  
  ect_after_sb <- matcho_outcome_limits %>%
    filter(first_preg_category == "SB", outcome_preg_category == "ECT") %>%
    pull(min_days)
  
  lb_after_ect <- matcho_outcome_limits %>%
    filter(first_preg_category == "ECT", outcome_preg_category == "LB") %>%
    pull(min_days)
  
  sb_after_ect <- matcho_outcome_limits %>%
    filter(first_preg_category == "ECT", outcome_preg_category == "SB") %>%
    pull(min_days)
  
  # Use minimum of the constraints
  before_min <- min(ect_after_lb, ect_after_sb, na.rm = TRUE)
  after_min <- min(lb_after_ect, sb_after_ect, na.rm = TRUE)
  
  # Apply spacing logic similar to stillbirths
  combined <- bind_rows(lb_sb_episodes, ect_episodes) %>%
    group_by(person_id) %>%
    arrange(outcome_date) %>%
    mutate(
      prev_category = lag(outcome_category),
      next_category = lead(outcome_category),
      days_after = as.numeric(safe_as_date(outcome_date) - safe_as_date(lag(outcome_date))),
      days_before = as.numeric(safe_as_date(lead(outcome_date)) - safe_as_date(outcome_date))
    )
  
  # Filter ECT episodes that meet spacing requirements
  valid_ect <- combined %>%
    filter(
      outcome_category == "ECT",
      (is.na(prev_category) & is.na(next_category)) |
      (is.na(prev_category) | days_after >= before_min) |
      (is.na(next_category) | days_before >= after_min)
    )
  
  # Combine valid ECT with previous episodes
  result <- bind_rows(
    combined %>% filter(outcome_category != "ECT"),
    valid_ect
  ) %>%
    select(-prev_category, -next_category, -days_after, -days_before) %>%
    arrange(person_id, outcome_date)
  
  return(result)
}

#' Add abortion episodes with proper spacing
#' @noRd
add_abortion_episodes <- function(prev_episodes, ab_sa_episodes, matcho_outcome_limits) {
  
  if (nrow(ab_sa_episodes) == 0) {
    return(prev_episodes)
  }
  
  # Get all relevant spacing constraints for AB/SA
  spacing_rules <- matcho_outcome_limits %>%
    filter(
      outcome_preg_category %in% c("AB", "SA") | 
      first_preg_category %in% c("AB", "SA")
    )
  
  # Apply similar spacing logic
  combined <- bind_rows(prev_episodes, ab_sa_episodes) %>%
    group_by(person_id) %>%
    arrange(outcome_date)
  
  # For simplicity, use a general minimum spacing of 56 days for abortions
  min_spacing <- 56
  
  valid_ab_sa <- combined %>%
    mutate(
      days_from_prev = as.numeric(outcome_date - lag(outcome_date))
    ) %>%
    filter(
      outcome_category %in% c("AB", "SA"),
      is.na(days_from_prev) | days_from_prev >= min_spacing
    )
  
  result <- bind_rows(
    combined %>% filter(!outcome_category %in% c("AB", "SA")),
    valid_ab_sa
  ) %>%
    select(-any_of("days_from_prev")) %>%
    arrange(person_id, outcome_date)
  
  return(result)
}

#' Add delivery episodes with proper spacing
#' @noRd
add_delivery_episodes <- function(prev_episodes, deliv_episodes, matcho_outcome_limits) {
  
  if (nrow(deliv_episodes) == 0) {
    return(prev_episodes)
  }
  
  # Delivery episodes are added last with appropriate spacing
  # Similar logic to other outcome additions
  combined <- bind_rows(prev_episodes, deliv_episodes) %>%
    group_by(person_id) %>%
    arrange(outcome_date)
  
  # Use 168 days as default minimum for delivery spacing
  min_spacing <- 168
  
  valid_deliv <- combined %>%
    mutate(
      days_from_prev = as.numeric(outcome_date - lag(outcome_date))
    ) %>%
    filter(
      outcome_category == "DELIV",
      is.na(days_from_prev) | days_from_prev >= min_spacing
    )
  
  result <- bind_rows(
    combined %>% filter(outcome_category != "DELIV"),
    valid_deliv
  ) %>%
    select(-any_of("days_from_prev")) %>%
    arrange(person_id, outcome_date) %>%
    ungroup()
  
  # Renumber episodes
  result <- result %>%
    group_by(person_id) %>%
    mutate(episode_number = row_number()) %>%
    ungroup()
  
  return(result)
}

#' Add gestational age information
#' @noRd
add_gestational_age_info <- function(episodes, all_records) {
  
  # Get gestational age records
  gest_records <- all_records %>%
    filter(
      !is.na(gest_value) | 
      category == "GEST" |
      grepl("gestation", concept_name, ignore.case = TRUE)
    )
  
  if (nrow(gest_records) == 0) {
    episodes$has_gestational_info <- FALSE
    episodes$gestational_weeks <- NA_real_
    return(episodes)
  }
  
  # Join gestational records to episodes within reasonable window
  episodes_with_gest <- episodes %>%
    left_join(
      gest_records %>%
        select(person_id, gest_date = event_date, gest_value, value_as_number),
      by = "person_id",
      relationship = "many-to-many"
    ) %>%
    filter(
      # Gestational records within pregnancy window
      gest_date >= outcome_date - 280,
      gest_date <= outcome_date
    ) %>%
    group_by(person_id, episode_number, outcome_date, outcome_category) %>%
    summarise(
      gestational_weeks = max(coalesce(gest_value, value_as_number), na.rm = TRUE),
      n_gest_records = n(),
      .groups = "drop"
    )
  
  # Merge back with episodes
  result <- episodes %>%
    left_join(
      episodes_with_gest %>%
        select(person_id, episode_number, gestational_weeks, n_gest_records),
      by = c("person_id", "episode_number")
    ) %>%
    mutate(
      has_gestational_info = !is.na(gestational_weeks),
      gestational_weeks = ifelse(is.infinite(gestational_weeks), NA_real_, gestational_weeks)
    )
  
  return(result)
}

#' Calculate HIP start dates V2
#' @noRd
calculate_hip_start_dates <- function(episodes, matcho_limits) {
  
  # Join with term limits
  episodes_with_terms <- episodes %>%
    left_join(
      matcho_limits %>% 
        select(category, min_term, max_term),
      by = c("outcome_category" = "category")
    )
  
  # Calculate start dates
  result <- episodes_with_terms %>%
    mutate(
      # If we have gestational info, use it
      gest_based_start = case_when(
        has_gestational_info & !is.na(gestational_weeks) ~ 
          safe_as_date(outcome_date) - (gestational_weeks * 7),
        TRUE ~ as.Date(NA)
      ),
      
      # Otherwise use term limits
      term_based_start = case_when(
        !is.na(max_term) ~ safe_as_date(outcome_date) - max_term,
        TRUE ~ safe_as_date(outcome_date) - 280  # Default max pregnancy
      ),
      
      # Choose the best estimate
      episode_start_date = safe_as_date(coalesce(gest_based_start, term_based_start)),
      episode_end_date = safe_as_date(outcome_date),
      
      # Calculate gestational age at outcome
      gestational_age_days = as.numeric(episode_end_date - episode_start_date)
    ) %>%
    select(
      person_id,
      episode_number,
      episode_start_date,
      episode_end_date,
      outcome_category,
      gestational_age_days,
      has_gestational_info
    )
  
  return(result)
}

#' Validate HIP episodes V2
#' @noRd
validate_hip_episodes <- function(episodes) {
  
  validated <- episodes %>%
    filter(
      # Remove implausible gestational ages
      gestational_age_days >= 0,
      gestational_age_days <= 320,  # ~45 weeks
      
      # Remove episodes that start in the future
      episode_start_date <= Sys.Date(),
      
      # Remove episodes with end before start
      episode_end_date >= episode_start_date
    ) %>%
    group_by(person_id) %>%
    arrange(episode_start_date) %>%
    mutate(
      # Check for overlapping episodes after validation
      prev_end_date = lag(episode_end_date),
      overlap_days = as.numeric(pmax(0, prev_end_date - episode_start_date + 1)),
      
      # Adjust start date if overlapping
      adjusted_start = case_when(
        !is.na(overlap_days) & overlap_days > 0 ~ safe_as_date(prev_end_date + 1),
        TRUE ~ safe_as_date(episode_start_date)
      ),
      
      # Recalculate gestational age
      gestational_age_days = as.numeric(episode_end_date - adjusted_start)
    ) %>%
    filter(
      # Remove episodes that become invalid after adjustment
      gestational_age_days > 0
    ) %>%
    ungroup() %>%
    mutate(
      episode_start_date = adjusted_start
    ) %>%
    select(-prev_end_date, -overlap_days, -adjusted_start, -has_gestational_info)
  
  return(validated)
}