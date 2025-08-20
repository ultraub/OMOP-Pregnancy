#' Merge HIP and PPS Episodes V2 (Aligned with All of Us)
#'
#' Merges pregnancy episodes with proper lookback/lookahead windows
#' as implemented in the All of Us algorithm.
#'
#' @param hip_episodes Data frame of HIP-identified episodes
#' @param pps_episodes Data frame of PPS-identified episodes
#' @param cohort_data List containing extracted cohort data
#'
#' @return Data frame of merged pregnancy episodes
#' @export
merge_pregnancy_episodes <- function(hip_episodes, pps_episodes, cohort_data = NULL) {
  
  # Handle empty inputs
  if (is.null(hip_episodes) || nrow(hip_episodes) == 0) {
    if (is.null(pps_episodes) || nrow(pps_episodes) == 0) {
      return(data.frame())
    }
    return(prepare_final_episodes(pps_episodes))
  }
  
  if (is.null(pps_episodes) || nrow(pps_episodes) == 0) {
    return(prepare_final_episodes(hip_episodes))
  }
  
  # Step 1: Prepare episodes with lookback/lookahead windows
  hip_with_windows <- add_episode_windows(hip_episodes, "HIP")
  pps_with_windows <- add_episode_windows(pps_episodes, "PPS")
  
  # Step 2: Identify overlapping episodes with window logic
  overlaps <- identify_episode_overlaps(hip_with_windows, pps_with_windows)
  
  # Step 3: Resolve overlaps using Matcho hierarchy
  resolved_episodes <- resolve_episode_overlaps(
    hip_with_windows,
    pps_with_windows,
    overlaps
  )
  
  # Step 4: Add any outcomes found in windows if cohort_data provided
  if (!is.null(cohort_data)) {
    resolved_episodes <- add_window_outcomes(resolved_episodes, cohort_data)
  }
  
  # Step 5: Finalize and renumber episodes
  final_episodes <- finalize_merged_episodes(resolved_episodes)
  
  return(final_episodes)
}

#' Add episode windows for merging
#' @noRd
add_episode_windows <- function(episodes, source) {
  
  # Check if gestational_age_days column exists
  has_gest_days <- "gestational_age_days" %in% names(episodes)
  
  episodes_with_windows <- episodes %>%
    mutate(
      algorithm_source = source,
      
      # Add lookback window (14 days before episode end)
      lookback_date = episode_end_date - 14,
      
      # Calculate expected end based on gestational info
      expected_end = if (has_gest_days) {
        case_when(
          # If we have gestational age value, use it
          !is.na(gestational_age_days) ~ 
            episode_start_date + gestational_age_days + DAYS_PER_MONTH,
          # Otherwise use outcome-based estimate
          outcome_category %in% c("LB", "SB", "DELIV") ~ episode_end_date + DAYS_PER_MONTH,
          outcome_category %in% c("ECT", "AB", "SA") ~ episode_end_date + 60,
          TRUE ~ episode_end_date + 90
        )
      } else {
        case_when(
          # Use outcome-based estimate when no gestational_age_days column
          outcome_category %in% c("LB", "SB", "DELIV") ~ episode_end_date + DAYS_PER_MONTH,
          outcome_category %in% c("ECT", "AB", "SA") ~ episode_end_date + 60,
          TRUE ~ episode_end_date + 90
        )
      }
    ) %>%
    group_by(person_id) %>%
    arrange(episode_start_date) %>%
    mutate(
      # Get next episode start for lookahead calculation
      next_episode_start = lead(episode_start_date),
      
      # Lookahead window: minimum of next episode or expected end
      lookahead_date = pmin(
        coalesce(next_episode_start - 1, as.Date("2999-01-01")),
        expected_end,
        na.rm = TRUE
      ),
      
      # Create expanded window for overlap detection
      window_start = as.Date(pmin(episode_start_date, lookback_date)),
      window_end = as.Date(pmax(episode_end_date, lookahead_date))
    ) %>%
    ungroup()
  
  return(episodes_with_windows)
}

#' Identify overlapping episodes with window logic
#' @noRd
identify_episode_overlaps <- function(hip_episodes, pps_episodes) {
  
  # Check if gestational_age_days exists
  has_hip_gest <- "gestational_age_days" %in% names(hip_episodes)
  has_pps_gest <- "gestational_age_days" %in% names(pps_episodes)
  
  # Build base select for HIP
  hip_select <- hip_episodes %>%
    select(
      person_id, 
      hip_episode_num = episode_number,
      hip_start = episode_start_date,
      hip_end = episode_end_date,
      hip_window_start = window_start,
      hip_window_end = window_end,
      hip_outcome = outcome_category
    )
  
  # Add gestational age if it exists
  if (has_hip_gest) {
    hip_select <- hip_select %>%
      mutate(hip_gest_days = hip_episodes$gestational_age_days)
  } else {
    hip_select <- hip_select %>%
      mutate(hip_gest_days = NA_real_)
  }
  
  # Build base select for PPS
  pps_select <- pps_episodes %>%
    select(
      person_id,
      pps_episode_num = episode_number,
      pps_start = episode_start_date,
      pps_end = episode_end_date,
      pps_window_start = window_start,
      pps_window_end = window_end,
      pps_outcome = outcome_category
    )
  
  # Add gestational age if it exists
  if (has_pps_gest) {
    pps_select <- pps_select %>%
      mutate(pps_gest_days = pps_episodes$gestational_age_days)
  } else {
    pps_select <- pps_select %>%
      mutate(pps_gest_days = NA_real_)
  }
  
  # Find overlaps using expanded windows
  overlaps <- hip_select %>%
    inner_join(
      pps_select,
      by = "person_id",
      relationship = "many-to-many"
    ) %>%
    filter(
      # Episodes overlap if their windows intersect
      hip_window_start <= pps_window_end & 
      hip_window_end >= pps_window_start
    ) %>%
    mutate(
      # Calculate overlap metrics
      core_overlap_start = as.Date(pmax(hip_start, pps_start)),
      core_overlap_end = as.Date(pmin(hip_end, pps_end)),
      core_overlap_days = pmax(0, as.numeric(core_overlap_end - core_overlap_start + 1)),
      
      # Calculate window overlap
      window_overlap_start = as.Date(pmax(hip_window_start, pps_window_start)),
      window_overlap_end = as.Date(pmin(hip_window_end, pps_window_end)),
      window_overlap_days = as.numeric(window_overlap_end - window_overlap_start + 1),
      
      # Calculate proportion of overlap
      hip_duration = as.numeric(hip_end - hip_start + 1),
      pps_duration = as.numeric(pps_end - pps_start + 1),
      hip_overlap_pct = core_overlap_days / hip_duration,
      pps_overlap_pct = core_overlap_days / pps_duration,
      
      # Determine overlap type
      overlap_type = case_when(
        core_overlap_days > 0 ~ "core",
        window_overlap_days > 0 ~ "window",
        TRUE ~ "none"
      )
    )
  
  return(overlaps)
}

#' Resolve overlapping episodes using Matcho hierarchy
#' @noRd
resolve_episode_overlaps <- function(hip_episodes, pps_episodes, overlaps) {
  
  if (nrow(overlaps) == 0) {
    # No overlaps, return all episodes
    return(bind_rows(
      hip_episodes %>% mutate(merge_status = "HIP_only"),
      pps_episodes %>% mutate(merge_status = "PPS_only")
    ))
  }
  
  # Define outcome hierarchy (Matcho et al)
  outcome_hierarchy <- c("LB", "SB", "DELIV", "ECT", "AB", "SA", "PREG")
  
  # Determine which episode to keep for each overlap
  overlap_decisions <- overlaps %>%
    mutate(
      # Prioritize based on outcome hierarchy
      hip_priority = match(hip_outcome, outcome_hierarchy),
      pps_priority = match(pps_outcome, outcome_hierarchy),
      
      # Decision rules
      keep_source = case_when(
        # If core overlap > 50%, merge them
        hip_overlap_pct > 0.5 & pps_overlap_pct > 0.5 ~ "MERGE",
        
        # Otherwise prefer better outcome
        hip_priority < pps_priority ~ "HIP",
        pps_priority < hip_priority ~ "PPS",
        
        # If same outcome, prefer higher overlap
        hip_overlap_pct >= pps_overlap_pct ~ "HIP",
        TRUE ~ "PPS"
      )
    )
  
  # Create merged episodes
  merged_episodes <- overlap_decisions %>%
    filter(keep_source == "MERGE") %>%
    mutate(
      # Take best information from both
      episode_start_date = as.Date(pmin(hip_start, pps_start)),
      episode_end_date = as.Date(pmax(hip_end, pps_end)),
      
      # Use better outcome (lower hierarchy number)
      outcome_category = ifelse(hip_priority <= pps_priority, hip_outcome, pps_outcome),
      
      # Average gestational age if both available
      gestational_age_days = case_when(
        !is.na(hip_gest_days) & !is.na(pps_gest_days) ~ 
          as.integer((hip_gest_days + pps_gest_days) / 2),
        !is.na(hip_gest_days) ~ hip_gest_days,
        !is.na(pps_gest_days) ~ pps_gest_days,
        TRUE ~ NA_integer_
      ),
      
      algorithm_used = "MERGED",
      merge_status = "merged",
      person_id = person_id
    ) %>%
    select(person_id, episode_start_date, episode_end_date, 
           outcome_category, gestational_age_days, algorithm_used, merge_status)
  
  # Get non-overlapping episodes
  hip_keep <- hip_episodes %>%
    anti_join(
      overlap_decisions %>% 
        filter(keep_source %in% c("PPS", "MERGE")),
      by = c("person_id", "episode_number" = "hip_episode_num")
    ) %>%
    mutate(
      algorithm_used = "HIP",
      merge_status = "HIP_only"
    )
  
  pps_keep <- pps_episodes %>%
    anti_join(
      overlap_decisions %>%
        filter(keep_source %in% c("HIP", "MERGE")),
      by = c("person_id", "episode_number" = "pps_episode_num")
    ) %>%
    mutate(
      algorithm_used = "PPS",
      merge_status = "PPS_only"
    )
  
  # Get episodes chosen from overlaps (not merged)
  hip_chosen <- hip_episodes %>%
    semi_join(
      overlap_decisions %>% filter(keep_source == "HIP"),
      by = c("person_id", "episode_number" = "hip_episode_num")
    ) %>%
    mutate(
      algorithm_used = "HIP",
      merge_status = "HIP_chosen"
    )
  
  pps_chosen <- pps_episodes %>%
    semi_join(
      overlap_decisions %>% filter(keep_source == "PPS"),
      by = c("person_id", "episode_number" = "pps_episode_num")
    ) %>%
    mutate(
      algorithm_used = "PPS",
      merge_status = "PPS_chosen"
    )
  
  # Combine all episodes
  all_episodes <- bind_rows(
    merged_episodes,
    hip_keep,
    pps_keep,
    hip_chosen,
    pps_chosen
  )
  
  return(all_episodes)
}

#' Add outcomes found in windows
#' @noRd
add_window_outcomes <- function(episodes, cohort_data) {
  
  # Check if we have the necessary window columns
  if (!all(c("lookback_date", "lookahead_date") %in% names(episodes))) {
    # No window columns, return episodes as-is
    return(episodes)
  }
  
  # Get all outcome records
  outcome_records <- bind_rows(
    cohort_data$conditions,
    cohort_data$procedures,
    cohort_data$observations,
    cohort_data$measurements
  ) %>%
    filter(category %in% c("LB", "SB", "DELIV", "ECT", "AB", "SA", "PREG")) %>%
    select(person_id, outcome_date = event_date, found_outcome = category)
  
  if (nrow(outcome_records) == 0) {
    return(episodes)
  }
  
  # Convert dates to ensure they're Date objects
  outcome_records <- outcome_records %>%
    mutate(outcome_date = as.Date(outcome_date))
  
  episodes <- episodes %>%
    mutate(
      lookback_date = as.Date(lookback_date),
      lookahead_date = as.Date(lookahead_date),
      episode_start_date = as.Date(episode_start_date),
      episode_end_date = as.Date(episode_end_date)
    )
  
  # Check for outcomes in windows
  episodes_with_found <- episodes %>%
    left_join(
      outcome_records,
      by = "person_id",
      relationship = "many-to-many"
    ) %>%
    filter(
      # Outcome within lookback/lookahead window
      outcome_date >= lookback_date,
      outcome_date <= lookahead_date
    ) %>%
    group_by(person_id, episode_number) %>%
    # Use Matcho hierarchy to select best outcome
    arrange(
      factor(found_outcome, levels = c("LB", "SB", "DELIV", "ECT", "AB", "SA", "PREG")),
      outcome_date
    ) %>%
    slice(1) %>%
    ungroup()
  
  if (nrow(episodes_with_found) == 0) {
    # No outcomes found in windows
    return(episodes)
  }
  
  # Check if gestational_age_days exists
  has_gest_days <- "gestational_age_days" %in% names(episodes)
  
  # Update episodes with found outcomes
  updated_episodes <- episodes %>%
    left_join(
      episodes_with_found %>%
        select(person_id, episode_number, found_outcome, outcome_date),
      by = c("person_id", "episode_number")
    ) %>%
    mutate(
      # Update outcome if better one found
      outcome_category = coalesce(found_outcome, outcome_category),
      
      # Update end date if outcome found
      episode_end_date = coalesce(outcome_date, episode_end_date)
    )
  
  # Recalculate gestational age
  if (has_gest_days) {
    # If column exists, conditionally update it
    updated_episodes <- updated_episodes %>%
      mutate(
        gestational_age_days = case_when(
          !is.na(episode_end_date) & !is.na(episode_start_date) ~ 
            as.numeric(as.Date(episode_end_date) - as.Date(episode_start_date)),
          TRUE ~ gestational_age_days
        )
      )
  } else {
    # If column doesn't exist, create it
    updated_episodes <- updated_episodes %>%
      mutate(
        gestational_age_days = as.numeric(as.Date(episode_end_date) - as.Date(episode_start_date))
      )
  }
  
  # Clean up temporary columns
  if ("found_outcome" %in% names(updated_episodes)) {
    updated_episodes <- updated_episodes %>%
      select(-found_outcome)
  }
  if ("outcome_date" %in% names(updated_episodes)) {
    updated_episodes <- updated_episodes %>%
      select(-outcome_date)
  }
  
  return(updated_episodes)
}

#' Finalize merged episodes
#' @noRd
finalize_merged_episodes <- function(episodes) {
  
  # Clean up and renumber episodes
  final <- episodes %>%
    select(-any_of(c("lookback_date", "lookahead_date", "expected_end",
                    "next_episode_start", "window_start", "window_end",
                    "algorithm_source", "merge_status"))) %>%
    arrange(person_id, episode_start_date) %>%
    group_by(person_id) %>%
    mutate(
      episode_number = row_number()
    ) %>%
    ungroup()
  
  # Final validation to remove any remaining overlaps
  validated <- final %>%
    group_by(person_id) %>%
    arrange(episode_start_date) %>%
    mutate(
      # Check for overlaps with previous episode
      prev_end = lag(episode_end_date),
      overlap_with_prev = !is.na(prev_end) & episode_start_date <= prev_end,
      
      # Adjust start date if overlapping
      adjusted_start = case_when(
        overlap_with_prev ~ as.Date(prev_end + 1),
        TRUE ~ as.Date(episode_start_date)
      ),
      
      # Recalculate gestational age
      adjusted_gest_days = as.numeric(as.Date(episode_end_date) - as.Date(adjusted_start))
    ) %>%
    filter(
      # Keep only valid episodes
      adjusted_gest_days > 0,
      adjusted_gest_days <= 320
    ) %>%
    mutate(
      episode_start_date = adjusted_start,
      gestational_age_days = adjusted_gest_days
    ) %>%
    select(-prev_end, -overlap_with_prev, -adjusted_start, -adjusted_gest_days) %>%
    ungroup()
  
  return(validated)
}

#' Prepare final episode structure
#' @noRd
prepare_final_episodes <- function(episodes) {
  episodes %>%
    mutate(
      algorithm_used = ifelse(
        !exists("algorithm_used", where = episodes),
        "UNKNOWN",
        algorithm_used
      )
    ) %>%
    select(
      person_id,
      episode_number,
      episode_start_date,
      episode_end_date,
      outcome_category,
      gestational_age_days,
      algorithm_used,
      any_of(c("n_GT_concepts", "n_records", "precision_category", "precision_days"))
    )
}