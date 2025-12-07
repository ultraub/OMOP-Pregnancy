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
    # Ensure PPS episodes have algorithm_used column
    pps_episodes$algorithm_used <- "PPS"
    return(prepare_final_episodes(pps_episodes))
  }
  
  if (is.null(pps_episodes) || nrow(pps_episodes) == 0) {
    # Ensure HIP episodes have algorithm_used column
    hip_episodes$algorithm_used <- "HIP"
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
  
  # Step 6: Prepare final output structure matching All of Us
  final_episodes <- prepare_final_episodes(final_episodes)
  
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

#' Resolve overlapping episodes using iterative deduplication (All of Us aligned)
#'
#' Implements the exact All of Us deduplication logic:
#' 1. Full join HIP and PPS episodes with overlap detection
#' 2. Flag duplicates (one episode overlapping multiple from other algorithm)
#' 3. Iteratively resolve duplicates by selecting best match based on:
#'    - Date proximity (closest end dates)
#'    - Outcome presence (prefer episodes with outcomes)
#'    - Valid episode length (<310 days)
#' @noRd
resolve_episode_overlaps <- function(hip_episodes, pps_episodes, overlaps) {


  if (nrow(overlaps) == 0) {
    # No overlaps, return all episodes separately
    hip_only <- hip_episodes %>%
      mutate(
        algo1_id = paste(person_id, episode_number, "1", sep = "_"),
        algo2_id = NA_character_,
        algorithm_used = "HIP",
        HIP_outcome_category = outcome_category,
        PPS_outcome_category = NA_character_,
        HIP_end_date = episode_end_date,
        PPS_end_date = as.Date(NA),
        episode_min_date = as.Date(NA),
        episode_max_date = as.Date(NA)
      )

    pps_only <- pps_episodes %>%
      mutate(
        algo1_id = NA_character_,
        algo2_id = paste(person_id, episode_number, "2", sep = "_"),
        algorithm_used = "PPS",
        HIP_outcome_category = NA_character_,
        PPS_outcome_category = outcome_category,
        HIP_end_date = as.Date(NA),
        PPS_end_date = episode_end_date,
        episode_min_date = episode_start_date,
        episode_max_date = episode_end_date
      )

    return(bind_rows(hip_only, pps_only))
  }

  # Step 1: Create full join of HIP and PPS with overlap detection
  # Following All of Us final_merged_episodes() pattern
  all_episodes <- create_merged_episode_set(hip_episodes, pps_episodes, overlaps)

  # Step 2: Apply iterative deduplication (All of Us pattern)
  deduplicated <- resolve_duplicates_iteratively(all_episodes)

  # Step 3: Format output
  result <- format_resolved_episodes(deduplicated, hip_episodes, pps_episodes)


  return(result)
}

#' Create merged episode set with duplicate flags (All of Us aligned)
#' @noRd
create_merged_episode_set <- function(hip_episodes, pps_episodes, overlaps) {

  # Prepare HIP episodes with algo1 naming
  algo1 <- hip_episodes %>%
    transmute(
      person_id,
      algo1_id = paste(person_id, episode_number, "1", sep = "_"),
      pregnancy_start = episode_start_date,
      pregnancy_end = episode_end_date,
      first_gest_date = episode_start_date,  # Use start as first gest date
      category = outcome_category,
      hip_gest_days = if("gestational_age_days" %in% names(.)) gestational_age_days else NA_real_

    )


  # Prepare PPS episodes with algo2 naming
  algo2 <- pps_episodes %>%
    transmute(
      person_id,
      algo2_id = paste(person_id, episode_number, "2", sep = "_"),
      episode_min_date = episode_start_date,
      episode_max_date = episode_end_date,
      episode_max_date_plus_two_months = episode_end_date + (2 * DAYS_PER_MONTH),
      algo2_category = outcome_category,
      algo2_outcome_date = episode_end_date,
      pps_gest_days = if("gestational_age_days" %in% names(.)) gestational_age_days else NA_real_
    )

  # Full join with overlap detection (All of Us pattern)
  all_episodes <- algo1 %>%
    full_join(algo2, by = join_by(
      person_id,
      overlaps(
        pregnancy_start, pregnancy_end,
        episode_min_date, episode_max_date_plus_two_months
      )
    )) %>%
    mutate(
      merged_episode_start = pmin(first_gest_date, episode_min_date, pregnancy_end, na.rm = TRUE),
      merged_episode_end = pmax(episode_max_date, pregnancy_end, na.rm = TRUE),
      merged_episode_length = as.numeric(difftime(merged_episode_end, merged_episode_start, units = "days")) / 30.25
    )

  # Add duplicate flags for HIP episodes (algo1)
  all_episodes <- all_episodes %>%
    group_by(algo1_id) %>%
    mutate(
      algo1_dup = if_else(is.na(algo1_id)[1], NA_integer_, as.integer(n() > 1))
    ) %>%
    ungroup()

  # Add duplicate flags for PPS episodes (algo2)
  all_episodes <- all_episodes %>%
    group_by(algo2_id) %>%
    mutate(
      algo2_dup = if_else(is.na(algo2_id)[1], NA_integer_, as.integer(n() > 1))
    ) %>%
    ungroup()

  return(all_episodes)
}

#' Resolve duplicates iteratively (All of Us aligned)
#'
#' Implements exact All of Us final_merged_episodes_no_duplicates() logic:
#' - Iteratively select best match for duplicated episodes
#' - Priority: closest end dates -> valid episode length -> has outcome
#' @noRd
resolve_duplicates_iteratively <- function(all_episodes) {

  # Separate non-duplicated episodes
  no_dup_df <- all_episodes %>%
    filter(
      (algo1_dup == 0 & algo2_dup == 0) |
      (algo1_dup == 0 & is.na(algo2_dup)) |
      (is.na(algo1_dup) & algo2_dup == 0)
    )

  # Get episodes needing deduplication
  dup_df <- all_episodes %>%
    filter(
      (algo1_dup == 1 & !is.na(algo2_id)) |
      (algo2_dup == 1 & !is.na(algo1_id))
    )

  if (nrow(dup_df) == 0) {
    return(no_dup_df)
  }

  # Iterative deduplication (up to 5 rounds, matching All of Us)
  keep_list <- list()
  current_df <- dup_df

  for (round in 1:5) {
    if (nrow(current_df) == 0) break

    # Process HIP duplicates with PPS overlap
    best_algo1 <- current_df %>%
      filter(algo1_dup == 1 & !is.na(algo2_id)) %>%
      mutate(
        # Calculate date difference (All of Us logic)
        date_diff = abs(as.numeric(difftime(pregnancy_end, episode_max_date, units = "days"))),
        # Deprioritize episodes without outcomes
        date_diff = ifelse(is.na(algo2_category), 10000, date_diff),
        # Calculate episode length for tie-breaking
        new_date_diff = abs(as.numeric(difftime(episode_max_date, episode_min_date, units = "days"))),
        # Deprioritize invalid length or no outcome
        new_date_diff = ifelse(is.na(algo2_category) | new_date_diff > 310, -1, new_date_diff)
      ) %>%
      group_by(algo1_id) %>%
      slice_min(date_diff, n = 1, with_ties = TRUE) %>%
      slice_max(new_date_diff, n = 1, with_ties = FALSE) %>%
      ungroup() %>%
      select(-date_diff, -new_date_diff)

    # Process PPS duplicates with HIP overlap
    best_algo2 <- current_df %>%
      filter(algo2_dup == 1 & !is.na(algo1_id)) %>%
      mutate(
        date_diff = abs(as.numeric(difftime(pregnancy_end, episode_max_date, units = "days"))),
        new_date_diff = abs(as.numeric(difftime(episode_max_date, episode_min_date, units = "days"))),
        new_date_diff = ifelse(new_date_diff > 310, -1, new_date_diff)
      ) %>%
      group_by(algo2_id) %>%
      slice_min(date_diff, n = 1, with_ties = TRUE) %>%
      slice_max(new_date_diff, n = 1, with_ties = FALSE) %>%
      ungroup() %>%
      select(-date_diff, -new_date_diff)

    # Combine results from this round
    best_both <- bind_rows(best_algo1, best_algo2) %>%
      select(-any_of(c("algo1_dup", "algo2_dup"))) %>%
      distinct()

    # Recalculate duplicate flags
    best_both <- best_both %>%
      group_by(algo1_id) %>%
      mutate(algo1_dup = if_else(is.na(algo1_id)[1], NA_integer_, as.integer(n() > 1))) %>%
      ungroup() %>%
      group_by(algo2_id) %>%
      mutate(algo2_dup = if_else(is.na(algo2_id)[1], NA_integer_, as.integer(n() > 1))) %>%
      ungroup()

    # Separate resolved and still-duplicated
    resolved <- best_both %>%
      filter(
        !(algo1_dup == 1 & !is.na(algo2_id)) &
        !(algo2_dup == 1 & !is.na(algo1_id))
      )

    keep_list[[round]] <- resolved

    # Remaining duplicates for next round
    current_df <- best_both %>%
      filter(
        (algo1_dup == 1 & !is.na(algo2_id)) |
        (algo2_dup == 1 & !is.na(algo1_id))
      )
  }

  # If any duplicates remain after 5 rounds, keep them anyway
  if (nrow(current_df) > 0) {
    keep_list[[length(keep_list) + 1]] <- current_df
  }

  # Combine all results
  all_resolved <- bind_rows(no_dup_df, bind_rows(keep_list)) %>%
    distinct() %>%
    # Final duplicate flag recalculation
    group_by(algo1_id) %>%
    mutate(algo1_dup = if_else(is.na(algo1_id)[1], NA_integer_, as.integer(n() > 1))) %>%
    ungroup() %>%
    group_by(algo2_id) %>%
    mutate(algo2_dup = if_else(is.na(algo2_id)[1], NA_integer_, as.integer(n() > 1))) %>%
    ungroup()

  return(all_resolved)
}

#' Format resolved episodes for output (All of Us aligned)
#' @noRd
format_resolved_episodes <- function(resolved, hip_episodes, pps_episodes) {

  # Recalculate merged dates and create output structure
  result <- resolved %>%
    mutate(
      # Recalculate merged episode boundaries
      merged_episode_start = pmin(first_gest_date, episode_min_date, pregnancy_end, na.rm = TRUE),
      merged_episode_end = pmax(episode_max_date, pregnancy_end, na.rm = TRUE),

      # Set episode dates
      episode_start_date = merged_episode_start,
      episode_end_date = merged_episode_end,

      # Determine algorithm used
      algorithm_used = case_when(
        !is.na(algo1_id) & !is.na(algo2_id) ~ "MERGED",
        !is.na(algo1_id) ~ "HIP",
        !is.na(algo2_id) ~ "PPS",
        TRUE ~ "UNKNOWN"
      ),

      # Set outcome columns (All of Us naming)
      HIP_outcome_category = category,
      PPS_outcome_category = algo2_category,
      HIP_end_date = pregnancy_end,
      PPS_end_date = algo2_outcome_date,

      # Combined outcome (prefer HIP hierarchy)
      outcome_category = coalesce(category, algo2_category),

      # Gestational age
      gestational_age_days = case_when(
        !is.na(hip_gest_days) & !is.na(pps_gest_days) ~
          as.integer((hip_gest_days + pps_gest_days) / 2),
        !is.na(hip_gest_days) ~ as.integer(hip_gest_days),
        !is.na(pps_gest_days) ~ as.integer(pps_gest_days),
        TRUE ~ as.integer(as.numeric(episode_end_date - episode_start_date))
      )
    ) %>%
    # Assign PPS episodes without outcomes to PREG (All of Us logic)
    mutate(
      PPS_outcome_category = if_else(
        !is.na(algo2_id) & is.na(PPS_outcome_category),
        "PREG",
        PPS_outcome_category
      ),
      PPS_end_date = if_else(
        !is.na(algo2_id) & is.na(PPS_end_date),
        episode_max_date,
        PPS_end_date
      )
    ) %>%
    # Renumber episodes per person
    arrange(person_id, episode_start_date) %>%
    group_by(person_id) %>%
    mutate(episode_number = row_number()) %>%
    ungroup() %>%
    # Select output columns
    select(
      person_id,
      episode_number,
      episode_start_date,
      episode_end_date,
      HIP_outcome_category,
      PPS_outcome_category,
      HIP_end_date,
      PPS_end_date,
      outcome_category,
      gestational_age_days,
      algorithm_used,
      algo1_id,
      algo2_id
    )

  return(result)
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
    # Use Matcho hierarchy to select best outcome found in window
    # Hierarchy (best to least definitive): LB, SB, DELIV, ECT, AB, SA, PREG
    # LB (Live Birth) and SB (Stillbirth) are most definitive outcomes
    # DELIV captures delivery without specific outcome detail
    # ECT (Ectopic), AB (Abortion), SA (Spontaneous Abortion) are loss outcomes  
    # PREG is used when pregnancy detected but outcome unknown
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
      gestational_age_days = adjusted_gest_days,
      
      # Also adjust HIP/PPS end dates if they match episode_end_date
      HIP_end_date = if("HIP_end_date" %in% names(.)) {
        as.Date(HIP_end_date)
      } else {
        as.Date(NA)
      },
      PPS_end_date = if("PPS_end_date" %in% names(.)) {
        as.Date(PPS_end_date)
      } else {
        as.Date(NA)
      }
    ) %>%
    select(-prev_end, -overlap_with_prev, -adjusted_start, -adjusted_gest_days) %>%
    ungroup()
  
  return(validated)
}

#' Prepare final episode structure
#' @noRd
prepare_final_episodes <- function(episodes) {
  
  # Ensure required columns exist
  if (!all(c("episode_start_date", "episode_end_date") %in% names(episodes))) {
    stop("Episodes must have episode_start_date and episode_end_date columns")
  }
  
  episodes %>%
    mutate(
      # Ensure dates are Date type (handle NA values safely)
      episode_start_date = as.Date(episode_start_date),
      episode_end_date = as.Date(episode_end_date),
      
      # Preserve algorithm_used if it exists, otherwise set based on context
      algorithm_used = if (!"algorithm_used" %in% names(episodes)) {
        # If column doesn't exist, it means we only ran one algorithm
        # Check which one based on flag patterns or default to HIP
        if ("HIP_flag" %in% names(.) && any(HIP_flag == 1, na.rm = TRUE)) {
          "HIP"
        } else if ("PPS_flag" %in% names(.) && any(PPS_flag == 1, na.rm = TRUE)) {
          "PPS"
        } else {
          "HIP"  # Default to HIP if can't determine
        }
      } else {
        algorithm_used  # Keep existing values
      },
      
      # Calculate episode length in months (All of Us uses 30.25)
      # Handle NA dates gracefully
      recorded_episode_length = ifelse(
        !is.na(episode_end_date) & !is.na(episode_start_date),
        as.numeric(episode_end_date - episode_start_date) / 30.25,
        NA_real_
      ),
      
      # Set flags based on algorithm
      HIP_flag = case_when(
        algorithm_used == "HIP" ~ 1L,
        algorithm_used == "MERGED" ~ 1L,
        algorithm_used == "HIP_chosen" ~ 1L,
        algorithm_used == "HIP_only" ~ 1L,
        TRUE ~ 0L
      ),
      PPS_flag = case_when(
        algorithm_used == "PPS" ~ 1L,
        algorithm_used == "MERGED" ~ 1L,
        algorithm_used == "PPS_chosen" ~ 1L,
        algorithm_used == "PPS_only" ~ 1L,
        TRUE ~ 0L
      ),
      
      # Ensure HIP/PPS columns exist if not already present
      HIP_outcome_category = if("HIP_outcome_category" %in% names(.)) {
        HIP_outcome_category
      } else {
        ifelse(HIP_flag == 1, outcome_category, NA_character_)
      },
      PPS_outcome_category = if("PPS_outcome_category" %in% names(.)) {
        PPS_outcome_category
      } else {
        ifelse(PPS_flag == 1, outcome_category, NA_character_)
      },
      HIP_end_date = if("HIP_end_date" %in% names(.)) {
        HIP_end_date
      } else {
        ifelse(HIP_flag == 1, episode_end_date, as.Date(NA))
      },
      PPS_end_date = if("PPS_end_date" %in% names(.)) {
        PPS_end_date
      } else {
        ifelse(PPS_flag == 1, episode_end_date, as.Date(NA))
      }
    ) %>%
    # Select columns in All of Us order but keep both naming conventions
    # episode_start_date/episode_end_date for ESD compatibility
    # recorded_episode_start/recorded_episode_end for All of Us compatibility
    transmute(
      person_id,
      episode_number,
      # All of Us naming
      recorded_episode_start = episode_start_date,
      recorded_episode_end = episode_end_date,
      recorded_episode_length,
      # Also keep original names for ESD algorithm
      episode_start_date,
      episode_end_date,
      # Outcome information
      HIP_outcome_category,
      PPS_outcome_category,
      HIP_end_date,
      PPS_end_date,
      HIP_flag,
      PPS_flag,
      outcome_category,
      gestational_age_days,
      algorithm_used
    )
}