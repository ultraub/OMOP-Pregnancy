#' Merge HIP and PPS Episodes V2 (Aligned with All of Us)
#'
#' Merges pregnancy episodes exactly as the reference does: a full join of
#' HIP and PPS episodes on temporal overlap (HIP start..end against first PPS
#' concept..last PPS concept + 2 months), iterative resolution of episodes
#' that overlap more than one from the other algorithm, and outcome/end-date
#' reconciliation. No episodes are dropped or truncated here; term-length
#' problems are flagged later by add_episode_quality_metadata().
#'
#' @param hip_episodes Data frame of HIP-identified episodes
#' @param pps_episodes Data frame of PPS-identified episodes
#' @param cohort_data Unused; kept for backward compatibility
#' @param matcho_limits Unused; kept for backward compatibility
#'
#' @return Data frame of merged pregnancy episodes
#' @export
merge_pregnancy_episodes <- function(hip_episodes, pps_episodes, cohort_data = NULL, matcho_limits = NULL) {

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

  # Step 1: Full join on overlap with duplicate flags (reference final_merged_episodes)
  all_episodes <- create_merged_episode_set(hip_episodes, pps_episodes)

  # Step 2: Iterative deduplication (reference final_merged_episodes_no_duplicates)
  deduplicated <- resolve_duplicates_iteratively(all_episodes)

  # Step 3: Outcome and end-date reconciliation, output naming
  resolved_episodes <- format_resolved_episodes(deduplicated, hip_episodes, pps_episodes)

  # Step 4: Renumber episodes per person
  final_episodes <- finalize_merged_episodes(resolved_episodes)

  # Step 5: Prepare final output structure matching All of Us
  final_episodes <- prepare_final_episodes(final_episodes)

  return(final_episodes)
}

#' Create merged episode set with duplicate flags (All of Us aligned)
#' @noRd
create_merged_episode_set <- function(hip_episodes, pps_episodes) {

  # Prepare HIP episodes with algo1 naming
  algo1 <- hip_episodes %>%
    transmute(
      person_id,
      algo1_id = paste(person_id, episode_number, "1", sep = "_"),
      pregnancy_start = as.Date(episode_start_date),
      pregnancy_end = as.Date(episode_end_date),
      # Earliest gestational-age record observed within the episode
      # (reference gest_date from final_episodes_with_length); NA if none
      first_gest_date = if("first_gest_date" %in% names(.)) {
        as.Date(first_gest_date)
      } else {
        as.Date(NA)
      },
      category = outcome_category
    )


  # Prepare PPS episodes with algo2 naming
  algo2 <- pps_episodes %>%
    transmute(
      person_id,
      algo2_id = paste(person_id, episode_number, "2", sep = "_"),
      # First/last gestational timing concept dates, and the outcome found in
      # the lookahead window (NA if none), exactly as the reference joins them
      episode_min_date = as.Date(episode_min_date),
      episode_max_date = as.Date(episode_max_date),
      episode_max_date_plus_two_months = lubridate::`%m+%`(as.Date(episode_max_date),
                                                            lubridate::period(2, "months")),
      algo2_category = outcome_category,
      algo2_outcome_date = as.Date(outcome_date)
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
      # Recorded episode boundaries: earliest and latest observed evidence
      # (reference merged_episode_start/end). For a HIP-only episode with no
      # gestation record the recorded start is the outcome date itself.
      recorded_episode_start = pmin(first_gest_date, episode_min_date, pregnancy_end, na.rm = TRUE),
      recorded_episode_end = pmax(episode_max_date, pregnancy_end, na.rm = TRUE),
      HIP_start_date = pregnancy_start,

      # Working start for the ESD search window: the reference uses
      # pmin(pregnancy_start, recorded_episode_start). The ESD replaces it
      # with the inferred start when timing evidence exists.
      episode_start_date = pmin(pregnancy_start, recorded_episode_start, na.rm = TRUE),
      episode_end_date = recorded_episode_end,

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
      PPS_end_date = algo2_outcome_date
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
    # Outcome concordance check, computed after the PREG fill as in the
    # reference merged_episodes_with_metadata (so PREG/PREG can match)
    mutate(
      outcome_match = case_when(
        HIP_outcome_category == PPS_outcome_category & HIP_outcome_category != "PREG" &
          abs(as.numeric(difftime(HIP_end_date, PPS_end_date, units = "days"))) <= 14 ~ 1L,
        HIP_outcome_category == "PREG" & PPS_outcome_category == "PREG" ~ 1L,
        TRUE ~ 0L
      )
    ) %>%
    # Complex outcome resolution (matching original merged_episodes_with_metadata)
    arrange(person_id, episode_start_date) %>%
    group_by(person_id) %>%
    mutate(
      next_HIP_outcome = lead(HIP_outcome_category),
      outcome_category = case_when(
        outcome_match == 1L ~ HIP_outcome_category,
        outcome_match == 0L & is.na(PPS_outcome_category) ~ HIP_outcome_category,
        outcome_match == 0L & is.na(HIP_outcome_category) ~ PPS_outcome_category,
        # If PPS matches next HIP outcome and dates are 14+ days apart, use HIP
        outcome_match == 0L & HIP_outcome_category != "PREG" & PPS_outcome_category != "PREG" &
          !is.na(next_HIP_outcome) & PPS_outcome_category == next_HIP_outcome &
          HIP_end_date <= PPS_end_date - 14 ~ HIP_outcome_category,
        # If HIP date is 7+ days before PPS, use PPS
        outcome_match == 0L & HIP_outcome_category != "PREG" & PPS_outcome_category != "PREG" &
          HIP_end_date <= PPS_end_date - 7 ~ PPS_outcome_category,
        # Default: use HIP
        TRUE ~ HIP_outcome_category
      ),
      # Matching end date resolution from original
      episode_end_date = case_when(
        outcome_match == 1L ~ HIP_end_date,
        outcome_match == 0L & is.na(PPS_outcome_category) ~ HIP_end_date,
        outcome_match == 0L & is.na(HIP_outcome_category) ~ PPS_end_date,
        outcome_match == 0L & HIP_outcome_category != "PREG" & PPS_outcome_category != "PREG" &
          !is.na(next_HIP_outcome) & PPS_outcome_category == next_HIP_outcome &
          HIP_end_date <= PPS_end_date - 14 ~ HIP_end_date,
        outcome_match == 0L & HIP_outcome_category != "PREG" & PPS_outcome_category != "PREG" &
          HIP_end_date <= PPS_end_date - 7 ~ PPS_end_date,
        !is.na(HIP_end_date) ~ HIP_end_date,
        !is.na(PPS_end_date) ~ PPS_end_date,
        TRUE ~ episode_end_date
      ),
      # Outcome concordance score (0/1/2)
      outcome_concordance = case_when(
        outcome_match == 1L ~ 2L,  # Fully concordant
        outcome_match == 0L & !is.na(HIP_outcome_category) & !is.na(PPS_outcome_category) ~ 1L,
        TRUE ~ 0L  # Insufficient info
      ),
      # Episode length from the resolved dates (no averaging across algorithms)
      gestational_age_days = as.integer(as.numeric(episode_end_date - episode_start_date))
    ) %>%
    mutate(episode_number = row_number()) %>%
    ungroup() %>%
    # Select output columns
    select(
      person_id,
      episode_number,
      recorded_episode_start,
      recorded_episode_end,
      episode_start_date,
      episode_end_date,
      HIP_start_date,
      HIP_outcome_category,
      PPS_outcome_category,
      HIP_end_date,
      PPS_end_date,
      outcome_category,
      outcome_concordance,
      gestational_age_days,
      algorithm_used,
      algo1_id,
      algo2_id
    )

  return(result)
}

#' Finalize merged episodes
#'
#' Drops working columns and renumbers episodes per person in start-date
#' order. Unlike earlier versions, nothing is truncated or dropped here; the
#' reference keeps every merged episode and only flags term-length problems
#' in add_episode_quality_metadata().
#' @noRd
finalize_merged_episodes <- function(episodes, matcho_limits = NULL) {
  episodes %>%
    select(-any_of(c("lookback_date", "lookahead_date", "expected_end",
                    "next_episode_start", "window_start", "window_end",
                    "algorithm_source", "merge_status"))) %>%
    arrange(person_id, episode_start_date) %>%
    group_by(person_id) %>%
    mutate(episode_number = row_number()) %>%
    ungroup()
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
      
      # Recorded boundaries: keep them if the merge produced them, otherwise
      # (single-algorithm path) they equal the episode dates
      recorded_episode_start = if ("recorded_episode_start" %in% names(.)) {
        as.Date(recorded_episode_start)
      } else {
        episode_start_date
      },
      recorded_episode_end = if ("recorded_episode_end" %in% names(.)) {
        as.Date(recorded_episode_end)
      } else {
        episode_end_date
      },

      # Calculate episode length in months (All of Us uses 30.25)
      # Handle NA dates gracefully
      recorded_episode_length = ifelse(
        !is.na(recorded_episode_end) & !is.na(recorded_episode_start),
        as.numeric(recorded_episode_end - recorded_episode_start) / 30.25,
        NA_real_
      ),

      # Episode length in days (single-algorithm path may not carry it)
      gestational_age_days = if ("gestational_age_days" %in% names(.)) {
        gestational_age_days
      } else {
        as.integer(as.numeric(episode_end_date - episode_start_date))
      },
      
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
      recorded_episode_start,
      recorded_episode_end,
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

#' Add episode quality metadata (All of Us aligned)
#'
#' Port of the reference merged_episodes_with_metadata(). Call after the
#' merge and the ESD. Finalizes the inferred dates and adds quality flags:
#' - inferred_episode_end: the merge's resolved end date
#' - inferred_episode_start: the ESD start, or inferred end minus the
#'   category max term when the ESD found no timing evidence
#' - precision_days / precision_category: from the ESD, or
#'   max_term - min_term when no timing evidence
#' - gestational_age_days_calculated: inferred end minus inferred start
#' - term_duration_flag: 1 if GA within category [min_term, max_term]
#'   (PREG: <= 301), 0 otherwise
#' - outcome_concordance_score: 2 = outcome match + term ok + GW evidence,
#'   1 = term ok + GW evidence, 0 otherwise
#' - preterm_status_from_calculation: 1 if GA < 259 days
#' episode_start_date / episode_end_date / gestational_age_days are set to
#' the inferred values so downstream output keeps its meaning.
#'
#' Note: the reference term table has no PREG row, so a PREG episode with
#' no timing evidence gets an NA inferred start there. matcho_limits.csv
#' carries PREG (30, 301), which yields end - 301 and "non-specific".
#'
#' @param episodes Data frame of episodes (post-merge, post-ESD)
#' @param matcho_limits Data frame with min_term and max_term per category
#' @return Episodes with finalized dates and quality metadata
#' @export
add_episode_quality_metadata <- function(episodes, matcho_limits = NULL) {

  if (is.null(episodes) || nrow(episodes) == 0) {
    return(episodes)
  }

  # Load matcho_limits if not provided
  if (is.null(matcho_limits)) {
    matcho_limits <- load_matcho_limits()
  }

  result <- episodes

  # Columns the ESD adds; default them if the ESD was not run
  if (!"GW_flag" %in% names(result)) result$GW_flag <- 0L
  if (!"GR3m_flag" %in% names(result)) result$GR3m_flag <- 0L
  if (!"inferred_episode_start" %in% names(result)) result$inferred_episode_start <- as.Date(NA)
  if (!"precision_days" %in% names(result)) result$precision_days <- NA_real_
  if (!"precision_category" %in% names(result)) result$precision_category <- NA_character_

  # outcome_match (reference lines 494-501); the merge computes it too
  if (!"outcome_match" %in% names(result)) {
    if (all(c("HIP_outcome_category", "PPS_outcome_category",
              "HIP_end_date", "PPS_end_date") %in% names(result))) {
      result <- result %>%
        mutate(
          outcome_match = case_when(
            HIP_outcome_category == PPS_outcome_category &
              HIP_outcome_category != "PREG" &
              abs(as.numeric(difftime(HIP_end_date, PPS_end_date, units = "days"))) <= 14 ~ 1L,
            HIP_outcome_category == "PREG" & PPS_outcome_category == "PREG" ~ 1L,
            TRUE ~ 0L
          )
        )
    } else {
      result$outcome_match <- 0L
    }
  }

  result <- result %>%
    mutate(
      GW_flag = coalesce(as.integer(GW_flag), 0L),
      GR3m_flag = coalesce(as.integer(GR3m_flag), 0L),
      # The merge already resolved the end date the reference way
      inferred_episode_end = as.Date(episode_end_date),
      inferred_episode_start = as.Date(inferred_episode_start),
      precision_days = as.numeric(precision_days)
    ) %>%
    left_join(
      matcho_limits %>% select(category, min_term, max_term),
      by = c("outcome_category" = "category")
    ) %>%
    mutate(
      # Reference fallbacks when the ESD found no timing evidence
      inferred_episode_start = if_else(
        is.na(inferred_episode_start),
        inferred_episode_end - max_term,
        inferred_episode_start
      ),
      precision_days = if_else(is.na(precision_days),
                               as.numeric(max_term - min_term), precision_days),
      precision_category = if_else(is.na(precision_category),
                                   assign_precision_category(precision_days),
                                   precision_category),

      gestational_age_days_calculated = as.integer(
        difftime(inferred_episode_end, inferred_episode_start, units = "days")
      ),

      term_duration_flag = case_when(
        gestational_age_days_calculated >= min_term &
          gestational_age_days_calculated <= max_term ~ 1L,
        outcome_category == "PREG" &
          gestational_age_days_calculated <= 301L ~ 1L,
        TRUE ~ 0L
      ),
      outcome_concordance_score = case_when(
        outcome_match == 1L & term_duration_flag == 1L & GW_flag == 1L ~ 2L,
        outcome_match == 0L & term_duration_flag == 1L & GW_flag == 1L ~ 1L,
        TRUE ~ 0L
      ),
      preterm_status_from_calculation = if_else(
        gestational_age_days_calculated < 259L, 1L, 0L
      ),

      # Final dates for output
      episode_start_date = inferred_episode_start,
      episode_end_date = inferred_episode_end,
      gestational_age_days = gestational_age_days_calculated
    ) %>%
    select(-min_term, -max_term)

  return(result)
}
