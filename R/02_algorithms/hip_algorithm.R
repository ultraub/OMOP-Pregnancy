#' Run HIP Algorithm V2 (Aligned with All of Us)
#'
#' Implements the Hierarchical Identification of Pregnancy (HIP) algorithm
#' following the exact methodology from Matcho et al. and All of Us Research Program.
#' 
#' The algorithm processes pregnancy outcomes in strict hierarchical order:
#' 1. Live Births (LB) - highest priority, establishes primary episode dates
#' 2. Stillbirths (SB) - added with spacing constraints relative to LB
#' 3. Ectopic pregnancies (ECT) - added with spacing constraints relative to LB/SB
#' 4. Abortions (AB/SA) - lowest priority outcomes, strictest spacing requirements
#' 5. Delivery records (DELIV) - delivery-only records, may modify LB/SB dates
#'
#' Each outcome type has specific minimum spacing requirements from Matcho et al.
#' that prevent overlapping or implausibly close pregnancies.
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
  
  # Source database utilities for compute operations
  source("R/03_utilities/database_utils.R")
  
  # Step 2: Process outcomes hierarchically following Matcho et al. methodology
  # HIERARCHY STEP 1: Live Births (LB) - Highest Priority
  # Live births are processed first as they represent the most definitive pregnancy 
  # outcomes and establish the primary timeline for episode identification.
  # Minimum spacing between LB episodes: defined in matcho_outcome_limits table.
  # All subsequent outcome types must respect LB episode boundaries.
  lb_episodes <- process_outcome_category(
    initial_cohort, 
    categories = "LB",
    matcho_outcome_limits
  )
  
  # DATABASE OPTIMIZATION: Compute intermediate results (mirrors All of Us aou_compute())
  # The All of Us implementation uses aou_compute() to materialize intermediate
  # query results at strategic points, preventing memory issues with large datasets
  # and complex joins. Our omop_compute() serves the same function for OMOP CDM.
  # Critical after each hierarchical step to ensure stable data for subsequent joins.
  if ("tbl_lazy" %in% class(lb_episodes) || "tbl_sql" %in% class(lb_episodes)) {
    lb_episodes <- omop_compute(lb_episodes)
  }
  
  # HIERARCHY STEP 2: Add Stillbirths (SB) with spacing constraints
  # Stillbirths are added only if they maintain required minimum spacing from 
  # existing live birth episodes. The spacing requirements prevent implausible 
  # scenarios where deliveries occur too close together.
  # - SB after LB: minimum days from matcho_outcome_limits (LB->SB)
  # - LB after SB: minimum days from matcho_outcome_limits (SB->LB)
  # Episodes that violate spacing constraints are excluded from final results.
  sb_episodes <- process_outcome_category(
    initial_cohort,
    categories = "SB", 
    matcho_outcome_limits
  )
  
  # Compute after processing
  if ("tbl_lazy" %in% class(sb_episodes) || "tbl_sql" %in% class(sb_episodes)) {
    sb_episodes <- omop_compute(sb_episodes)
  }
  
  # Combine LB and SB, checking spacing
  lb_sb_episodes <- add_stillbirth_episodes(
    lb_episodes,
    sb_episodes,
    matcho_outcome_limits
  )
  
  # Compute combined result
  if ("tbl_lazy" %in% class(lb_sb_episodes) || "tbl_sql" %in% class(lb_sb_episodes)) {
    lb_sb_episodes <- omop_compute(lb_sb_episodes)
  }
  
  # HIERARCHY STEP 3: Add Ectopic Pregnancies (ECT) with complex spacing logic
  # Ectopic pregnancies have different spacing requirements relative to deliveries:
  # - ECT after LB/SB: minimum days from matcho_outcome_limits  
  # - LB after ECT: different minimum days (typically shorter than LB->ECT)
  # - SB after ECT: different minimum days (typically shorter than SB->ECT)
  # The asymmetric spacing reflects biological reality that ectopic pregnancies
  # can be followed more quickly by normal pregnancies than vice versa.
  ect_episodes <- process_outcome_category(
    initial_cohort,
    categories = "ECT",
    matcho_outcome_limits
  )
  
  # Compute after processing
  if ("tbl_lazy" %in% class(ect_episodes) || "tbl_sql" %in% class(ect_episodes)) {
    ect_episodes <- omop_compute(ect_episodes)
  }
  
  lb_sb_ect_episodes <- add_ectopic_episodes(
    lb_sb_episodes,
    ect_episodes,
    matcho_outcome_limits
  )
  
  # Compute combined result
  if ("tbl_lazy" %in% class(lb_sb_ect_episodes) || "tbl_sql" %in% class(lb_sb_ect_episodes)) {
    lb_sb_ect_episodes <- omop_compute(lb_sb_ect_episodes)
  }
  
  # HIERARCHY STEP 4: Add Abortions (AB/SA) - Lowest Priority Outcomes
  # Spontaneous abortions (SA) and induced abortions (AB) are treated identically
  # in the hierarchy and have the most restrictive spacing requirements.
  # Combined processing (SA converted to AB internally) ensures consistent spacing
  # logic between the two abortion types while maintaining separate outcome codes.
  # Must respect minimum spacing from ALL higher-priority outcomes (LB, SB, ECT).
  ab_sa_episodes <- process_outcome_category(
    initial_cohort,
    categories = c("AB", "SA"),
    matcho_outcome_limits
  )
  
  # Compute after processing
  if ("tbl_lazy" %in% class(ab_sa_episodes) || "tbl_sql" %in% class(ab_sa_episodes)) {
    ab_sa_episodes <- omop_compute(ab_sa_episodes)
  }
  
  all_outcome_episodes <- add_abortion_episodes(
    lb_sb_ect_episodes,
    ab_sa_episodes,
    matcho_outcome_limits
  )
  
  # Compute combined result
  if ("tbl_lazy" %in% class(all_outcome_episodes) || "tbl_sql" %in% class(all_outcome_episodes)) {
    all_outcome_episodes <- omop_compute(all_outcome_episodes)
  }
  
  # HIERARCHY STEP 5: Add Delivery-only records (DELIV) with date modification
  # Delivery records without specific outcomes are processed last and have special
  # behavior: if a DELIV record precedes an LB/SB within the minimum spacing window,
  # the LB/SB date is moved BACK to match the earlier delivery date.
  # This handles cases where the delivery procedure is recorded separately from
  # the birth outcome, ensuring the actual delivery date is captured.
  deliv_episodes <- process_outcome_category(
    initial_cohort,
    categories = "DELIV",
    matcho_outcome_limits
  )
  
  # Compute after processing
  if ("tbl_lazy" %in% class(deliv_episodes) || "tbl_sql" %in% class(deliv_episodes)) {
    deliv_episodes <- omop_compute(deliv_episodes)
  }
  
  final_episodes <- add_delivery_episodes(
    all_outcome_episodes,
    deliv_episodes,
    matcho_outcome_limits
  )
  
  # Compute final result
  if ("tbl_lazy" %in% class(final_episodes) || "tbl_sql" %in% class(final_episodes)) {
    final_episodes <- omop_compute(final_episodes)
  }
  
  # Step 3: Add gestational age information
  episodes_with_gest <- add_gestational_age_info(final_episodes, all_records, matcho_limits)
  
  # Step 4: Calculate estimated start dates
  episodes_with_dates <- calculate_hip_start_dates(episodes_with_gest, matcho_limits)
  
  # Step 5: Validate and finalize
  validated_episodes <- validate_hip_episodes(episodes_with_dates)
  
  # Add algorithm identifier
  validated_episodes$algorithm_used <- "HIP"
  
  return(validated_episodes)
}

#' Process a specific outcome category with same-type spacing
#' 
#' Identifies distinct episodes within a single outcome category (e.g., multiple
#' live births for the same person) using minimum spacing requirements from 
#' matcho_outcome_limits. Each outcome category has self-spacing rules that
#' prevent episodes of the same type from being implausibly close.
#' 
#' For AB/SA categories processed together, uses the minimum spacing between
#' AB and SA types to ensure proper episode separation.
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
    # For AB/SA, look up the appropriate spacing from matcho_outcome_limits
    # Use the minimum spacing between AB and SA
    ab_sa_spacing <- matcho_outcome_limits %>%
      filter(
        (first_preg_category == "AB" & outcome_preg_category == "SA") |
        (first_preg_category == "SA" & outcome_preg_category == "AB")
      ) %>%
      pull(min_days)
    
    min_days <- ifelse(length(ab_sa_spacing) > 0, min(ab_sa_spacing), 56)
  }
  
  if (length(min_days) == 0) {
    min_days <- 180  # Default fallback
  }
  
  # Deduplicate: one record per person-date, keeping lowest concept_id (matches original)
  category_records <- category_records %>%
    group_by(person_id, event_date) %>%
    slice_min(order_by = concept_id, n = 1, with_ties = FALSE) %>%
    ungroup()

  # Identify episodes using original's approach:
  # First record for each person = always an episode start
  # Any subsequent record with days_since_last >= min_days = new episode start
  # The episode date is the FIRST qualifying date (not max), matching original final_visits()
  episodes_marked <- category_records %>%
    group_by(person_id) %>%
    arrange(event_date) %>%
    mutate(
      days_since_last = as.numeric(event_date - lag(event_date))
    ) %>%
    ungroup()

  # First records per person
  first_records <- episodes_marked %>%
    group_by(person_id) %>%
    slice_min(order_by = event_date, n = 1, with_ties = FALSE) %>%
    ungroup()

  # Records that start new episodes (sufficient spacing from previous)
  spaced_records <- episodes_marked %>%
    filter(!is.na(days_since_last), days_since_last >= min_days)

  # Combine: each row is an episode with outcome_date = the first qualifying date
  episodes <- bind_rows(first_records, spaced_records) %>%
    distinct(person_id, event_date, .keep_all = TRUE) %>%
    arrange(person_id, event_date) %>%
    group_by(person_id) %>%
    mutate(episode_number = row_number()) %>%
    ungroup() %>%
    transmute(
      person_id,
      episode_number,
      outcome_date = as.Date(event_date),
      outcome_category = category,
      n_visits = 1L  # Each episode is represented by its first qualifying record
    )

  return(episodes)
}

#' Add stillbirth episodes with proper spacing following Matcho et al.
#' 
#' Implements the complex spacing logic for stillbirths relative to live births:
#' 1. SB episodes are validated against existing LB episodes in both directions
#' 2. before_min: minimum days that must pass after LB before SB is valid
#' 3. after_min: minimum days that must pass after SB before LB is valid  
#' 4. SB episodes failing either constraint are excluded from final results
#' 
#' The asymmetric spacing accounts for different biological constraints in
#' each direction and follows Matcho et al. evidence-based recommendations.
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
      days_after = as.numeric(as.Date(outcome_date) - as.Date(lag(outcome_date))),
      days_before = as.numeric(as.Date(lead(outcome_date)) - as.Date(outcome_date))
    )
  
  # Filter SB episodes that don't meet spacing requirements
  valid_sb <- combined %>%
    filter(outcome_category == "SB") %>%
    filter(
      # Keep if isolated or properly spaced
      (is.na(prev_category) & is.na(next_category)) |
      
      (prev_category != "LB" & is.na(next_category)) |
      # same but opposite
      (next_category != "LB" & is.na(prev_category)) |
      (prev_category != "LB" & next_category != "LB") |
      # the last episode was a live birth and this one happens after the minimum
      (prev_category == "LB" & days_after >= before_min & is.na(next_category)) |
      # the next episode is a live birth and happens after the minimum
      (next_category == "LB" & days_before >= after_min & is.na(prev_category)) |
      # or surrounded by two live births spaced sufficiently
      (next_category == "LB" & days_before >= after_min & prev_category == "LB" & days_after >= before_min)
    ) %>%
    ungroup()
  
  # Combine valid SB with all LB
  result <- bind_rows(
    lb_episodes,  # Use original LB episodes
    valid_sb
  ) %>%
    select(-any_of(c("prev_category", "next_category", "days_after", "days_before"))) %>%
    arrange(person_id, outcome_date)
  
  return(result)
}

#' Add ectopic episodes with asymmetric spacing constraints
#' 
#' Ectopic pregnancies have the most complex spacing rules due to their unique
#' clinical characteristics:
#' - Different minimum spacing when ECT follows LB vs SB (delivery outcomes)
#' - Different minimum spacing when LB/SB follows ECT (typically shorter)
#' - Must validate against ALL existing higher-priority episodes (LB, SB)
#' 
#' The asymmetric nature reflects that ectopic pregnancies can be followed by
#' normal pregnancies sooner than normal pregnancies can be followed by ectopic.
#' @noRd
add_ectopic_episodes <- function(lb_sb_episodes, ect_episodes, matcho_outcome_limits) {
  
  if (nrow(ect_episodes) == 0) {
    return(lb_sb_episodes)
  }
  
  # Get minimum days for ECT spacing
  # LB and SB have the same days for ECT following them (both 70 in original)
  before_min <- matcho_outcome_limits %>%
    filter(first_preg_category == "LB", outcome_preg_category == "ECT") %>%
    pull(min_days)

  # Keep separate after_min for LB vs SB (asymmetric: ECT->LB=168, ECT->SB=154)
  after_min_lb <- matcho_outcome_limits %>%
    filter(first_preg_category == "ECT", outcome_preg_category == "LB") %>%
    pull(min_days)

  after_min_sb <- matcho_outcome_limits %>%
    filter(first_preg_category == "ECT", outcome_preg_category == "SB") %>%
    pull(min_days)

  # Apply spacing logic matching original add_ectopic()
  combined <- bind_rows(lb_sb_episodes, ect_episodes) %>%
    group_by(person_id) %>%
    arrange(outcome_date) %>%
    mutate(
      prev_category = lag(outcome_category),
      next_category = lead(outcome_category),
      days_after = as.numeric(as.Date(outcome_date) - as.Date(lag(outcome_date))),
      days_before = as.numeric(as.Date(lead(outcome_date)) - as.Date(outcome_date))
    )

  # Filter ECT episodes using exact same 9 conditions as original add_ectopic()
  valid_ect <- combined %>%
    filter(outcome_category == "ECT") %>%
    filter(
      # 1. It's the only episode
      (is.na(days_after) & is.na(days_before)) |
      # 2-4. Not preceded/followed by LB or SB
      (!prev_category %in% c("LB", "SB") & is.na(next_category)) |
      (!next_category %in% c("LB", "SB") & is.na(prev_category)) |
      (!prev_category %in% c("LB", "SB") & !next_category %in% c("LB", "SB")) |
      # 5. Preceded by LB/SB, sufficiently spaced, no next
      (prev_category %in% c("LB", "SB") & days_after >= before_min & is.na(next_category)) |
      # 6-7. No previous, followed by LB or SB (use asymmetric spacing)
      (next_category == "LB" & days_before >= after_min_lb & is.na(prev_category)) |
      (next_category == "SB" & days_before >= after_min_sb & is.na(prev_category)) |
      # 8-9. Surrounded by LB/SB on both sides (use asymmetric spacing)
      (next_category == "LB" & days_before >= after_min_lb & prev_category %in% c("LB", "SB") & days_after >= before_min) |
      (next_category == "SB" & days_before >= after_min_sb & prev_category %in% c("LB", "SB") & days_after >= before_min)
    ) %>%
    ungroup()
  
  # Combine valid ECT with previous episodes
  result <- bind_rows(
    lb_sb_episodes,  # Use original LB+SB episodes
    valid_ect
  ) %>%
    select(-any_of(c("prev_category", "next_category", "days_after", "days_before"))) %>%
    arrange(person_id, outcome_date)
  
  return(result)
}

#' Add abortion episodes with comprehensive spacing validation
#' 
#' Abortions (AB/SA) are processed with the most restrictive spacing requirements:
#' 1. Must respect minimum spacing from ALL higher-priority outcomes (LB, SB, ECT)
#' 2. Different spacing requirements based on preceding outcome type  
#' 3. SA and AB treated identically for spacing but maintain separate codes
#' 4. Uses comprehensive lookup from matcho_outcome_limits for all combinations
#' 
#' The restrictive spacing prevents implausible rapid repeat pregnancies ending
#' in abortion, following Matcho et al. clinical evidence guidelines.
#' @noRd
add_abortion_episodes <- function(prev_episodes, ab_sa_episodes, matcho_outcome_limits) {
  
  if (nrow(ab_sa_episodes) == 0) {
    return(prev_episodes)
  }

  # Get minimum days for AB/SA spacing
  ab_after_lb <- matcho_outcome_limits %>%
    filter(first_preg_category == "LB", outcome_preg_category == "AB") %>%
    pull(min_days)
  
  ab_after_ect <- matcho_outcome_limits %>%
    filter(first_preg_category == "ECT", outcome_preg_category == "AB") %>%
    pull(min_days)
  
  lb_after_ab <- matcho_outcome_limits %>%
    filter(first_preg_category == "AB", outcome_preg_category == "LB") %>%
    pull(min_days)
  
  sb_after_ab <- matcho_outcome_limits %>%
    filter(first_preg_category == "AB", outcome_preg_category == "SB") %>%
    pull(min_days)
  
  ect_after_ab <- matcho_outcome_limits %>%
    filter(first_preg_category == "AB", outcome_preg_category == "ECT") %>%
    pull(min_days)
  
  # Apply spacing logic similar to stillbirths
  combined <- bind_rows(prev_episodes, ab_sa_episodes) %>%
    group_by(person_id) %>%
    arrange(outcome_date) %>%
    mutate(
      prev_category = lag(outcome_category),
      next_category = lead(outcome_category),
      days_after = as.numeric(as.Date(outcome_date) - as.Date(lag(outcome_date))),
      days_before = as.numeric(as.Date(lead(outcome_date)) - as.Date(outcome_date))
    )
  
  # Filter AB episodes that meet spacing requirements
  valid_ab <- combined %>%
    filter(
      outcome_category %in% c("AB", "SA"),
      (is.na(prev_category) & is.na(next_category)) |

      (!prev_category %in% c("LB", "SB", "ECT") & is.na(next_category)) |
      (!next_category %in% c("LB", "SB", "ECT") & is.na(prev_category)) |
      (!prev_category %in% c("LB", "SB", "ECT") & !next_category %in% c("LB", "SB", "ECT")) |

      # the last episode was a delivery and this one happens after the minimum
      (prev_category %in% c("LB", "SB") & days_after >= ab_after_lb & is.na(next_category)) |
      (next_category == "LB" & days_before >= lb_after_ab & is.na(prev_category)) |
      (next_category == "SB" & days_before >= sb_after_ab & is.na(prev_category)) |
      (next_category == "LB" & prev_category %in% c("LB", "SB") & days_before >= lb_after_ab & days_after >= ab_after_lb) |
      (next_category == "SB" & prev_category %in% c("LB", "SB") & days_before >= sb_after_ab & days_after >= ab_after_lb) |
      (prev_category == "ECT" & days_after >= ab_after_ect & is.na(next_category)) |
      (next_category == "ECT" & days_before >= ect_after_ab & is.na(prev_category)) |
      (next_category == "ECT" & prev_category == "ECT" & days_before >= ect_after_ab & days_after >= ab_after_ect) |
      (next_category == "ECT" & prev_category %in% c("LB", "SB") & days_before >= ect_after_ab & days_after >= ab_after_lb) |
      (next_category == "LB" & prev_category == "ECT" & days_before >= lb_after_ab & days_after >= ab_after_ect) |
      (next_category == "SB" & prev_category == "ECT" & days_before >= sb_after_ab & days_after >= ab_after_ect)
    ) %>%
    ungroup()
  
  # Combine valid AB with previous episodes
  result <- bind_rows(
    prev_episodes,  # Use original previous episodes
    valid_ab
  ) %>%
    select(-any_of(c("prev_category", "next_category", "days_after", "days_before"))) %>%
    arrange(person_id, outcome_date)
  
  return(result)
}




#' Add delivery-only episodes with backwards date modification capability
#' 
#' Delivery records have unique processing logic:
#' 1. Standard forward spacing validation like other outcome types
#' 2. SPECIAL FEATURE: Can modify existing LB/SB dates backwards in time
#' 3. If DELIV precedes LB/SB within minimum spacing, LB/SB date moves to DELIV date
#' 4. Handles cases where delivery procedure and birth outcome recorded separately
#' 
#' This backwards modification ensures capture of actual delivery timing when
#' administrative records split the delivery event across multiple entries.
#' @noRd
add_delivery_episodes <- function(prev_episodes, deliv_episodes, matcho_outcome_limits) {
  
  if (nrow(deliv_episodes) == 0) {
    return(prev_episodes)
  }

  # Get minimum days for DELIV
  deliv_after_lb <- matcho_outcome_limits %>%
    filter(first_preg_category == "LB", outcome_preg_category == "DELIV") %>%
    pull(min_days)
  
  deliv_after_ect <- matcho_outcome_limits %>%
    filter(first_preg_category == "ECT", outcome_preg_category == "DELIV") %>%
    pull(min_days)

  
  lb_after_deliv <- matcho_outcome_limits %>%
    filter(first_preg_category == "DELIV", outcome_preg_category == "LB") %>%
    pull(min_days)
  
  sb_after_deliv <- matcho_outcome_limits %>%
    filter(first_preg_category == "DELIV", outcome_preg_category == "SB") %>%
    pull(min_days)
  
  ect_after_deliv <- matcho_outcome_limits %>%
    filter(first_preg_category == "DELIV", outcome_preg_category == "ECT") %>%
    pull(min_days)
  
  # Apply spacing logic similar to stillbirths
  combined <- bind_rows(prev_episodes, deliv_episodes) %>%
    group_by(person_id) %>%
    arrange(outcome_date) %>%
    mutate(
      prev_category = lag(outcome_category),
      next_category = lead(outcome_category),
      days_after = as.numeric(as.Date(outcome_date) - as.Date(lag(outcome_date))),
      days_before = as.numeric(as.Date(lead(outcome_date)) - as.Date(outcome_date))
    )
  
  # CRITICAL: Move LB/SB dates backward to DELIV date when DELIV immediately precedes

  # them within the spacing window (matches original add_delivery() lines 356-364)
  # This handles the common case where delivery procedure is coded before birth outcome
  date_modified_records <- combined %>%
    mutate(
      outcome_date = as.Date(ifelse(
        !is.na(prev_category) &
          prev_category == "DELIV" &
          outcome_category %in% c("LB", "SB") &
          days_after < sb_after_deliv,
        as.Date(lag(outcome_date)),
        as.Date(outcome_date)
      ))
    ) %>%
    filter(outcome_category != "DELIV") %>%
    ungroup()

  # Filter DELIV episodes that meet spacing requirements
  # Note: original uses temp_category normalizing SA to AB, so checks against c("ECT", "AB")
  valid_deliv <- combined %>%
    filter(outcome_category == "DELIV") %>%
    filter(
      (is.na(days_after) & is.na(days_before)) |
        (!prev_category %in% c("LB", "SB", "ECT", "AB", "SA") & is.na(next_category)) |
        (!next_category %in% c("LB", "SB", "ECT", "AB", "SA") & is.na(prev_category)) |
        (!prev_category %in% c("LB", "SB", "ECT", "AB", "SA") & !next_category %in% c("LB", "SB", "ECT", "AB", "SA")) |
        # timing
        (prev_category %in% c("LB", "SB") & days_after >= deliv_after_lb & is.na(next_category)) |
        (next_category == "LB" & days_before >= lb_after_deliv & is.na(prev_category)) |
        (next_category == "SB" & days_before >= sb_after_deliv & is.na(prev_category)) |
        (next_category == "LB" & prev_category %in% c("LB", "SB") & days_before >= lb_after_deliv & days_after >= deliv_after_lb) |
        (next_category == "SB" & prev_category %in% c("LB", "SB") & days_before >= sb_after_deliv & days_after >= deliv_after_lb) |
        (prev_category %in% c("ECT", "AB", "SA") & days_after >= deliv_after_ect & is.na(next_category)) |
        (next_category %in% c("ECT", "AB", "SA") & days_before >= ect_after_deliv & is.na(prev_category)) |
        (next_category %in% c("ECT", "AB", "SA") & prev_category %in% c("ECT", "AB", "SA") & days_before >= ect_after_deliv & days_after >= deliv_after_ect) |
        (next_category %in% c("ECT", "AB", "SA") & prev_category %in% c("LB", "SB") & days_before >= ect_after_deliv & days_after >= deliv_after_lb) |
        (next_category == "LB" & prev_category %in% c("ECT", "AB", "SA") & days_before >= lb_after_deliv & days_after >= deliv_after_ect) |
        (next_category == "SB" & prev_category %in% c("ECT", "AB", "SA") & days_before >= sb_after_deliv & days_after >= deliv_after_ect)
    ) %>%
    ungroup()

  # Combine date-modified non-DELIV records with validated DELIV records
  # (matches original: union_all(add_abortion_df_rev, final_temp_df))
  result <- bind_rows(
    date_modified_records,
    valid_deliv
  ) %>%
    select(-any_of(c("prev_category", "next_category", "days_after", "days_before"))) %>%
    distinct() %>%
    arrange(person_id, outcome_date)

  return(result)
}

#' Add gestational age information following All of Us methodology
#'
#' Matches original add_gestation() approach: groups GA records into episodes
#' first (using gestation_episodes() logic), then matches gestation episodes
#' to outcome episodes using temporal overlap instead of simple lookback.
#'
#' Three groups result (matching original):
#' 1. Episodes with both outcome AND gestation data (overlapping)
#' 2. Outcome-only episodes (no matching gestation data)
#' 3. Gestation-only episodes (PREG category, no matching outcome)
#' @noRd
add_gestational_age_info <- function(episodes, all_records, matcho_limits = NULL) {

  # Get gestational age records
  gest_records <- all_records %>%
    filter(
      !is.na(gest_value) |
      category == "GEST" |
      concept_id %in% c(3002209, 3048230, 3012266, 3050433) |
      grepl("gestation", concept_name, ignore.case = TRUE)
    )

  if (nrow(gest_records) == 0) {
    episodes$has_gestational_info <- FALSE
    episodes$gestational_weeks <- NA_real_
    return(episodes)
  }

  # --- Step 1: Build gestation episodes (matching original gestation_episodes) ---
  gest_only <- gest_records %>%
    filter(
      !is.na(gest_value) | !is.na(value_as_number),
      coalesce(gest_value, value_as_number) > 0,
      coalesce(gest_value, value_as_number) <= 44
    ) %>%
    mutate(gest_weeks = coalesce(gest_value, value_as_number)) %>%
    # Keep max gest_value if two records share same date (original logic)
    group_by(person_id, event_date) %>%
    mutate(gest_week = max(gest_weeks)) %>%
    ungroup() %>%
    filter(gest_weeks == gest_week)

  if (nrow(gest_only) == 0) {
    episodes$has_gestational_info <- FALSE
    episodes$gestational_weeks <- NA_real_
    gestation_only_episodes <- identify_gestation_only_episodes(gest_records, episodes)
    if (nrow(gestation_only_episodes) > 0) {
      return(bind_rows(episodes, gestation_only_episodes))
    }
    return(episodes)
  }

  # Build episodes using corrected logic (same as identify_gestation_only_episodes)
  gest_episodes <- gest_only %>%
    group_by(person_id) %>%
    arrange(event_date) %>%
    mutate(
      prev_weeks = lag(gest_week),
      prev_date = lag(event_date),
      days_diff = as.numeric(event_date - prev_date),
      weeks_diff = gest_week - prev_weeks,
      adj_weeks_diff = case_when(
        is.na(prev_weeks) ~ NA_real_,
        weeks_diff <= 0 & days_diff < 70 ~ 1,
        TRUE ~ weeks_diff
      ),
      adj_weeks_diff2 = case_when(
        is.na(adj_weeks_diff) ~ NA_real_,
        adj_weeks_diff > 0 & days_diff >= (adj_weeks_diff * 7 + 28) ~ -1,
        TRUE ~ adj_weeks_diff
      ),
      new_episode = is.na(prev_weeks) | adj_weeks_diff2 <= 0,
      gest_episode = cumsum(new_episode)
    ) %>%
    ungroup()

  # --- Step 2: Summarize gestation episodes (matching get_min_max_gestation) ---
  gest_episode_summary <- gest_episodes %>%
    group_by(person_id, gest_episode) %>%
    summarise(
      max_gest_week = max(gest_week),
      min_gest_week = min(gest_week),
      max_gest_date = min(event_date[gest_week == max(gest_week)]),  # First occurrence of max week
      min_gest_date = min(event_date[gest_week == min(gest_week)]),  # First occurrence of min week
      end_gest_date = max(event_date),  # Last visit date
      n_gest_records = n(),
      .groups = "drop"
    ) %>%
    mutate(
      # Estimated start dates from gestation data
      max_gest_start_date = as.Date(max_gest_date - (max_gest_week * 7)),
      min_gest_start_date = as.Date(min_gest_date - (min_gest_week * 7)),
      # Ensure max_gest_start_date is always the earlier one (matching original)
      temp_max = pmin(max_gest_start_date, min_gest_start_date),
      min_gest_start_date = pmax(max_gest_start_date, min_gest_start_date),
      max_gest_start_date = temp_max
    ) %>%
    select(-temp_max)

  # --- Step 3: Match gestation episodes to outcome episodes using overlap ---
  # Estimate outcome episode windows for overlap matching using category-specific
  # max_term from matcho_limits (original uses category-specific max_start_date).
  # Fallback to 301 (LB max_term) for categories without term limits.
  episodes_for_match <- episodes %>%
    left_join(
      matcho_limits %>% select(category, max_term),
      by = c("outcome_category" = "category")
    ) %>%
    mutate(
      est_start = as.Date(outcome_date - coalesce(max_term, 301L))
    ) %>%
    select(-max_term)

  # Find overlapping gestation and outcome episodes
  # Original uses: overlaps(max_start_date, visit_date, max_gest_start_date, max_gest_date)
  both_matched <- episodes_for_match %>%
    inner_join(
      gest_episode_summary,
      by = "person_id",
      relationship = "many-to-many"
    ) %>%
    filter(
      # Temporal overlap: gestation episode overlaps with outcome episode window
      max_gest_start_date <= outcome_date,
      max_gest_date >= est_start
    ) %>%
    # Calculate days_diff (matching original: visit_date - max_gest_date)
    mutate(
      days_diff = as.numeric(outcome_date - max_gest_date)
    ) %>%
    # When multiple overlaps: keep best match per outcome (closest days_diff)
    group_by(person_id, episode_number) %>%
    slice_min(order_by = abs(days_diff), n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    # When multiple overlaps: keep best match per gestation episode too
    group_by(person_id, gest_episode) %>%
    slice_min(order_by = abs(days_diff), n = 1, with_ties = FALSE) %>%
    ungroup()

  # --- Step 4: Separate into three groups ---
  # Outcome episodes that matched gestation data
  matched_episode_keys <- both_matched %>%
    select(person_id, episode_number) %>%
    distinct()

  matched_gest_keys <- both_matched %>%
    select(person_id, gest_episode) %>%
    distinct()

  # Outcome-only episodes (no gestation match)
  outcome_only <- episodes %>%
    anti_join(matched_episode_keys, by = c("person_id", "episode_number")) %>%
    mutate(
      has_gestational_info = FALSE,
      gestational_weeks = NA_real_,
      n_gest_records = NA_integer_,
      max_gest_date = as.Date(NA)
    )

  # Episodes with both outcome and gestation
  outcome_with_gest <- both_matched %>%
    mutate(
      has_gestational_info = TRUE,
      gestational_weeks = max_gest_week,
      max_gest_date = as.Date(max_gest_date)
    ) %>%
    select(
      person_id, episode_number, outcome_date, outcome_category,
      has_gestational_info, gestational_weeks, n_gest_records, max_gest_date
    )

  # Gestation-only episodes (no matching outcome)
  gest_only_episodes <- gest_episode_summary %>%
    anti_join(matched_gest_keys, by = c("person_id", "gest_episode")) %>%
    mutate(
      outcome_date = as.Date(max_gest_date),  # Last actual data point
      outcome_category = "PREG",
      has_gestational_info = TRUE,
      gestational_weeks = max_gest_week,
      episode_number = NA_integer_,
      max_gest_date = as.Date(max_gest_date)
    ) %>%
    select(
      person_id, episode_number, outcome_date, outcome_category,
      has_gestational_info, gestational_weeks, n_gest_records, max_gest_date
    )

  # --- Step 5: Combine all three groups ---
  result <- bind_rows(
    outcome_with_gest,
    outcome_only,
    gest_only_episodes
  ) %>%
    mutate(
      gestational_weeks = ifelse(is.infinite(gestational_weeks), NA_real_, gestational_weeks)
    ) %>%
    arrange(person_id, outcome_date)

  return(result)
}

#' Calculate pregnancy start dates using hierarchical estimation approach
#' 
#' Start date calculation follows Matcho et al. methodology with preference order:
#' 1. PREFERRED: Gestational age-based calculation (outcome_date - gestational_weeks * 7)
#' 2. FALLBACK: Term duration-based calculation (outcome_date - max_term from category)
#' 3. DEFAULT: Standard pregnancy duration (outcome_date - 280 days)
#' 
#' Gestational age takes precedence when available as it provides the most
#' accurate pregnancy timeline. Term duration estimates vary by outcome category
#' based on Matcho et al. clinical evidence for typical pregnancy lengths.
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
          as.Date(outcome_date) - (gestational_weeks * 7),
        TRUE ~ as.Date(NA)
      ),
      
      # Otherwise use term limits
      term_based_start = case_when(
        !is.na(max_term) ~ as.Date(outcome_date) - max_term,
        TRUE ~ as.Date(outcome_date) - 280  # Default max pregnancy
      ),
      
      # Choose the best estimate
      episode_start_date = as.Date(coalesce(gest_based_start, term_based_start)),
      episode_end_date = as.Date(outcome_date),
      
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
      has_gestational_info,
      max_gest_date,
      min_term,
      max_term
    )

  return(result)
}

#' Identify gestation-only episodes following All of Us episode detection logic
#' 
#' Creates pregnancy episodes from gestational age records alone, using the same
#' episode boundary detection as the All of Us gestation_episodes() function:
#' 
#' NEW EPISODE TRIGGERS:
#' 1. Gestational age decreases from previous record (new pregnancy started)
#' 2. Time gap >70 days between records (minimum episode separation)  
#' 3. Time progression inconsistent with gestational progression (gap too large)
#' 
#' EPISODE VALIDATION:
#' - Buffer of 28 days added to expected gestational progression
#' - Episodes validated against outcome-based episodes to prevent double-counting
#' - Final outcome date estimated as last_gest_date + remaining pregnancy time
#' 
#' This captures pregnancies that appear only in gestational monitoring without
#' recorded outcomes, critical for comprehensive pregnancy identification.
#' @noRd
identify_gestation_only_episodes <- function(gest_records, existing_episodes) {
  
  if (nrow(gest_records) == 0) {
    return(data.frame())
  }
  
  # Get gestational age values from specific concepts
  gest_only <- gest_records %>%
    filter(
      !is.na(value_as_number) | !is.na(gest_value),
      # Valid gestational weeks (0-44)
      coalesce(gest_value, value_as_number) > 0,
      coalesce(gest_value, value_as_number) <= 44
    ) %>%
    mutate(
      gest_weeks = coalesce(gest_value, value_as_number)
    ) %>%
    select(person_id, gest_date = event_date, gest_weeks)
  
  if (nrow(gest_only) == 0) {
    return(data.frame())
  }
  
  # Group gestational records into potential episodes
  # Matches original gestation_episodes() logic with 70-day minimum gap check:
  # - GA decrease + date gap >= 70 days → new episode
  # - GA decrease + date gap < 70 days → recording error, NOT new episode
  # - GA increase but date gap > expected progression + 28-day buffer → new episode
  potential_episodes <- gest_only %>%
    group_by(person_id) %>%
    arrange(gest_date) %>%
    mutate(
      prev_weeks = lag(gest_weeks),
      prev_date = lag(gest_date),
      days_diff = as.numeric(gest_date - prev_date),
      weeks_diff = gest_weeks - prev_weeks,

      # Original logic: if GA decreases but gap < 70 days, treat as recording error
      # (set to positive so it doesn't trigger new episode)
      adj_weeks_diff = case_when(
        is.na(prev_weeks) ~ NA_real_,
        weeks_diff <= 0 & days_diff < 70 ~ 1,  # Not a new episode (recording error)
        TRUE ~ weeks_diff
      ),
      # If GA increases but gap exceeds expected progression + buffer, new episode
      adj_weeks_diff2 = case_when(
        is.na(adj_weeks_diff) ~ NA_real_,
        adj_weeks_diff > 0 & days_diff >= (adj_weeks_diff * 7 + 28) ~ -1,  # New episode
        TRUE ~ adj_weeks_diff
      ),

      # New episode if first record or adjusted diff indicates new pregnancy
      new_episode = is.na(prev_weeks) | adj_weeks_diff2 <= 0,

      episode_num = cumsum(new_episode)
    ) %>%
    group_by(person_id, episode_num) %>%
    summarise(
      last_gest_date = max(gest_date),
      max_gest_weeks = max(gest_weeks),
      n_gest_records = n(),
      .groups = "drop"
    )
  
  # Create gestation-only episodes
  # Let calculate_hip_start_dates handle date calculations to avoid redundancy
  gest_episodes <- potential_episodes %>%
    mutate(
      # End date: last actual gestation date (matching original visit_date = max_gest_date)
      # Original does NOT extrapolate to 40 weeks — uses last observed data point
      outcome_date = as.Date(last_gest_date),
      # Category is PREG for gestation-only
      outcome_category = "PREG",
      has_gestational_info = TRUE,
      gestational_weeks = max_gest_weeks,
      episode_number = NA_integer_  # Will be renumbered later
    )
  
  # Remove episodes that overlap with existing outcome-based episodes
  # Use temporal window overlap (not exact date match) to prevent double-counting.
  # A gestation-only episode is removed if its estimated pregnancy window
  # overlaps with any existing outcome episode's window.
  if (nrow(existing_episodes) > 0 && "outcome_date" %in% names(existing_episodes)) {
    # Estimate pregnancy window for each gestation-only episode
    gest_with_window <- gest_episodes %>%
      mutate(
        gest_est_start = as.Date(outcome_date - (max_gest_weeks * 7)),
        gest_est_end = as.Date(outcome_date)
      )

    # Existing episodes' windows
    existing_windows <- existing_episodes %>%
      transmute(
        person_id,
        exist_start = as.Date(outcome_date - 301),  # Conservative window
        exist_end = as.Date(outcome_date)
      )

    # Find gestation episodes that overlap with ANY existing episode
    overlapping <- gest_with_window %>%
      inner_join(existing_windows, by = "person_id", relationship = "many-to-many") %>%
      filter(
        gest_est_start <= exist_end & gest_est_end >= exist_start
      ) %>%
      distinct(person_id, last_gest_date) %>%
      mutate(is_overlap = TRUE)

    gest_episodes <- gest_episodes %>%
      left_join(overlapping, by = c("person_id", "last_gest_date")) %>%
      filter(is.na(is_overlap)) %>%
      select(-is_overlap)
  }
  
  # Return minimal structure - let existing pipeline handle the rest
  result <- gest_episodes %>%
    select(
      person_id,
      episode_number,
      outcome_date,
      outcome_category,
      has_gestational_info,
      gestational_weeks,
      n_gest_records
    )
  
  return(result)
}

#' Validate and clean pregnancy episodes following All of Us quality standards
#'
#' Matches original clean_episodes() logic: episodes failing category-specific
#' term duration validation are RECLASSIFIED to "PREG" (not deleted), preserving
#' pregnancy evidence. Uses Matcho et al. term durations per outcome category.
#'
#' VALIDATION RULES (from original clean_episodes):
#' 1. Over max term: gestational_age_days > category max_term → reclassify to PREG
#' 2. Under min term: gestational_age_days < category min_term → reclassify to PREG
#' 3. Basic temporal validity (no future dates, end >= start)
#'
#' OVERLAP HANDLING:
#' When episodes overlap after initial processing, start dates are adjusted
#' forward to prevent overlap while maintaining episode validity. Episodes
#' that become invalid after adjustment (duration <= 0) are removed.
#' @noRd
validate_hip_episodes <- function(episodes, buffer_days = 28L) {

  # --- Basic temporal validity (remove truly invalid records) ---
  validated <- episodes %>%
    filter(
      episode_start_date <= Sys.Date(),
      episode_end_date >= episode_start_date
    )

  # --- Category-specific term validation: reclassify to PREG, don't delete ---
  # Matches original clean_episodes() which reclassifies episodes outside
  # [min_term, max_term] range to "PREG" category.
  #
  # IMPORTANT: Original only reclassifies when BOTH gestation data and outcome
  # data exist (!is.na(gest_id) & !is.na(visit_id)). We gate on has_gestational_info
  # to match this behavior — outcome-only episodes are not reclassified based on
  # date-arithmetic GA alone.
  #
  # For under-min check, original also requires days_diff < -buffer_days (default 28)
  # providing tolerance for borderline cases.
  validated <- validated %>%
    mutate(
      removed_category = NA_character_,
      removed_outcome = 0L,

      # Over max term for category → reclassify to PREG
      # Gate: only when has_gestational_info (both gest + outcome data)
      # Original also sets visit_date = max_gest_date on reclassification
      over_max_reclassify = has_gestational_info & !is.na(max_term) &
        gestational_age_days > max_term,
      removed_category = case_when(
        over_max_reclassify ~ outcome_category,
        TRUE ~ removed_category
      ),
      removed_outcome = case_when(
        over_max_reclassify ~ 1L,
        TRUE ~ removed_outcome
      ),
      outcome_category = case_when(
        over_max_reclassify ~ "PREG",
        TRUE ~ outcome_category
      ),
      # Original: visit_date = max_gest_date (episode end moves to last GA date)
      episode_end_date = case_when(
        over_max_reclassify & !is.na(max_gest_date) ~ as.Date(max_gest_date),
        TRUE ~ as.Date(episode_end_date)
      ),

      # Under min term for category → reclassify to PREG
      # Gate: only when has_gestational_info AND last GA record extends >buffer_days
      # past the outcome date. Original: days_diff < -buffer_days where
      # days_diff = visit_date - max_gest_date. This checks that the GA data
      # significantly overshoots the outcome, indicating a pregnancy mismatch.
      days_diff = as.numeric(episode_end_date - max_gest_date),
      under_min_reclassify = has_gestational_info & !is.na(min_term) &
        gestational_age_days < min_term &
        !is.na(days_diff) & days_diff < -buffer_days &
        outcome_category != "PREG",

      removed_category = case_when(
        under_min_reclassify ~ outcome_category,
        TRUE ~ removed_category
      ),
      removed_outcome = case_when(
        under_min_reclassify ~ 1L,
        TRUE ~ removed_outcome
      ),
      outcome_category = case_when(
        under_min_reclassify ~ "PREG",
        TRUE ~ outcome_category
      ),
      # Original: visit_date = max_gest_date on under-min reclassification too
      episode_end_date = case_when(
        under_min_reclassify & !is.na(max_gest_date) ~ as.Date(max_gest_date),
        TRUE ~ as.Date(episode_end_date)
      )
    ) %>%
    select(-days_diff, -under_min_reclassify, -over_max_reclassify)

  # --- Overlap resolution (matches original remove_overlaps) ---

  # Step 1: Remove overlapping PREG episodes (lower confidence)
  # When a PREG episode overlaps with a subsequent outcome episode, remove the PREG
  validated <- validated %>%
    group_by(person_id) %>%
    arrange(episode_end_date) %>%
    mutate(
      prev_end_date = lag(episode_end_date),
      prev_category = lag(outcome_category),
      has_overlap = !is.na(prev_end_date) & episode_start_date <= prev_end_date
    ) %>%
    ungroup()

  # Identify PREG episodes to remove (previous episode is PREG and overlaps)
  preg_to_remove <- validated %>%
    filter(has_overlap & prev_category == "PREG") %>%
    mutate(remove_key = paste(person_id, prev_end_date, sep = "_"))

  if (nrow(preg_to_remove) > 0) {
    # Get the PREG episodes that overlap with subsequent outcome episodes
    validated <- validated %>%
      mutate(
        my_key = paste(person_id, episode_end_date, sep = "_"),
        is_removable_preg = my_key %in% preg_to_remove$remove_key & outcome_category == "PREG"
      ) %>%
      filter(!is_removable_preg) %>%
      select(-my_key, -is_removable_preg)
  }

  # Step 2: Use category-specific retry periods for remaining overlaps
  # Retry periods: LB/SB/DELIV = 28 days, ECT/AB/SA/PREG = 14 days
  validated <- validated %>%
    group_by(person_id) %>%
    arrange(episode_end_date) %>%
    mutate(
      prev_end_date = lag(episode_end_date),
      prev_category = lag(outcome_category),
      overlap_days = as.numeric(pmax(0, prev_end_date - episode_start_date + 1)),
      # Category-specific retry period (matching Matcho et al.)
      prev_retry = case_when(
        prev_category %in% c("LB", "SB", "DELIV") ~ 28L,
        prev_category %in% c("ECT", "AB", "SA", "PREG") ~ 14L,
        TRUE ~ 14L
      ),
      # Use retry period instead of simple prev_end + 1
      adjusted_start = case_when(
        !is.na(overlap_days) & overlap_days > 0 & !is.na(prev_retry) ~
          as.Date(prev_end_date + prev_retry),
        !is.na(overlap_days) & overlap_days > 0 ~ as.Date(prev_end_date + 1),
        TRUE ~ as.Date(episode_start_date)
      ),
      gestational_age_days = as.numeric(episode_end_date - adjusted_start)
    ) %>%
    filter(
      gestational_age_days > 0
    ) %>%
    ungroup() %>%
    mutate(
      episode_start_date = adjusted_start
    ) %>%
    select(-prev_end_date, -prev_category, -overlap_days, -adjusted_start,
           -prev_retry, -has_overlap)

  # Step 3: Post-adjustment validation (matching original remove_overlaps lines 929-941)
  # After overlap resolution adjusts start dates, some episodes may now fall outside
  # their category's term limits. Re-check and reclassify to PREG if needed.
  validated <- validated %>%
    mutate(
      gestational_age_days = as.numeric(episode_end_date - episode_start_date),
      # Reclassify if GA now under min_term after adjustment
      post_adj_reclassify = has_gestational_info & !is.na(min_term) &
        gestational_age_days < min_term &
        outcome_category != "PREG",
      removed_category = case_when(
        post_adj_reclassify ~ outcome_category,
        TRUE ~ removed_category
      ),
      removed_outcome = case_when(
        post_adj_reclassify ~ 1L,
        TRUE ~ removed_outcome
      ),
      outcome_category = case_when(
        post_adj_reclassify ~ "PREG",
        TRUE ~ outcome_category
      ),
      # Original: visit_date = max_gest_date on reclassification
      episode_end_date = case_when(
        post_adj_reclassify & !is.na(max_gest_date) ~ as.Date(max_gest_date),
        TRUE ~ as.Date(episode_end_date)
      )
    ) %>%
    select(-has_gestational_info, -max_gest_date, -min_term, -max_term, -post_adj_reclassify)

  return(validated)
}