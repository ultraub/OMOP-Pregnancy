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
  episodes_with_gest <- add_gestational_age_info(final_episodes, all_records)
  
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
      outcome_date = max(as.Date(event_date)),
      outcome_category = first(category),
      n_visits = n(),
      .groups = "drop"
    ) %>%
    ungroup() %>%
    mutate(
      outcome_date = as.Date(outcome_date)
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
      days_after = as.numeric(as.Date(outcome_date) - as.Date(lag(outcome_date))),
      days_before = as.numeric(as.Date(lead(outcome_date)) - as.Date(outcome_date))
    )
  
  # Filter ECT episodes that meet spacing requirements
  valid_ect <- combined %>%
    filter(
      outcome_category == "ECT",
      (is.na(prev_category) & is.na(next_category)) |
      (is.na(prev_category) | days_after >= before_min) |
      (is.na(next_category) | days_before >= after_min) |

      # the previous category was ectopic and there's no next category
      # or some configuration
      (!prev_category %in% c("LB", "SB") & is.na(next_category)) |
      (!next_category %in% c("LB", "SB") & is.na(prev_category)) |
      (!prev_category %in% c("LB", "SB") & !next_category %in% c("LB", "SB")) |
      # the last episode was a delivery and this one happens after the minimum
      (prev_category %in% c("LB", "SB") & days_after >= ect_after_lb & is.na(next_category)) |
      # there was no previous category and the next live birth happens after the minimum
      (next_category == "LB" & days_before >= lb_after_ect & is.na(prev_category)) |
      (next_category == "SB" & days_before >= sb_after_ect & is.na(prev_category)) |
      # surrounded by each, appropriately spaced
      (next_category == "LB" & days_before >= lb_after_ect & prev_category %in% c("LB", "SB") & days_after >= ect_after_lb) |
      (next_category == "SB" & days_before >= sb_after_ect & prev_category %in% c("LB", "SB") & days_after >= ect_after_lb)
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
  
  # Filter DELIV episodes that meet spacing requirements
  valid_deliv <- combined %>%
    filter(
      outcome_category == "DELIV",
      (is.na(prev_category) & is.na(next_category)) |

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
  
  # Combine valid DELIV with previous episodes
  result <- bind_rows(
    prev_episodes,  # Use original previous episodes
    valid_deliv
  ) %>%
    select(-any_of(c("prev_category", "next_category", "days_after", "days_before"))) %>%
    arrange(person_id, outcome_date)
  
  return(result)
}

#' Add gestational age information following All of Us methodology
#' 
#' Integrates gestational age data from multiple concept types:
#' - Explicit gestational age concepts (3002209, 3048230, 3012266, 3050433)
#' - Value_as_number fields containing gestational weeks
#' - PREG category records with timing information
#' 
#' Gestational records are linked to episodes within biologically plausible
#' windows (up to 280 days before outcome). Multiple gestational records per
#' episode are consolidated using maximum gestational age approach.
#' 
#' CRITICAL: Also identifies gestation-only episodes (pregnancies identified
#' only through gestational age records without specific outcomes).
#' @noRd
add_gestational_age_info <- function(episodes, all_records) {
  
  # Get gestational age records
  # Including concept 3050433 - Gestational age in weeks Calculated
  gest_records <- all_records %>%
    filter(
      !is.na(gest_value) | 
      category == "GEST" |
      concept_id %in% c(3002209, 3048230, 3012266, 3050433) |  # Specific gestational age concepts
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
  
  # NEW: Identify gestation-only episodes (pregnancies with only gestational timing, no outcomes)
  gestation_only_episodes <- identify_gestation_only_episodes(gest_records, result)
  
  # Combine outcome-based episodes with gestation-only episodes
  if (nrow(gestation_only_episodes) > 0) {
    result <- bind_rows(result, gestation_only_episodes)
  }
  
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
      has_gestational_info
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
  # A new episode starts when gestational age decreases or there's a large time gap
  potential_episodes <- gest_only %>%
    group_by(person_id) %>%
    arrange(gest_date) %>%
    mutate(
      prev_weeks = lag(gest_weeks),
      prev_date = lag(gest_date),
      days_diff = as.numeric(gest_date - prev_date),
      weeks_diff = gest_weeks - prev_weeks,
      
      # New episode if: gestational age decreases OR large time gap (>70 days)
      new_episode = is.na(prev_weeks) | 
                    weeks_diff <= 0 | 
                    days_diff > 70 |
                    # Or if time progression doesn't match gestational progression
                    (weeks_diff > 0 & days_diff > (weeks_diff * 7 + 28)),
      
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
      # End date: last gestation date plus remaining pregnancy time
      outcome_date = as.Date(last_gest_date + ((40 - max_gest_weeks) * 7)),
      # Category is PREG for gestation-only
      outcome_category = "PREG",
      has_gestational_info = TRUE,
      gestational_weeks = max_gest_weeks,
      episode_number = NA_integer_  # Will be renumbered later
    )
  
  # Remove episodes that overlap with existing outcome-based episodes
  # Check based on outcome dates to prevent double-counting
  if (nrow(existing_episodes) > 0 && "outcome_date" %in% names(existing_episodes)) {
    gest_episodes <- gest_episodes %>%
      anti_join(
        existing_episodes %>%
          select(person_id, outcome_date),
        by = join_by(
          person_id,
          outcome_date == outcome_date
        )
      )
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
#' QUALITY FILTERS:
#' 1. Gestational age plausibility (0-320 days, ~45 weeks maximum)
#' 2. Temporal validity (no future start dates, end >= start)  
#' 3. Overlap resolution (adjust start dates to prevent episode overlap)
#' 
#' OVERLAP HANDLING:
#' When episodes overlap after initial processing, start dates are adjusted
#' forward to prevent overlap while maintaining episode validity. Episodes
#' that become invalid after adjustment (duration <= 0) are removed.
#' 
#' This final validation ensures all returned episodes meet clinical plausibility
#' standards and maintain temporal consistency for downstream analysis.
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
        !is.na(overlap_days) & overlap_days > 0 ~ as.Date(prev_end_date + 1),
        TRUE ~ as.Date(episode_start_date)
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