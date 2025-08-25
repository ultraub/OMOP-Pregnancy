#' Matcho Outcome Limits and Term Durations
#'
#' Tables from Matcho et al. defining minimum days between pregnancy outcomes
#' and expected term durations for each outcome type.
#'
#' @return List containing outcome_limits and term_durations data frames
#' @export
get_matcho_tables <- function() {
  
  # Minimum days between pregnancy outcomes (from Matcho et al.)
  outcome_limits <- data.frame(
    first_preg_category = c(
      "LB", "LB", "LB", "LB", "LB", "LB",
      "SB", "SB", "SB", "SB", "SB", "SB",
      "ECT", "ECT", "ECT", "ECT", "ECT", "ECT",
      "AB", "AB", "AB", "AB", "AB", "AB",
      "SA", "SA", "SA", "SA", "SA", "SA",
      "DELIV", "DELIV", "DELIV", "DELIV", "DELIV", "DELIV"
    ),
    outcome_preg_category = c(
      "LB", "SB", "ECT", "AB", "SA", "DELIV",
      "LB", "SB", "ECT", "AB", "SA", "DELIV",
      "LB", "SB", "ECT", "AB", "SA", "DELIV",
      "LB", "SB", "ECT", "AB", "SA", "DELIV",
      "LB", "SB", "ECT", "AB", "SA", "DELIV",
      "LB", "SB", "ECT", "AB", "SA", "DELIV"
    ),
    min_days = c(
      # From LB (Live Birth)
      182, 168, 70, 70, 70, 168,
      # From SB (Still Birth)
      182, 70, 70, 70, 70, 168,
      # From ECT (Ectopic)
      168, 154, 56, 56, 56, 154,
      # From AB (Abortion)
      168, 154, 56, 56, 56, 154,
      # From SA (Spontaneous Abortion)
      168, 154, 56, 56, 56, 154,
      # From DELIV (Delivery)
      182, 168, 70, 70, 70, 168
    ),
    stringsAsFactors = FALSE
  )
  
  # Term durations for each outcome type (in days)
  term_durations <- data.frame(
    category = c("LB", "SB", "ECT", "AB", "SA", "DELIV", "PREG"),
    min_term = c(
      161,  # LB: 23 weeks
      140,  # SB: 20 weeks
      42,   # ECT: 4 weeks
      42,   # AB: 3 weeks
      28,   # SA: 3 weeks
      140,  # DELIV: 20 weeks
      30     # PREG: 4 weeks
    ),
    max_term = c(
      301,  # LB: 43 weeks
      301,  # SB: 43 weeks
      84,  # ECT: 18 weeks
      168,  # AB: 20 weeks
      139,  # SA: 20 weeks
      301,  # DELIV: 43 weeks
      301   # PREG: 43 weeks (default max)
    ),
    retry = c(28, 28, 14, 14, 14, 28, 14),
    stringsAsFactors = FALSE
  )
  
  return(list(
    outcome_limits = outcome_limits,
    term_durations = term_durations
  ))
}

#' Get minimum days between outcomes
#'
#' @param first_outcome First pregnancy outcome category
#' @param second_outcome Second pregnancy outcome category
#' @return Minimum days required between outcomes
#' @export
get_min_days_between <- function(first_outcome, second_outcome) {
  tables <- get_matcho_tables()
  
  min_days <- tables$outcome_limits %>%
    filter(
      first_preg_category == first_outcome,
      outcome_preg_category == second_outcome
    ) %>%
    pull(min_days)
  
  if (length(min_days) == 0) {
    return(28)  # Default minimum
                # Did not find the default days
  }
  
  return(min_days[1])
}

#' Get term duration for outcome
#'
#' @param outcome_category Pregnancy outcome category
#' @return List with min_term and max_term in days
#' @export
get_term_duration <- function(outcome_category) {
  tables <- get_matcho_tables()
  
  duration <- tables$term_durations %>%
    filter(category == outcome_category)
  
  if (nrow(duration) == 0) {
    # Default for unknown outcomes
    return(list(min_term = 0, max_term = 301))
  }
  
  return(list(
    min_term = duration$min_term[1],
    max_term = duration$max_term[1]
  ))
}

#' Validate gestational age for outcome
#'
#' @param gestational_age_days Gestational age in days
#' @param outcome_category Pregnancy outcome category
#' @return TRUE if gestational age is valid for outcome
#' @export
validate_gestational_age <- function(gestational_age_days, outcome_category) {
  duration <- get_term_duration(outcome_category)
  
  return(
    gestational_age_days >= duration$min_term &&
    gestational_age_days <= duration$max_term
  )
}