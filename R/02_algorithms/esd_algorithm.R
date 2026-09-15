#' Calculate Estimated Start Dates (ESD Algorithm)
#'
#' Implements the Estimated Start Date algorithm from All of Us
#' to refine pregnancy start dates using gestational timing concepts.
#'
#' @param episodes Data frame of pregnancy episodes
#' @param cohort_data List containing extracted cohort data
#' @param pps_concepts Data frame with PPS concepts and timing
#'
#' @return Episodes with refined start dates and precision categories
#' @export

# =============================================================================
# ESD Timing Concept Lists (aligned with allofus-pregnancy)
# These concept lists capture gestational timing information from various OMOP
# domains. Concept names can be looked up in Athena: https://athena.ohdsi.org/
# =============================================================================

# Concept lists are those of Jones et al. (N3C) as carried into the All of Us
# reference implementation; all are standard OMOP concepts (verified against
# the N3C code, none are All of Us-specific).

# Gestational week (GW) concepts: classified as week-level evidence
ESD_GW_CONCEPTS <- c(3048230, 3002209, 3012266, 3050433)

# Gestational age concepts whose numeric value is read as weeks (0 < v < 44)
ESD_GA_VALUE_CONCEPTS <- c(3048230, 3002209, 3012266)

# Observation concepts that may contain gestational timing
ESD_OBSERVATION_CONCEPTS <- c(
  3011536, 3026070, 3024261, 4260747, 40758410, 3002549, 43054890,
  46234792, 4266763, 40485048, 3048230, 3002209, 3012266
)

# Measurement concepts that may contain gestational timing
ESD_MEASUREMENT_CONCEPTS <- c(3036844, 3048230, 3001105, 3002209, 3050433, 3012266)

# Estimated Date of Delivery (EDD) concepts
ESD_DELIVERY_DATE_CONCEPTS <- c(
  1175623, 3001105, 3011536, 3024261, 3024973, 3026070, 3036322,
  3038318, 3038608, 4059478, 4128833, 40490322, 40760182, 40760183, 42537958
)

# Estimated Date of Conception (EDC) concepts
ESD_CONCEPTION_DATE_CONCEPTS <- c(3002314, 3043737, 4058439, 4072438, 4089559, 44817092)

# Length of Gestation at Birth (LOG) concepts
ESD_GESTATION_LENGTH_CONCEPTS <- c(4260747, 43054890, 46234792, 4266763, 40485048)

# Combined list of all ESD timing concepts for extraction and filtering
ESD_ALL_TIMING_CONCEPTS <- unique(c(
  ESD_GW_CONCEPTS,
  ESD_OBSERVATION_CONCEPTS,
  ESD_MEASUREMENT_CONCEPTS,
  ESD_DELIVERY_DATE_CONCEPTS,
  ESD_CONCEPTION_DATE_CONCEPTS,
  ESD_GESTATION_LENGTH_CONCEPTS
))

calculate_estimated_start_dates <- function(episodes, cohort_data, pps_concepts) {
  
  if (nrow(episodes) == 0) {
    return(episodes)
  }
  
  # Ensure date columns are Date type (handle SQL Server numeric dates)
  # Use safe date parsing to handle various formats
  episodes <- episodes %>%
    mutate(
      across(any_of(c("episode_start_date", "episode_end_date", "outcome_date")), 
             ~as.Date(.x))
    )
  
  # Get all timing concepts
  timing_concepts <- get_timing_concepts(episodes, cohort_data, pps_concepts)
  
  if (nrow(timing_concepts) == 0) {
    # No timing data anywhere: leave the inferred start and precision NA;
    # add_episode_quality_metadata() applies the reference term-based fallback
    episodes$inferred_episode_start <- as.Date(NA)
    episodes$precision_days <- NA_real_
    episodes$precision_category <- NA_character_
    episodes$GW_flag <- 0L
    episodes$GR3m_flag <- 0L
    return(episodes)
  }
  
  # Process each episode
  # First, handle episodes with timing concepts
  # Need to preserve original episode dates through the join
  episodes_for_join <- episodes %>%
    select(person_id, episode_number, 
           orig_episode_start_date = episode_start_date,
           orig_episode_end_date = episode_end_date,
           everything())
  
  # Get total number of groups for progress tracking
  n_groups_with_timing <- episodes_for_join %>%
    inner_join(
      timing_concepts,
      by = c("person_id", "episode_number"),
      relationship = "many-to-many"
    ) %>%
    group_by(person_id, episode_number) %>%
    summarise(.groups = "drop") %>%
    nrow()
  
  if (n_groups_with_timing > 0) {
    message(sprintf("  Processing %d episodes with timing concepts...", n_groups_with_timing))
  }
  
  # OMOP OPTIMIZATION: Simplified progress tracking
  # Reports progress at intervals instead of per-episode
  current_group <- 0
  progress_interval <- max(1, n_groups_with_timing %/% 10)  # Show progress ~10 times
  
  # Pre-compute GW_flag and GR3m_flag per episode (matching original lines 439-441)
  episode_gt_flags <- timing_concepts %>%
    group_by(person_id, episode_number) %>%
    summarise(
      GW_flag = as.integer(any(GT_type == "GW", na.rm = TRUE)),
      GR3m_flag = as.integer(any(GT_type == "GR3m", na.rm = TRUE)),
      .groups = "drop"
    )

  episodes_with_timing <- episodes_for_join %>%
    inner_join(
      timing_concepts,
      by = c("person_id", "episode_number"),
      relationship = "many-to-many"
    ) %>%
    # Ensure we keep the original episode dates
    mutate(
      episode_start_date = orig_episode_start_date,
      episode_end_date = orig_episode_end_date
    ) %>%
    select(-orig_episode_start_date, -orig_episode_end_date) %>%
    group_by(person_id, episode_number) %>%
    group_modify(function(x, keys) {
      # Update progress
      current_group <<- current_group + 1

      # Show progress at intervals
      # All of Us shows progress per-episode, we show at intervals
      if (current_group %% progress_interval == 0 || current_group == n_groups_with_timing) {
        pct_complete <- round((current_group / n_groups_with_timing) * 100)
        message(sprintf("    %d%% complete", pct_complete))
      }

      # Call the original function
      calculate_episode_esd(x)
    }, .keep = TRUE) %>%
    ungroup() %>%
    # Attach GW_flag and GR3m_flag
    left_join(episode_gt_flags, by = c("person_id", "episode_number"))

  # Then, handle episodes without timing concepts
  episodes_without_timing <- episodes %>%
    anti_join(
      timing_concepts,
      by = c("person_id", "episode_number")
    ) %>%
    mutate(
      # Ensure date columns are Date type, using safe conversion
      episode_start_date = as.Date(episode_start_date),
      episode_end_date = as.Date(episode_end_date),
      inferred_episode_start = as.Date(NA),
      precision_days = NA_real_,
      precision_category = NA_character_,
      GW_flag = 0L,
      GR3m_flag = 0L
    )

  # Combine results
  episodes_with_esd <- bind_rows(
    episodes_with_timing,
    episodes_without_timing
  ) %>%
    # Ensure flags default to 0 (matching original lines 488-491)
    mutate(
      GW_flag = coalesce(GW_flag, 0L),
      GR3m_flag = coalesce(GR3m_flag, 0L)
    ) %>%
    arrange(person_id, episode_number)
  
  return(episodes_with_esd)
}

#' Get timing concepts for ESD calculation (All of Us aligned)
#'
#' Builds the ESD evidence per episode the way the reference does:
#' 1. Records come from cohort_data$esd_timing (concept-table driven
#'    extraction: names containing "gestation period" plus the fixed ESD
#'    lists and all PPS concepts). If that is absent, falls back to the HIP
#'    domain frames plus the PPS timing frame, deduplicated per record.
#' 2. Window: episode working start to recorded end, no padding.
#' 3. Week value: "Gestation period, N weeks" name, else the numeric value
#'    for the gestational age concepts when 0 < v < 44; truncated to integer.
#' 4. GT_type: GW if name contains "gestation period" or concept is in
#'    ESD_GW_CONCEPTS (dropped if no week value); else GR3m if the concept
#'    has PPS month bounds.
#' 5. Rollup: GW rows collapse to one per date (highest week), others to one
#'    per concept per date; ordered as the reference groups them.
#' @noRd
get_timing_concepts <- function(episodes, cohort_data, pps_concepts) {

  records <- get_esd_timing_records(cohort_data)
  if (is.null(records) || nrow(records) == 0) {
    return(data.frame())
  }

  # PPS month bounds (GR3m evidence)
  pps_months <- pps_concepts %>%
    select(concept_id, pps_min = min_month, pps_max = max_month) %>%
    distinct(concept_id, .keep_all = TRUE)
  records <- records %>%
    left_join(pps_months, by = "concept_id") %>%
    mutate(
      min_month = if ("min_month" %in% names(.)) coalesce(as.numeric(min_month), pps_min) else pps_min,
      max_month = if ("max_month" %in% names(.)) coalesce(as.numeric(max_month), pps_max) else pps_max
    ) %>%
    select(-pps_min, -pps_max)

  # Episode windows: [working start, recorded end], no padding (reference)
  episode_windows <- episodes %>%
    transmute(
      person_id,
      episode_number,
      window_start = as.Date(episode_start_date),
      window_end = if ("recorded_episode_end" %in% names(.)) {
        as.Date(coalesce(recorded_episode_end, episode_end_date))
      } else {
        as.Date(episode_end_date)
      }
    )

  episode_timing <- records %>%
    mutate(event_date = as.Date(event_date)) %>%
    inner_join(
      episode_windows,
      by = join_by(person_id, event_date >= window_start, event_date <= window_end)
    ) %>%
    select(-window_start, -window_end)

  if (nrow(episode_timing) == 0) {
    return(data.frame())
  }

  # Week value (reference domain_value): name first, then numeric value.
  # Reference reads value_as_string for observations and value_as_number for
  # measurements; a generic CDM may hold weeks in either, so take both.
  episode_timing <- episode_timing %>%
    mutate(
      concept_name = as.character(concept_name),
      name_lower = tolower(coalesce(concept_name, "")),
      value_from_name = suppressWarnings(as.numeric(
        ifelse(grepl("gestation period, *[0-9]+", name_lower),
               sub(".*gestation period, *([0-9]+).*", "\\1", name_lower), NA)
      )),
      value_from_string = suppressWarnings(as.numeric(
        ifelse(!is.na(value_as_string) & grepl("[0-9]", value_as_string),
               sub(".*?([0-9]+(\\.[0-9]+)?).*", "\\1", value_as_string), NA)
      )),
      domain_value = coalesce(value_from_name, as.numeric(value_as_number), value_from_string),
      domain_value = as.integer(domain_value),
      # Jones et al. (N3C) rule: a "Gestation period, N weeks" name, or a
      # numeric value on the gestational age concepts within (0, 44). The
      # All of Us port also accepts any "gestational age" name, which makes
      # the bound dead (those concepts are named "Gestational age"); we keep
      # the N3C bound so implausible values (e.g. 50 weeks) are excluded.
      keep_value = grepl("gestation period,", name_lower, fixed = TRUE) |
        (concept_id %in% ESD_GA_VALUE_CONCEPTS & !is.na(domain_value) &
           domain_value < 44 & domain_value > 0),
      gestational_weeks = if_else(keep_value, as.numeric(domain_value), NA_real_),
      implied_start_date = if_else(keep_value & !is.na(domain_value),
                                   event_date - domain_value * 7, as.Date(NA)),

      GT_type = case_when(
        grepl("gestation period", name_lower, fixed = TRUE) |
          concept_id %in% ESD_GW_CONCEPTS ~ "GW",
        !is.na(min_month) & !is.na(max_month) ~ "GR3m",
        TRUE ~ NA_character_
      ),
      GT_type = case_when(
        GT_type == "GW" & (is.na(domain_value) | is.na(implied_start_date)) ~ NA_character_,
        TRUE ~ GT_type
      ),

      # GR3m start-date range (reference uses 30.4 days per month)
      range_start = if_else(GT_type == "GR3m", event_date - round(max_month * 30.4), as.Date(NA)),
      range_end = if_else(GT_type == "GR3m", event_date - round(min_month * 30.4), as.Date(NA))
    ) %>%
    filter(GT_type %in% c("GW", "GR3m")) %>%
    mutate(
      implied_start_date = if_else(GT_type == "GW", implied_start_date, as.Date(NA)),
      gestational_weeks = if_else(GT_type == "GW", gestational_weeks, NA_real_),
      # Reference rollup: all GW rows share one label so one date keeps
      # only its highest week; other concepts are unique per concept/date
      concept_rollup = if_else(GT_type == "GW" & !is.na(domain_value),
                               "Gestation Week",
                               coalesce(concept_name, as.character(concept_id)))
    ) %>%
    arrange(person_id, episode_number, desc(domain_value)) %>%
    group_by(person_id, episode_number, concept_rollup, event_date, GT_type) %>%
    slice(1) %>%
    ungroup() %>%
    arrange(person_id, episode_number, concept_rollup, event_date, GT_type) %>%
    select(
      person_id, episode_number, concept_id, concept_name, event_date,
      GT_type, gestational_weeks, implied_start_date, range_start, range_end,
      min_month, max_month
    )

  episode_timing
}

#' Assemble ESD timing records from cohort data
#'
#' Prefers cohort_data$esd_timing (see extract_esd_timing_records). Falls
#' back to the HIP domain frames plus the PPS timing frame, deduplicated per
#' (person, concept, date) preferring the row that carries a concept name, so
#' a record is never classified twice.
#' @noRd
get_esd_timing_records <- function(cohort_data) {

  std_cols <- c("person_id", "concept_id", "concept_name", "event_date",
                "value_as_number", "value_as_string", "min_month", "max_month")

  normalize <- function(df) {
    if (is.null(df) || nrow(df) == 0) return(NULL)
    for (col in std_cols) if (!col %in% names(df)) df[[col]] <- NA
    df %>%
      transmute(
        person_id = as.integer(person_id),
        concept_id = as.integer(concept_id),
        concept_name = as.character(concept_name),
        event_date = as.Date(event_date),
        value_as_number = suppressWarnings(as.numeric(value_as_number)),
        value_as_string = as.character(value_as_string),
        min_month = suppressWarnings(as.numeric(min_month)),
        max_month = suppressWarnings(as.numeric(max_month))
      )
  }

  if (!is.null(cohort_data$esd_timing) && nrow(cohort_data$esd_timing) > 0) {
    return(normalize(cohort_data$esd_timing) %>% distinct())
  }

  fallback <- bind_rows(
    normalize(cohort_data$conditions),
    normalize(cohort_data$procedures),
    normalize(cohort_data$observations),
    normalize(cohort_data$measurements),
    normalize(cohort_data$gestational_timing)
  )
  if (is.null(fallback) || nrow(fallback) == 0) return(NULL)

  fallback %>%
    filter(!is.na(event_date)) %>%
    arrange(person_id, concept_id, event_date, is.na(concept_name), is.na(min_month)) %>%
    group_by(person_id, concept_id, event_date) %>%
    summarise(
      concept_name = first(concept_name),
      value_as_number = first(value_as_number),
      value_as_string = first(value_as_string),
      min_month = first(na.omit(min_month), default = NA_real_),
      max_month = first(na.omit(max_month), default = NA_real_),
      .groups = "drop"
    )
}

#' Calculate ESD for a single episode
#' @noRd
calculate_episode_esd <- function(episode_data) {
  
  # Handle empty data frame
  if (nrow(episode_data) == 0) {
    return(data.frame(
      inferred_episode_start = as.Date(NA),
      precision_category = NA_character_,
      precision_days = NA_real_
    ))
  }

  # If no usable timing data (neither week estimates nor ranges), return NA
  has_gw <- "implied_start_date" %in% names(episode_data) &&
    any(!is.na(episode_data$implied_start_date))
  has_gr3m <- "range_start" %in% names(episode_data) &&
    any(!is.na(episode_data$range_start))
  if (!has_gw && !has_gr3m) {

    # Get first row and clean up any timing columns that might exist
    result <- episode_data[1, ] %>%
      select(-any_of(c("implied_start_date", "gestational_weeks",
                       "range_start", "range_end", "event_date", "concept_id",
                       "concept_name", "category", "gest_value",
                       "value_as_number", "value_as_string",
                       "min_month", "max_month",
                       "person_id", "episode_number"))) %>%  # Remove grouping columns since .keep = TRUE
      mutate(
        inferred_episode_start = as.Date(NA),
        precision_category = NA_character_,
        precision_days = NA_real_
      )
    return(result)
  }
  
  # Separate week-level and range concepts
  week_concepts <- episode_data %>%
    filter(!is.na(implied_start_date), is.na(range_start))
  
  range_concepts <- episode_data %>%
    filter(!is.na(range_start))
  
  # Find intersection of timing estimates
  timing_result <- find_timing_intersection(week_concepts, range_concepts)
  
  # Get the original episode columns (first row has the episode info)
  original_episode <- episode_data[1, ]
  
  # Episode dates must be present - they come from the original episodes
  if (!"episode_end_date" %in% names(original_episode) || 
      !"episode_start_date" %in% names(original_episode)) {
    # This should never happen - episodes must have dates
    # Return with default precision indicating error
    warning("Episode dates missing in calculate_episode_esd - this indicates a data flow error")
    result <- original_episode[1, ] %>%
      select(-any_of(c("implied_start_date", "gestational_weeks", 
                       "range_start", "range_end", "event_date", "concept_id",
                       "concept_name", "category", "gest_value",
                       "value_as_number", "value_as_string",
                       "min_month", "max_month",
                       "person_id", "episode_number"))) %>%
      mutate(
        inferred_episode_start = as.Date(NA),
        precision_category = NA_character_,
        precision_days = NA_real_
      )
    return(result)
  }

  # Attach the inferred start and precision. episode_start_date is left as
  # the working start; add_episode_quality_metadata() finalizes dates.
  result <- original_episode %>%
    select(-any_of(c("implied_start_date", "gestational_weeks",
                     "range_start", "range_end", "event_date", "concept_id",
                     "concept_name", "category", "gest_value",
                     "value_as_number", "value_as_string",
                     "min_month", "max_month",
                     "person_id", "episode_number"))) %>%  # Remove grouping columns since .keep = TRUE
    mutate(
      episode_start_date = as.Date(episode_start_date),
      episode_end_date = as.Date(episode_end_date),
      inferred_episode_start = as.Date(timing_result$inferred_start_date),
      precision_days = as.numeric(timing_result$precision_days),
      precision_category = assign_precision_category(precision_days),
      intervalsCount = as.integer(timing_result$intervalsCount),
      majorityOverlapCount = as.integer(timing_result$majorityOverlapCount)
    )

  return(result)
}

#' Combine timing estimates for one episode (reference get_gt_timing)
#'
#' Direct port of the All of Us get_gt_timing():
#' - GR3m ranges -> findIntersection(); if the intersection is narrower than
#'   7 days it is widened to midpoint +/- 3 days before overlap testing.
#' - With week (GW) estimates: if a GR3m intersection exists and more than
#'   50% of the raw GW dates fall inside it, keep those, remove outliers and
#'   use the first (earliest) as the start with precision = their spread;
#'   otherwise use all GW dates after outlier removal, and -1 ("week,
#'   poor support") if only one survives.
#' - GR3m only: start = midpoint of the intersection, precision = span of
#'   the union of surviving ranges.
#' Week dates are expected in date order (get_timing_concepts guarantees it).
#' @noRd
find_timing_intersection <- function(week_concepts, range_concepts) {

  result <- list(
    inferred_start_date = as.Date(NA),
    precision_days = NA_real_,
    precision_category = NA_character_,
    intervalsCount = 0L,
    majorityOverlapCount = 0L
  )

  gw_dates <- as.Date(week_concepts$implied_start_date[
    week_concepts$GT_type == "GW" & !is.na(week_concepts$implied_start_date)
  ])

  gr3m <- range_concepts[!is.na(range_concepts$range_start) & !is.na(range_concepts$range_end), , drop = FALSE]
  ranges <- lapply(seq_len(nrow(gr3m)), function(i) {
    c(as.Date(gr3m$range_start[i]), as.Date(gr3m$range_end[i]))
  })

  interval_s <- NULL
  interval_e <- NULL
  midpoint <- NULL
  max_range_days <- 0

  if (length(ranges) > 0) {
    ci <- findIntersection(ranges)
    range_e <- as.Date(ci[1])
    range_s <- as.Date(ci[2])
    interval_e <- as.Date(ci[3])
    interval_s <- as.Date(ci[4])
    plausible_days <- as.numeric(interval_e - interval_s)
    max_range_days <- as.numeric(range_e - range_s)
    midpoint <- interval_s + as.integer(plausible_days / 2)
    if (plausible_days < 7) {
      interval_s <- midpoint - 3
      interval_e <- midpoint + 3
    }
  }

  if (length(gw_dates) > 0) {
    if (!is.null(interval_s)) {
      result$intervalsCount <- 1L
      overlapping <- gw_dates[gw_dates >= interval_s & gw_dates <= interval_e]
      if ((length(overlapping) / length(gw_dates)) * 100 > 50) {
        result$majorityOverlapCount <- 1L
        filt <- remove_GW_outliers(list(overlapping))
        result$inferred_start_date <- filt[1]
        result$precision_days <- as.numeric(max(filt) - min(filt))
      } else {
        filt <- remove_GW_outliers(list(gw_dates))
        result$inferred_start_date <- filt[1]
        result$precision_days <- as.numeric(max(filt) - min(filt))
        if (length(filt) == 1) result$precision_days <- -1
      }
    } else {
      filt <- remove_GW_outliers(list(gw_dates))
      result$inferred_start_date <- filt[1]
      result$precision_days <- as.numeric(max(filt) - min(filt))
      if (length(filt) == 1) result$precision_days <- -1
    }
  } else if (!is.null(midpoint)) {
    result$inferred_start_date <- midpoint
    result$precision_days <- max_range_days
  }

  result$precision_category <- assign_precision_category(result$precision_days)
  result
}

#' Assign precision category based on days
#' 
#' Categories from All of Us indicating confidence in pregnancy dating:
#' - week: ≤7 days (highest precision)
#' - two-week: 8-14 days
#' - three-week: 15-21 days  
#' - month: 22-28 days
#' - two-month: 29-56 days
#' - three-month: 57-84 days
#' - non-specific: >84 days or no timing data (lowest precision)
#' @noRd
assign_precision_category <- function(precision_days) {
  case_when(
    is.na(precision_days) ~ NA_character_,
    precision_days == -1 ~ "week_poor-support",
    precision_days >= 0 & precision_days <= 7 ~ "week",
    precision_days > 7 & precision_days <= 14 ~ "two-week",
    precision_days > 14 & precision_days <= 21 ~ "three-week",
    precision_days > 21 & precision_days <= 28 ~ "month",
    precision_days > 28 & precision_days <= 56 ~ "two-month",
    precision_days > 56 & precision_days <= 84 ~ "three-month",
    TRUE ~ "non-specific"
  )
}

#' Find intersection of date ranges (reference findIntersection)
#'
#' Direct port. Counts pairwise overlaps, removes outlier ranges by the
#' IQR*1.5 rule on overlap counts, orders survivors by overlap count, then
#' narrows the intersection sequentially, only accepting a bound that stays
#' inside the current intersection (so it is never empty). If no range
#' survives the filter, the range with the most overlaps is used.
#' Returns c(last, first, min_start, max_start): union end, union start,
#' intersection end, intersection start.
#' @noRd
findIntersection <- function(intervals) {
  if (length(intervals) == 0) {
    return(NULL)
  }

  intervals_df <- data.frame(
    V1 = do.call(c, lapply(intervals, function(x) as.Date(x[1]))),
    V2 = do.call(c, lapply(intervals, function(x) as.Date(x[2])))
  ) %>%
    arrange(V1)

  n <- nrow(intervals_df)
  overlapCount <- rep(0, n)
  for (j in seq_len(n)) {
    for (m in seq_len(n)) {
      if (j != m) {
        last <- intervals_df$V2[j]
        first <- intervals_df$V1[j]
        if ((intervals_df$V1[m] == last) || (intervals_df$V1[m] == first)) {
          overlapCount[j] <- overlapCount[j] + 1
        } else if ((intervals_df$V2[m] == last) || (intervals_df$V2[m] == first)) {
          overlapCount[j] <- overlapCount[j] + 1
        } else if ((intervals_df$V2[m] < last) && (intervals_df$V2[m] > first)) {
          overlapCount[j] <- overlapCount[j] + 1
        } else if ((intervals_df$V1[m] < last) && (intervals_df$V1[m] > first)) {
          overlapCount[j] <- overlapCount[j] + 1
        }
      }
    }
  }
  intervals_df$overlapCount <- overlapCount

  q1 <- quantile(overlapCount, 0.25)
  q3 <- quantile(overlapCount, 0.75)
  outlierMetric <- (q3 - q1) * 1.5
  outlierThreshold <- abs(q1 - outlierMetric)
  if (outlierThreshold == 0) {
    filtered <- intervals_df[intervals_df$overlapCount > outlierThreshold, , drop = FALSE]
  } else {
    filtered <- intervals_df[intervals_df$overlapCount >= outlierThreshold, , drop = FALSE]
  }
  filtered <- filtered[order(filtered$overlapCount, decreasing = TRUE), , drop = FALSE]

  N <- nrow(filtered)
  if (N == 1) {
    last <- filtered$V2[1]; min_start <- filtered$V2[1]
    first <- filtered$V1[1]; max_start <- filtered$V1[1]
  } else if (N == 0) {
    sorted <- intervals_df[order(overlapCount, decreasing = TRUE), , drop = FALSE]
    last <- sorted$V2[1]; min_start <- sorted$V2[1]
    first <- sorted$V1[1]; max_start <- sorted$V1[1]
  } else {
    last <- filtered$V2[1]; min_start <- filtered$V2[1]
    first <- filtered$V1[1]; max_start <- filtered$V1[1]
    for (i in 2:N) {
      if (filtered$V1[i] < first) first <- filtered$V1[i]
      if (filtered$V2[i] > last) last <- filtered$V2[i]
      if ((filtered$V2[i] < min_start) && (filtered$V2[i] > max_start)) min_start <- filtered$V2[i]
      if ((filtered$V1[i] > max_start) && (filtered$V1[i] < min_start)) max_start <- filtered$V1[i]
    }
  }

  c(last, first, min_start, max_start)
}

#' Remove outliers from GW concepts (All of Us algorithm)
#' 
#' Specific outlier removal for gestational week concepts using
#' distance from median approach with IQR * 1.5 threshold.
#' More stringent than general outlier removal due to higher
#' expected precision of gestational week measurements.
#' @noRd
remove_GW_outliers <- function(gw_concepts_list) {
  if (length(gw_concepts_list) == 0 || length(gw_concepts_list[[1]]) == 0) {
    return(c())
  }
  
  # Flatten list and convert to dates using safe conversion
  gw_dates <- as.Date(unlist(gw_concepts_list))
  
  if (length(gw_dates) <= 1) {
    return(gw_dates)
  }
  
  # Find median using ceiling index (matching original: sort()[ceiling(length/2)])
  # For even-length arrays this selects the lower-middle element
  median_date <- sort(gw_dates)[ceiling(length(gw_dates) / 2)]

  # Calculate distances from median (matching original's abs approach)
  distances <- numeric(length(gw_dates))
  for (j in seq_along(gw_dates)) {
    distances[j] <- as.numeric(max(gw_dates[j], median_date) - min(gw_dates[j], median_date))
  }

  # Remove outliers using IQR (matching original exactly)
  q1 <- quantile(distances, 0.25, na.rm = TRUE)
  q3 <- quantile(distances, 0.75, na.rm = TRUE)
  outlierMetric <- (q3 - q1) * 1.5
  lower_threshold <- q1 - outlierMetric
  upper_threshold <- q3 + outlierMetric

  # Filter dates within thresholds
  filtered_dates <- gw_dates[distances >= lower_threshold & distances <= upper_threshold]
  
  if (length(filtered_dates) == 0) {
    return(gw_dates[1])  # Return at least one date
  }
  
  return(filtered_dates)
}
