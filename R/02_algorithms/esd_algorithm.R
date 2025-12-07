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

# Gestational Week (GW) concepts - used for GW classification in precision scoring
ESD_GW_CONCEPTS <- c(3048230, 3002209, 3012266, 3050433)

# Observation concepts that may contain gestational timing
ESD_OBSERVATION_CONCEPTS <- c(
  3011536, 3026070, 3024261, 4260747, 40758410,
  3002549, 43054890, 46234792, 4266763, 40485048
)

# Measurement concepts that may contain gestational timing
ESD_MEASUREMENT_CONCEPTS <- c(3036844, 3001105)

# Estimated Date of Delivery (EDD) concepts
ESD_DELIVERY_DATE_CONCEPTS <- c(
  1175623, 3001105, 3011536, 3024261, 3024973, 3026070, 3036322,
  3038318, 3038608, 4059478, 4128833, 40490322, 40760182, 40760183, 42537958
)

# Estimated Date of Conception (EDC) concepts
ESD_CONCEPTION_DATE_CONCEPTS <- c(3002314, 3043737, 4058439, 4072438, 4089559, 44817092)

# Length of Gestation at Birth (LOG) concepts
ESD_GESTATION_LENGTH_CONCEPTS <- c(4260747, 43054890, 46234792, 4266763, 40485048)

# Combined list of all ESD timing concepts for filtering
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
    # No timing data, return episodes with default precision
    episodes$precision_category <- "non-specific"
    episodes$precision_days <- 999
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
    ungroup()
  
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
      precision_category = "non-specific",
      precision_days = 999
    )
  
  # Combine results
  episodes_with_esd <- bind_rows(
    episodes_with_timing,
    episodes_without_timing
  ) %>%
    arrange(person_id, episode_number)
  
  return(episodes_with_esd)
}

#' Get timing concepts for ESD calculation (All of Us aligned)
#' @noRd
get_timing_concepts <- function(episodes, cohort_data, pps_concepts) {

  # Get timing concept IDs to filter for
  timing_concept_ids <- pps_concepts$concept_id

  # ESD timing concepts are defined at module level (ESD_ALL_TIMING_CONCEPTS)
  # to allow reuse across functions in this file

  # OMOP OPTIMIZATION: Single-pass filtering for database performance
  # Unlike All of Us which filters each domain separately, we combine domains first
  # to reduce database round-trips while maintaining identical results
  
  # Cache column existence checks to avoid SQL errors on missing columns
  # All of Us assumes all columns exist, but OHDSI CDMs may vary in their
  # measurement/observation value columns (value_as_number, value_as_string, etc.).
  # This defensive programming ensures compatibility across diverse CDM implementations
  col_cache <- list()
  
  # Combine all domain data into single dataframe for efficient filtering
  all_domain_data <- bind_rows(
    if (!is.null(cohort_data$conditions) && nrow(cohort_data$conditions) > 0) 
      cohort_data$conditions %>% mutate(domain_source = "condition") else NULL,
    if (!is.null(cohort_data$procedures) && nrow(cohort_data$procedures) > 0) 
      cohort_data$procedures %>% mutate(domain_source = "procedure") else NULL,
    if (!is.null(cohort_data$observations) && nrow(cohort_data$observations) > 0) 
      cohort_data$observations %>% mutate(domain_source = "observation") else NULL,
    if (!is.null(cohort_data$measurements) && nrow(cohort_data$measurements) > 0) 
      cohort_data$measurements %>% mutate(domain_source = "measurement") else NULL
  )
  
  # Check which columns exist in the combined data
  if (!is.null(all_domain_data) && nrow(all_domain_data) > 0) {
    col_cache$has_concept_name <- "concept_name" %in% names(all_domain_data)
    col_cache$has_gest_value <- "gest_value" %in% names(all_domain_data)
    col_cache$has_category <- "category" %in% names(all_domain_data)
    col_cache$has_value_as_number <- "value_as_number" %in% names(all_domain_data)
    
    # Filter for timing concepts - aligned with All of Us implementation
    # Captures concepts from multiple sources:
    #   1. PPS timing concept IDs (from pps_concepts.csv)
    #   2. ESD timing concepts (hardcoded lists above - EDD, EDC, LOG, GW)
    #   3. Pattern matching on "gestation" in concept name
    #   4. GEST category from HIP concepts
    #   5. Records with gest_value populated
    timing_from_domains <- all_domain_data %>%
      filter(
        concept_id %in% timing_concept_ids |
        concept_id %in% ESD_ALL_TIMING_CONCEPTS |
        (col_cache$has_concept_name & !is.na(concept_name) & grepl("gestation", concept_name, ignore.case = TRUE)) |
        (col_cache$has_category & !is.na(category) & category == "GEST") |
        (col_cache$has_gest_value & !is.na(gest_value))
      ) %>%
      select(-domain_source)  # Remove temporary column
  } else {
    timing_from_domains <- NULL
  }
  
  # Handle gestational_timing separately since it's already timing-specific
  gestational_timing_normalized <- NULL
  if (!is.null(cohort_data$gestational_timing) && nrow(cohort_data$gestational_timing) > 0) {
    gestational_timing_normalized <- cohort_data$gestational_timing %>%
      mutate(
        concept_name = if (!"concept_name" %in% names(.)) NA_character_ else concept_name,
        category = "GEST",
        gest_value = if (!"gest_value" %in% names(.)) NA_real_ else gest_value,
        value_as_number = if (!"value_as_number" %in% names(.)) NA_real_ else value_as_number,
        value_as_string = if (!"value_as_string" %in% names(.)) NA_character_ else value_as_string
      ) %>%
      select(any_of(c("person_id", "concept_id", "event_date", "concept_name", 
                      "category", "gest_value", "value_as_number", "value_as_string",
                      "min_month", "max_month")))
  }
  
  # Combine the filtered timing records
  timing_records <- bind_rows(
    timing_from_domains,
    gestational_timing_normalized
  )
  
  if (is.null(timing_records) || nrow(timing_records) == 0) {
    return(data.frame())
  }
  
  # Add compute step if this is a lazy tbl (like All of Us's aou_compute())
  if ("tbl_lazy" %in% class(timing_records) || "tbl_sql" %in% class(timing_records)) {
    # Source database utilities
    source("R/03_utilities/database_utils.R")
    
    # Compute to temp table for better performance
    message("    Computing filtered timing records to temp table...")
    timing_records <- omop_compute(timing_records)
  }
  
  # Use cached column checks or check now if not already cached
  if (length(col_cache) == 0) {
    col_cache$has_concept_name <- "concept_name" %in% names(timing_records)
    col_cache$has_gest_value <- "gest_value" %in% names(timing_records)
    col_cache$has_value_as_number <- "value_as_number" %in% names(timing_records)
  }
  
  # Join with PPS concepts to get min_month and max_month (if not already present)
  if (!"min_month" %in% names(timing_records) || !"max_month" %in% names(timing_records)) {
    timing_records <- timing_records %>%
      left_join(
        pps_concepts %>% select(concept_id, min_month, max_month),
        by = "concept_id"
      )
  } else {
    # If min_month/max_month already exist from gestational_timing, use coalesce to fill gaps
    timing_records <- timing_records %>%
      left_join(
        pps_concepts %>% select(concept_id, pps_min = min_month, pps_max = max_month),
        by = "concept_id"
      ) %>%
      mutate(
        min_month = coalesce(min_month, pps_min),
        max_month = coalesce(max_month, pps_max)
      ) %>%
      select(-pps_min, -pps_max)
  }
  
  # Prepare episode windows for efficient joining
  episode_windows <- episodes %>%
    select(person_id, episode_number, episode_start_date, episode_end_date) %>%
    mutate(
      episode_start_date = as.Date(episode_start_date),
      episode_end_date = as.Date(episode_end_date),
      window_start = episode_start_date - 30,
      window_end = episode_end_date + 30
    )
  
  # Use inner join with date conditions (more efficient than left join + filter)
  # This follows the All of Us pattern of filtering during the join but uses
  # modern dplyr join_by syntax for cleaner date range conditions.
  # Window of ±30 days captures timing concepts near episode boundaries
  episode_timing <- timing_records %>%
    mutate(event_date = as.Date(event_date)) %>%
    inner_join(
      episode_windows,
      by = join_by(
        person_id,
        event_date >= window_start,
        event_date <= window_end
      )
    ) %>%
    select(-window_start, -window_end)
  
  # Classify timing concepts as GW (gestational week) or GR3m (3-month range)
  # This follows All of Us logic for precision category assignment:
  # - GW concepts: specific gestational week with high precision
  # - GR3m concepts: broader 3-month ranges with lower precision
  # Handles missing columns gracefully for OHDSI CDM variations
  episode_timing <- episode_timing %>%
    mutate(
      # Determine timing type (GW vs GR3m)
      GT_type = case_when(
        # GW concepts: specific gestational week concepts
        # All of Us uses str_detect on "gestation period," - we check column exists first
        col_cache$has_concept_name & !is.na(concept_name) & 
          grepl("gestation period,", concept_name, ignore.case = TRUE) ~ "GW",
        col_cache$has_concept_name & !is.na(concept_name) & 
          grepl("gestational age", concept_name, ignore.case = TRUE) ~ "GW",
        # Specific concept IDs known to be gestational week concepts (module-level constant)
        concept_id %in% ESD_GW_CONCEPTS ~ "GW",
        # If we have a gest_value, it's a GW concept
        col_cache$has_gest_value & !is.na(gest_value) ~ "GW",
        # GR3m concepts: range-based concepts from PPS with min/max months
        !is.na(min_month) & !is.na(max_month) ~ "GR3m",
        TRUE ~ NA_character_
      )
    )
  
  # Extract gestational weeks for GW concepts following All of Us priority:
  # 1. gest_value field (most reliable if available)
  # 2. value_as_number field (if reasonable <50 weeks)
  # 3. Text extraction from concept_name (e.g., "Gestation period, 20 weeks")
  # Column availability checked first to prevent SQL errors in diverse CDMs
  if (col_cache$has_gest_value || col_cache$has_value_as_number || col_cache$has_concept_name) {
    episode_timing <- episode_timing %>%
      mutate(
        gestational_weeks = case_when(
          # Priority 1: Use gest_value if available (most reliable)
          GT_type == "GW" & col_cache$has_gest_value & !is.na(gest_value) ~ gest_value,
          # Priority 2: Use value_as_number if reasonable (< 50 weeks)
          GT_type == "GW" & col_cache$has_value_as_number & !is.na(value_as_number) & 
            value_as_number < 50 ~ value_as_number,
          # Priority 3: Extract from concept_name text (e.g., "Gestation period, 20 weeks")
          GT_type == "GW" & col_cache$has_concept_name & !is.na(concept_name) & 
            grepl("\\d+ weeks?", concept_name) ~ 
            suppressWarnings(as.numeric(gsub(".*?(\\d+) weeks?.*", "\\1", concept_name))),
          TRUE ~ NA_real_
        )
      )
  } else {
    # No columns available for extracting gestational weeks
    episode_timing <- episode_timing %>%
      mutate(gestational_weeks = NA_real_)
  }
  
  # Calculate implied dates
  episode_timing <- episode_timing %>%
    mutate(
      # Calculate implied start date for GW concepts
      implied_start_date = case_when(
        GT_type == "GW" & !is.na(gestational_weeks) ~ as.Date(event_date) - (gestational_weeks * 7),
        TRUE ~ as.Date(NA)
      ),
      
      # Calculate range for GR3m concepts
      range_start = case_when(
        GT_type == "GR3m" & !is.na(max_month) ~ as.Date(event_date) - (max_month * DAYS_PER_MONTH),
        TRUE ~ as.Date(NA)
      ),
      range_end = case_when(
        GT_type == "GR3m" & !is.na(min_month) ~ as.Date(event_date) - (min_month * DAYS_PER_MONTH),
        TRUE ~ as.Date(NA)
      )
    ) %>%
    # Filter out invalid concepts
    filter(
      (GT_type == "GW" & !is.na(implied_start_date)) |
      (GT_type == "GR3m" & !is.na(range_start) & !is.na(range_end)) |
      (!is.na(GT_type))  # Keep any valid GT_type even if dates are missing
    )
  
  return(episode_timing)
}

#' Calculate ESD for a single episode
#' @noRd
calculate_episode_esd <- function(episode_data) {
  
  # Handle empty data frame
  if (nrow(episode_data) == 0) {
    return(data.frame(
      precision_category = "non-specific",
      precision_days = 999
    ))
  }
  
  # If no timing data, return original with default precision
  if (!"implied_start_date" %in% names(episode_data) ||
      all(is.na(episode_data$implied_start_date))) {
    
    # Get first row and clean up any timing columns that might exist
    result <- episode_data[1, ] %>%
      select(-any_of(c("implied_start_date", "gestational_weeks", 
                       "range_start", "range_end", "event_date", "concept_id",
                       "concept_name", "category", "gest_value",
                       "value_as_number", "value_as_string",
                       "min_month", "max_month",
                       "person_id", "episode_number"))) %>%  # Remove grouping columns since .keep = TRUE
      mutate(
        precision_category = "non-specific",
        precision_days = 999
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
        precision_category = "non-specific",
        precision_days = 999
      )
    return(result)
  }
  
  # Update episode with refined start date
  result <- original_episode %>%
    select(-any_of(c("implied_start_date", "gestational_weeks",
                     "range_start", "range_end", "event_date", "concept_id",
                     "concept_name", "category", "gest_value", 
                     "value_as_number", "value_as_string",
                     "min_month", "max_month",
                     "person_id", "episode_number"))) %>%  # Remove grouping columns since .keep = TRUE
    mutate(
      # Ensure dates are Date type using safe conversion
      episode_start_date = as.Date(coalesce(
        timing_result$inferred_start_date,
        episode_start_date
      )),
      episode_end_date = as.Date(episode_end_date),
      
      # Add precision information
      precision_days = timing_result$precision_days,
      precision_category = assign_precision_category(timing_result$precision_days),
      
      # Recalculate gestational age with new start
      gestational_age_days = as.numeric(as.Date(episode_end_date) - as.Date(episode_start_date))
    )
  
  return(result)
}

#' Find intersection of timing estimates (All of Us algorithm)
#' 
#' Implements the exact All of Us logic for finding consensus between
#' multiple timing estimates using IQR-based outlier removal and
#' intersection of date ranges for optimal precision categorization.
#' @noRd
find_timing_intersection <- function(week_concepts, range_concepts) {
  
  # Default result
  result <- list(
    inferred_start_date = as.Date(NA),
    precision_days = 999,
    precision_category = "non-specific"
  )
  
  # Separate GW and GR3m concepts
  gw_concepts <- week_concepts %>% filter(GT_type == "GW")
  gr3m_concepts <- rbind(
    week_concepts %>% filter(GT_type == "GR3m"),
    range_concepts
  )
  
  # Process GR3m concepts first if available
  gr3m_intersection <- NULL
  if (nrow(gr3m_concepts) > 0) {
    # Build list of ranges for intersection
    ranges_list <- list()
    for (i in 1:nrow(gr3m_concepts)) {
      if (!is.na(gr3m_concepts$range_start[i]) && !is.na(gr3m_concepts$range_end[i])) {
        ranges_list[[length(ranges_list) + 1]] <- c(
          gr3m_concepts$range_start[i],
          gr3m_concepts$range_end[i]
        )
      }
    }
    
    if (length(ranges_list) > 0) {
      # Find intersection using All of Us logic
      gr3m_intersection <- findIntersection(ranges_list)
    }
  }
  
  # Process GW concepts
  if (nrow(gw_concepts) > 0) {
    # Remove outliers from GW concepts
    gw_dates <- gw_concepts$implied_start_date
    gw_dates_clean <- remove_GW_outliers(list(gw_dates))
    
    if (length(gw_dates_clean) > 0) {
      # Check overlap with GR3m intersection if available
      if (!is.null(gr3m_intersection)) {
        # Extract intersection boundaries
        interval_start <- as.Date(gr3m_intersection[4])  # max_start
        interval_end <- as.Date(gr3m_intersection[3])    # min_start
        
        # Filter GW concepts that overlap with GR3m intersection
        overlapping_gw <- gw_dates_clean[
          gw_dates_clean >= interval_start & gw_dates_clean <= interval_end
        ]
        
        # If >50% overlap, use overlapping GW concepts
        if (length(overlapping_gw) / length(gw_dates_clean) > 0.5) {
          result$inferred_start_date <- median(overlapping_gw, na.rm = TRUE)
          result$precision_days <- as.numeric(max(overlapping_gw) - min(overlapping_gw))
        } else {
          # Use all GW concepts
          result$inferred_start_date <- median(gw_dates_clean, na.rm = TRUE)
          result$precision_days <- as.numeric(max(gw_dates_clean) - min(gw_dates_clean))
        }
      } else {
        # No GR3m intersection, use GW concepts alone
        result$inferred_start_date <- median(gw_dates_clean, na.rm = TRUE)
        if (length(gw_dates_clean) > 1) {
          result$precision_days <- as.numeric(max(gw_dates_clean) - min(gw_dates_clean))
        } else {
          result$precision_days <- -1  # Single GW concept (poor support)
        }
      }
    }
  } else if (!is.null(gr3m_intersection)) {
    # Only GR3m concepts available
    interval_start <- as.Date(gr3m_intersection[4])
    interval_end <- as.Date(gr3m_intersection[3])
    
    # Use midpoint of intersection
    result$inferred_start_date <- interval_start + 
                                  as.integer((interval_end - interval_start) / 2)
    result$precision_days <- as.numeric(interval_end - interval_start)
  }
  
  # Assign precision category
  result$precision_category <- assign_precision_category(result$precision_days)
  
  return(result)
}

#' Remove outliers from dates using IQR method
#' 
#' Implements standard IQR * 1.5 outlier detection as used in All of Us
#' to filter implausible gestational timing estimates.
#' @noRd
remove_date_outliers <- function(dates) {
  
  if (length(dates) <= 2) {
    return(dates)
  }
  
  # Convert to numeric for calculation
  dates_numeric <- as.numeric(dates)
  
  # Calculate IQR
  q1 <- quantile(dates_numeric, 0.25, na.rm = TRUE)
  q3 <- quantile(dates_numeric, 0.75, na.rm = TRUE)
  iqr <- q3 - q1
  
  # Define outlier thresholds
  lower_bound <- q1 - 1.5 * iqr
  upper_bound <- q3 + 1.5 * iqr
  
  # Filter outliers
  clean_dates <- dates[dates_numeric >= lower_bound & dates_numeric <= upper_bound]
  
  return(clean_dates)
}

#' Find intersection of date ranges
#' @noRd
find_range_intersection <- function(range_concepts) {
  
  # Get all ranges
  ranges <- range_concepts %>%
    select(range_start, range_end) %>%
    filter(!is.na(range_start), !is.na(range_end))
  
  if (nrow(ranges) == 0) {
    return(list(midpoint = as.Date(NA), range_days = 999))
  }
  
  # Find intersection
  intersection_start <- max(ranges$range_start, na.rm = TRUE)
  intersection_end <- min(ranges$range_end, na.rm = TRUE)
  
  if (intersection_start <= intersection_end) {
    # Valid intersection
    midpoint <- intersection_start + 
                as.integer((intersection_end - intersection_start) / 2)
    range_days <- as.numeric(intersection_end - intersection_start)
    
    return(list(midpoint = midpoint, range_days = range_days))
  } else {
    # No intersection, use median of all midpoints
    midpoints <- ranges %>%
      mutate(midpoint = range_start + as.integer((range_end - range_start) / 2)) %>%
      pull(midpoint)
    
    return(list(
      midpoint = median(midpoints, na.rm = TRUE),
      range_days = 999
    ))
  }
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
    is.na(precision_days) | precision_days == 999 ~ "non-specific",
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

#' Find intersection of date ranges (All of Us algorithm)
#' 
#' Direct implementation of All of Us findIntersection function that:
#' 1. Removes outlier ranges via IQR * 1.5 on overlap counts
#' 2. Finds the intersection of remaining ranges
#' 3. Returns boundaries for precision calculation
#' Critical for accurate pregnancy dating from multiple timing sources.
#' @noRd
findIntersection <- function(intervals) {
  if (length(intervals) == 0) {
    return(NULL)
  }
  
  # Convert to data frame
  if (length(intervals) == 1) {
    intervals_df <- data.frame(
      V1 = as.Date(intervals[[1]][1]),
      V2 = as.Date(intervals[[1]][2])
    )
  } else {
    intervals_df <- do.call(rbind, lapply(intervals, function(x) {
      data.frame(V1 = as.Date(x[1]), V2 = as.Date(x[2]))
    }))
  }
  
  intervals_df <- intervals_df %>%
    arrange(V1)
  
  # Remove outliers based on overlap count
  n <- nrow(intervals_df)
  overlapCount <- numeric(n)
  
  for (i in 1:n) {
    for (j in 1:n) {
      if (i != j) {
        # Check for overlap
        if (intervals_df$V1[j] <= intervals_df$V2[i] && 
            intervals_df$V2[j] >= intervals_df$V1[i]) {
          overlapCount[i] <- overlapCount[i] + 1
        }
      }
    }
  }
  
  # Remove outliers using IQR
  if (length(overlapCount) > 1) {
    q1 <- quantile(overlapCount, 0.25)
    q3 <- quantile(overlapCount, 0.75)
    iqr <- q3 - q1
    outlierThreshold <- max(0, q1 - 1.5 * iqr)
    
    filtered <- intervals_df[overlapCount >= outlierThreshold, ]
  } else {
    filtered <- intervals_df
  }
  
  if (nrow(filtered) == 0) {
    filtered <- intervals_df[1, , drop = FALSE]
  }
  
  # Find intersection
  if (nrow(filtered) == 1) {
    return(c(
      filtered$V2[1],  # last day
      filtered$V1[1],  # first day
      filtered$V2[1],  # min_start (end of intersection)
      filtered$V1[1]   # max_start (start of intersection)
    ))
  }
  
  # Multiple intervals - find overlapping region
  first <- min(filtered$V1)
  last <- max(filtered$V2)
  max_start <- max(filtered$V1)  # Latest start = beginning of intersection
  min_start <- min(filtered$V2)  # Earliest end = end of intersection
  
  # Ensure valid intersection
  if (max_start > min_start) {
    # No valid intersection, use first interval
    return(c(
      filtered$V2[1],
      filtered$V1[1],
      filtered$V2[1],
      filtered$V1[1]
    ))
  }
  
  return(c(last, first, min_start, max_start))
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
  
  # Find median date
  median_date <- median(gw_dates, na.rm = TRUE)
  
  # Calculate distances from median
  distances <- as.numeric(abs(gw_dates - median_date))
  
  # Remove outliers using IQR
  q1 <- quantile(distances, 0.25, na.rm = TRUE)
  q3 <- quantile(distances, 0.75, na.rm = TRUE)
  iqr <- q3 - q1
  
  lower_threshold <- q1 - 1.5 * iqr
  upper_threshold <- q3 + 1.5 * iqr
  
  # Filter dates within thresholds
  filtered_dates <- gw_dates[distances >= lower_threshold & distances <= upper_threshold]
  
  if (length(filtered_dates) == 0) {
    return(gw_dates[1])  # Return at least one date
  }
  
  return(filtered_dates)
}

#' Apply ESD refinement to episodes
#' @export
refine_episode_dates <- function(episodes, cohort_data, pps_concepts) {
  
  # Check if we have timing data
  has_timing <- any(
    !is.null(cohort_data$gestational_timing),
    any(grepl("gest", names(cohort_data$observations)), ignore.case = TRUE),
    any(grepl("gest", names(cohort_data$measurements)), ignore.case = TRUE)
  )
  
  if (!has_timing) {
    # No timing data available
    episodes$precision_category <- "non-specific"
    episodes$precision_days <- 999
    return(episodes)
  }
  
  # Apply ESD algorithm
  refined_episodes <- calculate_estimated_start_dates(
    episodes,
    cohort_data,
    pps_concepts
  )
  
  # Validate refined dates
  validated_episodes <- refined_episodes %>%
    mutate(
      # Ensure dates are reasonable
      episode_start_date = case_when(
        episode_start_date > episode_end_date ~ episode_end_date - 280,
        episode_start_date < episode_end_date - 320 ~ episode_end_date - 280,
        TRUE ~ episode_start_date
      ),
      
      # Recalculate gestational age
      gestational_age_days = as.numeric(episode_end_date - episode_start_date)
    )
  
  return(validated_episodes)
}