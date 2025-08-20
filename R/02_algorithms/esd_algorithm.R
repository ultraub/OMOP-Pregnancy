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
calculate_estimated_start_dates <- function(episodes, cohort_data, pps_concepts) {
  
  if (nrow(episodes) == 0) {
    return(episodes)
  }
  
  # Ensure date columns are Date type (handle SQL Server numeric dates)
  # Use safe date parsing to handle various formats
  episodes <- episodes %>%
    mutate(
      across(any_of(c("episode_start_date", "episode_end_date", "outcome_date")), 
             ~safe_as_date(.x))
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
  episodes_with_timing <- episodes %>%
    inner_join(
      timing_concepts,
      by = c("person_id", "episode_number"),
      relationship = "many-to-many"
    ) %>%
    group_by(person_id, episode_number) %>%
    group_modify(~calculate_episode_esd(.x), .keep = TRUE) %>%
    ungroup()
  
  # Then, handle episodes without timing concepts
  episodes_without_timing <- episodes %>%
    anti_join(
      timing_concepts,
      by = c("person_id", "episode_number")
    ) %>%
    mutate(
      # Ensure date columns are Date type, using safe conversion
      episode_start_date = safe_as_date(episode_start_date),
      episode_end_date = safe_as_date(episode_end_date),
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
  
  # Prepare gestational_timing data with normalized columns
  gestational_timing_normalized <- NULL
  if (!is.null(cohort_data$gestational_timing) && nrow(cohort_data$gestational_timing) > 0) {
    # Normalize gestational_timing to match other domain structures
    gestational_timing_normalized <- cohort_data$gestational_timing %>%
      mutate(
        # Add missing columns that other domains have
        concept_name = NA_character_,
        category = "GEST",  # Mark these as gestational timing
        gest_value = NA_real_,
        # Ensure we have value columns
        value_as_number = if ("value_as_number" %in% names(.)) value_as_number else NA_real_,
        value_as_string = if ("value_as_string" %in% names(.)) value_as_string else NA_character_
      ) %>%
      # Keep the important timing columns
      select(any_of(c("person_id", "concept_id", "event_date", "concept_name", 
                      "category", "gest_value", "value_as_number", "value_as_string",
                      "min_month", "max_month")))
  }
  
  # Combine all records that might have gestational timing
  all_records <- bind_rows(
    cohort_data$conditions,
    cohort_data$procedures,
    cohort_data$observations,
    cohort_data$measurements,
    gestational_timing_normalized  # Include the gestational timing data!
  )
  
  # Check which columns exist
  has_concept_name <- "concept_name" %in% names(all_records)
  has_gest_value <- "gest_value" %in% names(all_records)
  has_value_as_number <- "value_as_number" %in% names(all_records)
  
  # Filter to gestational timing concepts
  timing_records <- all_records
  
  if (has_concept_name || has_gest_value) {
    timing_records <- timing_records %>%
      filter(
        # Gestational age concepts
        (has_concept_name & grepl("gestation", concept_name, ignore.case = TRUE)) |
        concept_id %in% pps_concepts$concept_id |
        category == "GEST" |
        (has_gest_value & !is.na(gest_value))
      )
  } else {
    # Fallback to just concept_id and category
    timing_records <- timing_records %>%
      filter(
        concept_id %in% pps_concepts$concept_id |
        category == "GEST"
      )
  }
  
  if (nrow(timing_records) == 0) {
    return(data.frame())
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
  
  # Join with episodes to get relevant timing for each episode
  episode_timing <- episodes %>%
    select(person_id, episode_number, episode_start_date, episode_end_date) %>%
    mutate(
      # Ensure episode dates are properly converted
      episode_start_date = safe_as_date(episode_start_date),
      episode_end_date = safe_as_date(episode_end_date)
    ) %>%
    left_join(
      timing_records %>%
        mutate(event_date = safe_as_date(event_date)),
      by = "person_id",
      relationship = "many-to-many"
    ) %>%
    filter(
      # Timing must be within episode window (with some buffer)
      event_date >= episode_start_date - 30,
      event_date <= episode_end_date + 30
    )
  
  # Build GT_type classification based on available columns
  episode_timing <- episode_timing %>%
    mutate(
      # Determine timing type (GW vs GR3m)
      GT_type = case_when(
        # GW concepts: specific gestational week concepts
        has_concept_name & grepl("gestation period,", concept_name, ignore.case = TRUE) ~ "GW",
        has_concept_name & grepl("gestational age", concept_name, ignore.case = TRUE) ~ "GW",
        concept_id %in% c(3048230, 3002209, 3012266, 3050433) ~ "GW",
        has_gest_value & !is.na(gest_value) ~ "GW",
        # GR3m concepts: range-based concepts from PPS
        !is.na(min_month) & !is.na(max_month) ~ "GR3m",
        TRUE ~ NA_character_
      )
    )
  
  # Extract gestational weeks for GW concepts
  if (has_gest_value || has_value_as_number || has_concept_name) {
    episode_timing <- episode_timing %>%
      mutate(
        gestational_weeks = case_when(
          GT_type == "GW" & has_gest_value & !is.na(gest_value) ~ gest_value,
          GT_type == "GW" & has_value_as_number & !is.na(value_as_number) & value_as_number < 50 ~ value_as_number,
          GT_type == "GW" & has_concept_name & grepl("\\d+ weeks?", concept_name) ~ suppressWarnings(
            as.numeric(gsub(".*?(\\d+) weeks?.*", "\\1", concept_name))
          ),
          TRUE ~ NA_real_
        )
      )
  } else {
    episode_timing <- episode_timing %>%
      mutate(gestational_weeks = NA_real_)
  }
  
  # Calculate implied dates
  episode_timing <- episode_timing %>%
    mutate(
      # Calculate implied start date for GW concepts
      implied_start_date = case_when(
        GT_type == "GW" & !is.na(gestational_weeks) ~ safe_as_date(event_date) - (gestational_weeks * 7),
        TRUE ~ as.Date(NA)
      ),
      
      # Calculate range for GR3m concepts
      range_start = case_when(
        GT_type == "GR3m" & !is.na(max_month) ~ safe_as_date(event_date) - (max_month * 30.4),
        TRUE ~ as.Date(NA)
      ),
      range_end = case_when(
        GT_type == "GR3m" & !is.na(min_month) ~ safe_as_date(event_date) - (min_month * 30.4),
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
  
  # Ensure we have required date columns
  if (!"episode_end_date" %in% names(original_episode)) {
    if ("episode_start_date" %in% names(original_episode)) {
      # If we have start but not end, estimate end as start + 280 days
      original_episode$episode_end_date <- safe_as_date(original_episode$episode_start_date) + 280
    } else {
      # No dates at all, use current date as placeholder
      original_episode$episode_end_date <- Sys.Date()
      original_episode$episode_start_date <- Sys.Date() - 280
    }
  }
  
  if (!"episode_start_date" %in% names(original_episode)) {
    # If we have end but not start, estimate start as end - 280 days
    original_episode$episode_start_date <- safe_as_date(original_episode$episode_end_date) - 280
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
      episode_start_date = safe_as_date(coalesce(
        timing_result$inferred_start_date,
        episode_start_date
      )),
      episode_end_date = safe_as_date(episode_end_date),
      
      # Add precision information
      precision_days = timing_result$precision_days,
      precision_category = assign_precision_category(timing_result$precision_days),
      
      # Recalculate gestational age with new start
      gestational_age_days = as.numeric(safe_as_date(episode_end_date) - safe_as_date(episode_start_date))
    )
  
  return(result)
}

#' Find intersection of timing estimates (All of Us aligned)
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
        interval_start <- safe_as_date(gr3m_intersection[4])  # max_start
        interval_end <- safe_as_date(gr3m_intersection[3])    # min_start
        
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
    interval_start <- safe_as_date(gr3m_intersection[4])
    interval_end <- safe_as_date(gr3m_intersection[3])
    
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
#' @noRd
findIntersection <- function(intervals) {
  if (length(intervals) == 0) {
    return(NULL)
  }
  
  # Convert to data frame
  if (length(intervals) == 1) {
    intervals_df <- data.frame(
      V1 = safe_as_date(intervals[[1]][1]),
      V2 = safe_as_date(intervals[[1]][2])
    )
  } else {
    intervals_df <- do.call(rbind, lapply(intervals, function(x) {
      data.frame(V1 = safe_as_date(x[1]), V2 = safe_as_date(x[2]))
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
#' @noRd
remove_GW_outliers <- function(gw_concepts_list) {
  if (length(gw_concepts_list) == 0 || length(gw_concepts_list[[1]]) == 0) {
    return(c())
  }
  
  # Flatten list and convert to dates using safe conversion
  gw_dates <- safe_as_date(unlist(gw_concepts_list))
  
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